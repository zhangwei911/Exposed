package org.jetbrains.exposed.sql

import org.jetbrains.exposed.sql.statements.Statement
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

class ResultRow(internal val fieldIndex: Map<Expression<*>, Int>) {
    private val data = arrayOfNulls<Any?>(fieldIndex.size)

    /**
     * Retrieves value of a given expression on this row.
     *
     * @param c expression to evaluate
     * @throws IllegalStateException if expression is not in record set or if result value is uninitialized
     *
     * @see [getOrNull] to get null in the cases an exception would be thrown
     */
    operator fun <T> get(c: Expression<T>): T {
        val d = getRaw(c)

        if (d == null && c is Column<*> && c.dbDefaultValue != null && !c.columnType.nullable) {
            exposedLogger.warn("Column ${TransactionManager.current().fullIdentity(c)} is marked as not null, " +
                    "has default db value, but returns null. Possible have to re-read it from DB.")
        }

        return rawToColumnValue(d, c)
    }

    operator fun <T> set(c: Expression<out T>, value: T) {
        val index = fieldIndex[c] ?: error("$c is not in record set")
        data[index] = value
    }

    fun <T> hasValue(c: Expression<T>): Boolean = fieldIndex[c]?.let{ data[it] != NotInitializedValue } ?: false

    fun <T> getOrNull(c: Expression<T>): T? = if (hasValue(c)) rawToColumnValue(getRaw(c), c) else null

    @Deprecated("Replaced with getOrNull to be more kotlinish", replaceWith = ReplaceWith("getOrNull(c)"))
    fun <T> tryGet(c: Expression<T>): T? = getOrNull(c)

    @Suppress("UNCHECKED_CAST")
    private fun <T> rawToColumnValue(raw: T?, c: Expression<T>): T {
        return when {
            raw == null -> null
            raw == NotInitializedValue -> error("$c is not initialized yet")
            c is ExpressionAlias<T> && c.delegate is ExpressionWithColumnType<T> -> c.delegate.columnType.valueFromDB(raw)
            c is ExpressionWithColumnType<T> -> c.columnType.valueFromDB(raw)
            else -> raw
        } as T
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> getRaw(c: Expression<T>): T? =
            data[fieldIndex[c] ?: error("$c is not in record set")] as T?

    override fun toString(): String = fieldIndex.entries.joinToString { "${it.key}=${data[it.value]}" }

    internal object NotInitializedValue

    companion object {
        fun create(rs: ResultSet, fields: List<Expression<*>>): ResultRow {
            val fieldsIndex = fields.distinct().mapIndexed { i, field ->
                val value = (field as? ExpressionWithColumnType<*>)?.columnType?.readObject(rs, i + 1) ?: rs.getObject(i + 1)
                (field to i) to value
            }.toMap()
            return ResultRow(fieldsIndex.keys.toMap()).apply {
                fieldsIndex.forEach{ (i, f) ->
                    data[i.second] = f
                }
            }
        }

        internal fun createAndFillValues(data: Map<Expression<*>, Any?>) : ResultRow =
            ResultRow(data.keys.mapIndexed { i, c -> c to i }.toMap()).also { row ->
                data.forEach { (c, v) -> row[c] = v }
            }

        internal fun createAndFillDefaults(columns : List<Column<*>>): ResultRow =
            ResultRow(columns.mapIndexed { i, c -> c to i }.toMap()).apply {
                columns.forEach {
                    this[it] = it.defaultValueFun?.invoke() ?: if (!it.columnType.nullable) NotInitializedValue else null
                }
            }
    }
}

enum class SortOrder {
    ASC, DESC
}

open class Query(override var set: FieldSet, where: Op<Boolean>?) : AbstractQuery<Query>(set.source.targetTables()) {

    var groupedByColumns: List<Expression<*>> = mutableListOf()
        private set

    var having: Op<Boolean>? = null
        private set

    private var forUpdate: Boolean? = null

    //private set
    var where: Op<Boolean>? = where
        private set

    override val queryToExecute: Statement<ResultSet> get() {
        val distinctExpressions = set.fields.distinct()
        return if (distinctExpressions.size < set.fields.size) {
            copy().adjustSlice { slice(distinctExpressions) }
        } else
            this
    }

    override fun copy(): Query = Query(set, where).also { copy ->
        copyTo(copy)
        copy.groupedByColumns = groupedByColumns.toMutableList()
        copy.having = having
        copy.forUpdate = forUpdate
    }

    override fun forUpdate(): Query {
        this.forUpdate = true
        return this
    }

    override fun notForUpdate(): Query {
        forUpdate = false
        return this
    }

    /**
     * Changes [set.fields] field of a Query, [set.source] will be preserved
     * @param body builder for new column set, current [set.source] used as a receiver, you are expected to slice it
     * @sample org.jetbrains.exposed.sql.tests.shared.DMLTests.testAdjustQuerySlice
     */
    fun adjustSlice(body: ColumnSet.() -> FieldSet): Query = apply { set = set.source.body() }

    /**
     * Changes [set.source] field of a Query, [set.fields] will be preserved
     * @param body builder for new column set, previous value used as a receiver
     * @sample org.jetbrains.exposed.sql.tests.shared.DMLTests.testAdjustQueryColumnSet
     */
    fun adjustColumnSet(body: ColumnSet.() -> ColumnSet): Query {
        val oldSlice = set.fields
        return adjustSlice { body().slice(oldSlice) }
    }

    /**
     * Changes [where] field of a Query.
     * @param body new WHERE condition builder, previous value used as a receiver
     * @sample org.jetbrains.exposed.sql.tests.shared.DMLTests.testAdjustQueryWhere
     */
    fun adjustWhere(body: Op<Boolean>?.() -> Op<Boolean>): Query = apply { where = where.body() }

    fun hasCustomForUpdateState() = forUpdate != null
    fun isForUpdate() = (forUpdate ?: false) && currentDialect.supportsSelectForUpdate()

    override fun PreparedStatement.executeInternal(transaction: Transaction): ResultSet? {
        val fetchSize = this@Query.fetchSize ?: transaction.db.defaultFetchSize
        if (fetchSize != null) {
            this.fetchSize = fetchSize
        }
        return executeQuery()
    }

    override fun prepareSQL(builder: QueryBuilder): String {
        builder {
            append("SELECT ")

            if (count) {
                append("COUNT(*)")
            }
            else {
                if (distinct) {
                    append("DISTINCT ")
                }
                set.fields.appendTo { +it }
            }
            append(" FROM ")
            set.source.describe(transaction, this)

            where?.let {
                append(" WHERE ")
                +it
            }

            if (!count) {
                if (groupedByColumns.isNotEmpty()) {
                    append(" GROUP BY ")
                    groupedByColumns.appendTo {
                        +((it as? ExpressionAlias)?.aliasOnlyExpression() ?: it)
                    }
                }

                having?.let {
                    append(" HAVING ")
                    append(it)
                }

                if (orderByExpressions.isNotEmpty()) {
                    append(" ORDER BY ")
                    orderByExpressions.appendTo {
                        append((it.first as? ExpressionAlias<*>)?.alias ?: it.first, " ", it.second.name)
                    }
                }

                limit?.let {
                    append(" ")
                    append(currentDialect.functionProvider.queryLimit(it, offset, orderByExpressions.isNotEmpty()))
                }
            }

            if (isForUpdate()) {
                append(" FOR UPDATE")
            }
        }
        return builder.toString()
    }

    fun groupBy(vararg columns: Expression<*>): Query {
        for (column in columns) {
            (groupedByColumns as MutableList).add(column)
        }
        return this
    }

    fun having(op: SqlExpressionBuilder.() -> Op<Boolean>) : Query {
        val oop = SqlExpressionBuilder.op()
        if (having != null) {
            error ("HAVING clause is specified twice. Old value = '$having', new value = '$oop'")
        }
        having = oop
        return this
    }

    @Deprecated("use orderBy with SortOrder instead")
    @JvmName("orderByDeprecated")
    fun orderBy(column: Expression<*>, isAsc: Boolean) : Query = orderBy(column to isAsc)

    @Deprecated("use orderBy with SortOrder instead")
    @JvmName("orderByDeprecated2")
    fun orderBy(vararg columns: Pair<Expression<*>, Boolean>) : Query {
        (orderByExpressions as MutableList).addAll(columns.map{ it.first to if(it.second) SortOrder.ASC else SortOrder.DESC })
        return this
    }

    override fun count(): Int {
        flushEntities()

        return if (distinct || groupedByColumns.isNotEmpty() || limit != null) {
            fun Column<*>.makeAlias() = alias(transaction.db.identifierManager.quoteIfNecessary("${table.tableName}_$name"))

            val originalSet = set
            try {
                var expInx = 0
                adjustSlice {
                    slice(originalSet.fields.map {
                        it as? ExpressionAlias<*> ?: ((it as? Column<*>)?.makeAlias() ?: it.alias("exp${expInx++}"))
                    })
                }

                alias("subquery").selectAll().count()
            } finally {
                set = originalSet
            }
        } else {
            try {
                count = true
                transaction.exec(this) { rs ->
                    rs.next()
                    rs.getInt(1).also { rs.close() }
                }!!
            } finally {
                count = false
            }
        }
    }

    override fun empty(): Boolean {
        flushEntities()

        val oldLimit = limit
        try {
            if (!isForUpdate())
                limit = 1
            val resultSet = transaction.exec(this)!!
            return !resultSet.next().also { resultSet.close() }
        } finally {
            limit = oldLimit
        }
    }
}

/**
 * Mutate Query instance and add `andPart` to where condition with `and` operator.
 * @return same Query instance which was provided as a receiver.
 */
fun Query.andWhere(andPart: SqlExpressionBuilder.() -> Op<Boolean>) = adjustWhere {
    val expr = Op.build { andPart() }
    if(this == null) expr
    else this and expr
}

/**
 * Mutate Query instance and add `andPart` to where condition with `or` operator.
 * @return same Query instance which was provided as a receiver.
 */
fun Query.orWhere(andPart: SqlExpressionBuilder.() -> Op<Boolean>) = adjustWhere {
    val expr = Op.build { andPart() }
    if(this == null) expr
    else this or expr
}