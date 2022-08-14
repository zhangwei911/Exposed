package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.Statement
import kotlinx.coroutines.reactive.*
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.io.InputStream
import java.sql.ResultSet

class RdbcPreparedStatementImpl(val statement: Statement, private val returnValues: Boolean) : PreparedStatementApi {

    private var executedResultSet: ResultSetEmulator? = null

    override var fetchSize: Int? = 0

    override fun addBatch() {
        statement.add()
    }

    override fun executeQuery(): ResultSet {
        return runBlocking {
            executeAndPrepareResultSet()
        }
    }

    private suspend fun executeAndReturnUpdatedRow(): Int {
        return statement.execute().awaitFirst().rowsUpdated.awaitFirstOrElse { 0 }.toInt()
    }

    private suspend fun executeAndPrepareResultSet(): ResultSetEmulator {
        val resultSet = arrayListOf<List<Pair<String, Any?>>>()
        statement.execute().collect {
            it.map { row, metadata ->
                metadata.columnMetadatas.mapIndexed { index, columnMetadata ->
                    columnMetadata.name to row[index]
                }
            }.collect {
                resultSet.add(it)
            }
        }
        return ResultSetEmulator(resultSet).also {
            executedResultSet = it
        }
    }

    override fun executeUpdate(): Int = runBlocking {
        if (returnValues) {
            executeAndPrepareResultSet().rows.size
        } else {
            executeAndReturnUpdatedRow()
        }
    }

    override val resultSet: ResultSet?
        get() = executedResultSet

    override fun setNull(index: Int, columnType: IColumnType) {
        val encodeType = when (columnType) {
            is IntegerColumnType, is UIntegerColumnType, is LongColumnType, is ULongColumnType,
            is ShortColumnType, is UShortColumnType, is ByteColumnType, is UByteColumnType,
            -> java.lang.Long::class.java
            else -> String::class.java
        }
        statement.bindNull(index - 1, encodeType)
    }

    override fun setInputStream(index: Int, inputStream: InputStream) {
        statement.bind(index - 1, inputStream.readBytes())
    }

    override fun set(index: Int, value: Any) {
        statement.bind(index - 1, value)
    }

    override fun closeIfPossible() {
        /* nothing */
    }

    override fun executeBatch(): List<Int> {
        return runBlocking {
            executeAndPrepareResultSet().rows.map { 1 }
        }
    }

    override fun cancel() {
        // TODO:
    }
}
