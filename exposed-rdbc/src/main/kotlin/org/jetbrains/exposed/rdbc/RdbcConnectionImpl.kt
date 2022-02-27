package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.Connection
import io.r2dbc.spi.IsolationLevel
import io.r2dbc.spi.ValidationDepth
import kotlinx.coroutines.*
import kotlinx.coroutines.reactive.*
import org.jetbrains.exposed.sql.statements.api.ExposedConnection
import org.jetbrains.exposed.sql.statements.api.ExposedDatabaseMetadata
import org.jetbrains.exposed.sql.statements.api.ExposedSavepoint
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import org.jetbrains.exposed.sql.statements.jdbc.JdbcDatabaseMetadataImpl
import org.reactivestreams.Publisher
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
internal typealias JdbcConnection = java.sql.Connection

fun unsupportedByRdbcDriver(): Nothing = throw UnsupportedOperationException("Unsupported by RDBC driver")

class RdbcScope(dispatcher: CoroutineDispatcher?) : CoroutineScope {
    override val coroutineContext: CoroutineContext = dispatcher?.let { EmptyCoroutineContext + dispatcher } ?: EmptyCoroutineContext
}

@ExperimentalCoroutinesApi
class RdbcConnectionImpl(
    override val connection: Publisher<out Connection>,
    private val scope: RdbcScope,
    val jdbcConnection: (() -> JdbcConnection)?
) :
    ExposedConnection<Publisher<out Connection>> {
    private var localConnection: Connection? = null

    internal fun <T> withConnection(body: suspend (Connection) -> T): T = runBlocking {
        withContext(scope.coroutineContext) {
            if (localConnection == null) {
                localConnection = connection.awaitFirst().also {
                    it.beginTransaction()
                }
            }
            body(localConnection!!)
        }
    }

    override val isClosed: Boolean get() = withConnection {
        !it.validate(ValidationDepth.LOCAL).awaitSingle() || !it.validate(ValidationDepth.REMOTE).awaitSingle()
    }

    override fun commit() {
        withConnection {
            it.commitTransaction().awaitFirstOrNull()
        }
    }

    override fun rollback() {
        withConnection {
            it.rollbackTransaction().awaitFirstOrNull()
        }
    }

    override fun close() {
        withConnection {
            it.close().awaitFirstOrNull()
        }
    }

    override var autoCommit: Boolean
        get() = withConnection { it.isAutoCommit }
        set(value) {
            withConnection { it.setAutoCommit(value).awaitFirstOrNull() }
        }

    private fun IsolationLevel.asInt() = when (this) {
        IsolationLevel.READ_UNCOMMITTED -> 1
        IsolationLevel.READ_COMMITTED -> 2
        IsolationLevel.REPEATABLE_READ -> 4
        IsolationLevel.SERIALIZABLE -> 8
        else -> error("Unsupported isolation level $this")
    }

    private fun Int.asIsolationLevel() = when (this) {
        1 -> IsolationLevel.READ_UNCOMMITTED
        2 -> IsolationLevel.READ_COMMITTED
        4 -> IsolationLevel.REPEATABLE_READ
        8 -> IsolationLevel.SERIALIZABLE
        else -> error("Unsupported isolation level $this")
    }

    override var transactionIsolation: Int
        get() = withConnection { it.transactionIsolationLevel.asInt() }
        set(value) {
            withConnection { it.setTransactionIsolationLevel(value.asIsolationLevel()).awaitFirstOrNull() }
        }

    private fun postProcessSQL(sql: String): String {
        val preparedStatementParams = sql.count { it == '?' }
        if (preparedStatementParams > 0) {
            var patchedSQL = sql
            (1..preparedStatementParams).forEach {
                patchedSQL = patchedSQL.replaceFirst("?", "\$$it")
            }
            return patchedSQL
        }
        return sql
    }

    override fun prepareStatement(sql: String, returnKeys: Boolean): PreparedStatementApi {
        return withConnection {
            val statement = if (returnKeys) {
                it.createStatement(postProcessSQL(sql)).returnGeneratedValues()
            } else {
                it.createStatement(postProcessSQL(sql))
            }
            RdbcPreparedStatementImpl(statement, returnKeys)
        }
    }

    override fun prepareStatement(sql: String, columns: Array<String>): PreparedStatementApi {
        return withConnection {
            RdbcPreparedStatementImpl(it.createStatement(postProcessSQL(sql)).returnGeneratedValues(*columns), returnValues = true)
        }
    }

    override fun executeInBatch(sqls: List<String>) {
        withConnection {
            val batch = it.createBatch()
            sqls.forEach { batch.add(it) }
            batch.execute().awaitSingle()
        }
    }

    override var catalog: String
        get() = unsupportedByRdbcDriver()
        set(value) { unsupportedByRdbcDriver() }
    override var schema: String
        get() = unsupportedByRdbcDriver()
        set(value) { unsupportedByRdbcDriver() }

    override fun <T> metadata(body: ExposedDatabaseMetadata.() -> T): T {
        val metadata = jdbcConnection?.invoke()?.metaData?.let { JdbcDatabaseMetadataImpl("", it) }
            ?: withConnection { RdbcDatabaseMetadataImpl(it.metadata) }
        return metadata.body()
    }

    override fun setSavepoint(name: String): ExposedSavepoint {
        return withConnection {
            it.createSavepoint(name).awaitFirstOrNull()
            RdbcSavepoint(name)
        }
    }

    override fun releaseSavepoint(savepoint: ExposedSavepoint) {
        withConnection {
            it.releaseSavepoint(savepoint.name).awaitFirstOrNull()
        }
    }

    override fun rollback(savepoint: ExposedSavepoint) {
        withConnection {
            it.rollbackTransactionToSavepoint(savepoint.name).awaitFirstOrNull()
        }
    }
}
