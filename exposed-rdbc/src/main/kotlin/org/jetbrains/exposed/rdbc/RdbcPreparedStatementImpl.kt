package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.Statement
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.*
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.io.InputStream
import java.sql.ResultSet
import kotlin.coroutines.coroutineContext

class RdbcPreparedStatementImpl(val statement: Statement) : PreparedStatementApi {

    private var executedResultSet: ResultSetEmulator? = null

    override var fetchSize: Int? = 0

    override fun addBatch() {
        statement.add()
    }

    override fun executeQuery(): ResultSet {
        return runBlocking {
            executeAndPrepareResultSet()
            executedResultSet!!
        }
    }

    private suspend fun executeAndReturnUpdatedRow(): Int {
        return statement.execute().awaitFirst().rowsUpdated.awaitFirstOrElse { 0 }
    }

    private suspend fun executeAndPrepareResultSet(): ResultSetEmulator {
        val resultSet = arrayListOf<List<Pair<String, Any?>>>()
        val executedStatement = statement.execute().awaitFirst()
        executedStatement.map { row, metadata ->
            metadata.columnMetadatas.mapIndexed { index, columnMetadata ->
                columnMetadata.name to row[index]
            }
        }.collect {
            resultSet.add(it)
        }
        return ResultSetEmulator(resultSet).also {
            executedResultSet = it
        }
    }

    override fun executeUpdate(): Int = runBlocking {
        executeAndReturnUpdatedRow()
    }

    override val resultSet: ResultSet?
        get() = executedResultSet

    override fun setNull(index: Int, columnType: IColumnType) {
        statement.bindNull(index - 1, String::class.java)
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
