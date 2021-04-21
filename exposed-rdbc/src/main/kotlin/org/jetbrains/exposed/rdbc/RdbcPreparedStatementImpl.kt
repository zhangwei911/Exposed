package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.Statement
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.*
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.io.InputStream
import java.sql.ResultSet

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

    private suspend fun executeAndPrepareResultSet(): Int {
        val resultSet = arrayListOf<List<Pair<String, Any?>>>()
        var updatedRows = 0
        val executedStatement = statement.execute()
        executedStatement.collect {
            updatedRows = it.rowsUpdated.awaitFirstOrElse { 0 }
            it.map { row, metadata ->
                metadata.columnMetadatas.mapIndexed { index, columnMetadata ->
                    columnMetadata.name to row[index]
                }
            }.collect {
                resultSet.add(it)
            }
        }
        ResultSetEmulator(resultSet).also {
            executedResultSet = it
        }
        return updatedRows
    }

    override fun executeUpdate(): Int = runBlocking {
        executeAndPrepareResultSet()
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
            executeAndPrepareResultSet()
            executedResultSet!!.rows.map { 1 }
        }
    }
}
