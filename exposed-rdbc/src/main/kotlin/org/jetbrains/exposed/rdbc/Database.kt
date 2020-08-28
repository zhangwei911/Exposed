package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.Connection
import kotlinx.coroutines.CoroutineDispatcher
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.DEFAULT_REPETITION_ATTEMPTS
import org.jetbrains.exposed.sql.transactions.ThreadLocalTransactionManager
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.reactivestreams.Publisher

fun <T:Publisher<out Connection>> Database.Companion.connect(
    connection: T,
    jdbcConnection: (() -> JdbcConnection)? = null,
    dispatcher: CoroutineDispatcher? = null
) : Database {
    val scope = RdbcScope(dispatcher)
    return Database {
        RdbcConnectionImpl(connection, scope, jdbcConnection)
    }.apply {
        TransactionManager.registerManager(this, ThreadLocalTransactionManager(this, DEFAULT_REPETITION_ATTEMPTS))
    }
}