package org.jetbrains.exposed.jdbc

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.sql.transactions.DEFAULT_REPETITION_ATTEMPTS
import org.jetbrains.exposed.sql.transactions.ThreadLocalTransactionManager
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.vendors.*
import java.sql.Connection
import java.sql.DriverManager
import javax.sql.ConnectionPoolDataSource
import javax.sql.DataSource

private fun doConnect(
    explicitVendor: String?,
    getNewConnection: () -> Connection,
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    return Database(explicitVendor) {
        JdbcConnectionImpl(getNewConnection().apply { setupConnection(this) })
    }.apply {
        TransactionManager.registerManager(this, manager(this))
    }
}

fun Database.Companion.connect(
    datasource: DataSource,
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    return doConnect(explicitVendor = null, getNewConnection = { datasource.connection!! }, setupConnection = setupConnection, manager = manager)
}

@Deprecated(level = DeprecationLevel.ERROR, replaceWith = ReplaceWith("connectPool(datasource, setupConnection, manager)"), message = "Use connectPool instead")
fun Database.Companion.connect(
    datasource: ConnectionPoolDataSource,
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    return doConnect(explicitVendor = null, getNewConnection = { datasource.pooledConnection.connection!! }, setupConnection = setupConnection, manager = manager)
}

fun Database.Companion.connectPool(
    datasource: ConnectionPoolDataSource,
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    return doConnect(explicitVendor = null, getNewConnection = { datasource.pooledConnection.connection!! }, setupConnection = setupConnection, manager = manager)
}

fun Database.Companion.connect(
    getNewConnection: () -> Connection,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    return doConnect(explicitVendor = null, getNewConnection = getNewConnection, manager = manager)
}

fun Database.Companion.connect(
    url: String,
    driver: String = getDriver(url),
    user: String = "",
    password: String = "",
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it, DEFAULT_REPETITION_ATTEMPTS) }
): Database {
    Class.forName(driver).newInstance()

    return doConnect(getDialectName(url), { DriverManager.getConnection(url, user, password) }, setupConnection, manager)
}

private fun getDriver(url: String) = when {
    url.startsWith("jdbc:h2") -> "org.h2.Driver"
    url.startsWith("jdbc:postgresql") -> "org.postgresql.Driver"
    url.startsWith("jdbc:pgsql") -> "com.impossibl.postgres.jdbc.PGDriver"
    url.startsWith("jdbc:mysql") -> "com.mysql.cj.jdbc.Driver"
    url.startsWith("jdbc:mariadb") -> "org.mariadb.jdbc.Driver"
    url.startsWith("jdbc:oracle") -> "oracle.jdbc.OracleDriver"
    url.startsWith("jdbc:sqlite") -> "org.sqlite.JDBC"
    url.startsWith("jdbc:sqlserver") -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    else -> error("Database driver not found for $url")
}

private fun getDialectName(url: String) = when {
    url.startsWith("jdbc:h2") -> H2Dialect.dialectName
    url.startsWith("jdbc:postgresql") -> PostgreSQLDialect.dialectName
    url.startsWith("jdbc:pgsql") -> PostgreSQLNGDialect.dialectName
    url.startsWith("jdbc:mysql") -> MysqlDialect.dialectName
    url.startsWith("jdbc:mariadb") -> MariaDBDialect.dialectName
    url.startsWith("jdbc:oracle") -> OracleDialect.dialectName
    url.startsWith("jdbc:sqlite") -> SQLiteDialect.dialectName
    url.startsWith("jdbc:sqlserver") -> SQLServerDialect.dialectName
    else -> error("Can't resolve dialect for connection: $url")
}
