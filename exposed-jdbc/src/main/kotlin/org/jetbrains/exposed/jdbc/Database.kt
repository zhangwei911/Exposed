package org.jetbrains.exposed.jdbc

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.DatabaseConfig
import org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.sql.transactions.ThreadLocalTransactionManager
import org.jetbrains.exposed.sql.transactions.TransactionManager
import java.sql.Connection
import java.sql.DriverManager
import javax.sql.ConnectionPoolDataSource
import javax.sql.DataSource

private fun doConnect(
    explicitVendor: String?,
    config: DatabaseConfig?,
    getNewConnection: () -> Connection,
    setupConnection: (Connection) -> Unit = {},
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    return Database(explicitVendor, config ?: DatabaseConfig.invoke()) {
        JdbcConnectionImpl(getNewConnection().apply { setupConnection(this) })
    }.apply {
        TransactionManager.registerManager(this, manager(this))
    }
}

fun Database.Companion.connect(
    datasource: DataSource,
    setupConnection: (Connection) -> Unit = {},
    databaseConfig: DatabaseConfig? = null,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    return doConnect(
        explicitVendor = null,
        config = databaseConfig,
        getNewConnection = { datasource.connection!! },
        setupConnection = setupConnection,
        manager = manager
    )
}

@Deprecated(
    level = DeprecationLevel.ERROR,
    replaceWith = ReplaceWith("connectPool(datasource, setupConnection, manager)"),
    message = "Use connectPool instead"
)
fun Database.Companion.connect(
    datasource: ConnectionPoolDataSource,
    setupConnection: (Connection) -> Unit = {},
    databaseConfig: DatabaseConfig? = null,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    return doConnect(
        explicitVendor = null,
        config = databaseConfig,
        getNewConnection = { datasource.pooledConnection.connection!! },
        setupConnection = setupConnection,
        manager = manager
    )
}

fun Database.Companion.connectPool(
    datasource: ConnectionPoolDataSource,
    setupConnection: (Connection) -> Unit = {},
    databaseConfig: DatabaseConfig? = null,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    return doConnect(
        explicitVendor = null,
        config = databaseConfig,
        getNewConnection = { datasource.pooledConnection.connection!! },
        setupConnection = setupConnection,
        manager = manager
    )
}

fun Database.Companion.connect(
    getNewConnection: () -> Connection,
    databaseConfig: DatabaseConfig? = null,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    return doConnect(
        explicitVendor = null,
        config = databaseConfig,
        getNewConnection = getNewConnection,
        manager = manager)
}

fun Database.Companion.connect(
    url: String,
    driver: String = getDriver(url),
    user: String = "",
    password: String = "",
    setupConnection: (Connection) -> Unit = {},
    databaseConfig: DatabaseConfig? = null,
    manager: (Database) -> TransactionManager = { ThreadLocalTransactionManager(it) }
): Database {
    Class.forName(driver).newInstance()
    val dialectName = getDialectName(url)
    return doConnect(dialectName, databaseConfig, { DriverManager.getConnection(url, user, password) }, setupConnection, manager)
}
