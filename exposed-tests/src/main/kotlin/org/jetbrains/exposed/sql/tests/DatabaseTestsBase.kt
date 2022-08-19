package org.jetbrains.exposed.sql.tests

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import io.r2dbc.h2.H2ConnectionConfiguration
import io.r2dbc.h2.H2ConnectionFactory
import io.r2dbc.h2.H2ConnectionOption
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.codec.EnumCodec
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.ConnectionFactoryOptions.DATABASE
import io.r2dbc.spi.ConnectionFactoryOptions.DRIVER
import io.r2dbc.spi.ConnectionFactoryOptions.HOST
import io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD
import io.r2dbc.spi.ConnectionFactoryOptions.PORT
import io.r2dbc.spi.ConnectionFactoryOptions.USER
import org.h2.engine.Mode
import org.jetbrains.exposed.jdbc.connect
import org.jetbrains.exposed.rdbc.connect as rdbcConnect
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.sql.tests.shared.Foo
import org.jetbrains.exposed.sql.transactions.inTopLevelTransaction
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.transactions.transactionManager
import org.junit.Assume
import org.junit.AssumptionViolatedException
import org.testcontainers.containers.MySQLContainer
import java.sql.Connection
import java.util.*
import kotlin.concurrent.thread
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KVisibility
import kotlin.reflect.full.declaredMemberProperties

sealed class TestDB(
    val beforeConnection: () -> Unit = {},
    val afterTestFinished: () -> Unit = {},
    val dbConfig: DatabaseConfig.Builder.() -> Unit = {}
) {
    lateinit var db: Database
    protected abstract fun initDatabase(config: DatabaseConfig): Database

    open val name: String get() = this::class.simpleName!!

    fun connect(configure: DatabaseConfig.Builder.() -> Unit = {}): Database {
        val config = DatabaseConfig {
            dbConfig()
            configure()
        }
        return initDatabase(config)
    }

    sealed class Jdbc(
        val connection: () -> String,
        val driver: String,
        val user: String = "root",
        val pass: String = "",
        beforeConnection: () -> Unit = {},
        afterTestFinished: () -> Unit = {},
        dbConfig: DatabaseConfig.Builder.() -> Unit = {}
    ) : TestDB(beforeConnection, afterTestFinished, dbConfig) {

        override fun initDatabase(config: DatabaseConfig) =
            Database.connect(connection(), user = user, password = pass, driver = driver, databaseConfig = config)

        object H2 : Jdbc({ "jdbc:h2:mem:regular;DB_CLOSE_DELAY=-1;" }, "org.h2.Driver")

        object H2_MYSQL : Jdbc(
            { "jdbc:h2:mem:mysql;MODE=MySQL;DB_CLOSE_DELAY=-1" }, "org.h2.Driver",
            beforeConnection = {
                Mode::class.declaredMemberProperties.firstOrNull { it.name == "convertInsertNullToZero" }?.let { field ->
                   val mode = Mode.getInstance("MySQL")
                   (field as KMutableProperty1<Mode, Boolean>).set(mode, false)
                }
            })

        internal object H2_RDBC : Jdbc({ "jdbc:h2:mem:rdbc;DB_CLOSE_DELAY=-1;" }, "org.h2.Driver")

        object SQLITE : Jdbc({ "jdbc:sqlite:file:test?mode=memory&cache=shared" }, "org.sqlite.JDBC")

        object MYSQL : Jdbc(
            connection = {
                if (runTestContainersMySQL()) {
                "${mySQLProcess.jdbcUrl}?createDatabaseIfNotExist=true&characterEncoding=UTF-8&useSSL=false&zeroDateTimeBehavior=convertToNull"
                } else {
                    val host = System.getProperty("exposed.test.mysql.host") ?: System.getProperty("exposed.test.mysql8.host")
                    val port = System.getProperty("exposed.test.mysql.port") ?: System.getProperty("exposed.test.mysql8.port")
                    host.let { dockerHost ->
                    "jdbc:mysql://$dockerHost:$port/testdb?useSSL=false&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
                    }
                }
            },
            user = "root",
            pass = if (runTestContainersMySQL()) "test" else "",
            driver = "com.mysql.jdbc.Driver",
            beforeConnection = { if (runTestContainersMySQL()) mySQLProcess },
            afterTestFinished = { if (runTestContainersMySQL()) mySQLProcess.close() }
        )

        object POSTGRESQL : Jdbc(
            connection = { "jdbc:postgresql://localhost:12346/template1?user=postgres&password=&lc_messages=en_US.UTF-8" },
            driver = "org.postgresql.Driver",
            user = "postgres",
            beforeConnection = { postgresSQLProcess },
            afterTestFinished = { postgresSQLProcess.close() }
        )

        object POSTGRESQLNG : Jdbc(
            connection = { "jdbc:pgsql://localhost:12346/template1?user=postgres&password=" },
            driver = "com.impossibl.postgres.jdbc.PGDriver",
            user = "postgres",
            beforeConnection = { postgresSQLProcess },
            afterTestFinished = { postgresSQLProcess.close() }
        )

        object ORACLE : Jdbc(
            driver = "oracle.jdbc.OracleDriver",
            user = "ExposedTest",
            pass = "12345",
            connection = {
                "jdbc:oracle:thin:@//${System.getProperty("exposed.test.oracle.host", "localhost")}:${System.getProperty("exposed.test.oracle.port", "1521")}/XEPDB1"
            },
            beforeConnection = {
                Locale.setDefault(Locale.ENGLISH)
                val tmp = Database.connect(ORACLE.connection(), user = "sys as sysdba", password = "Oracle18", driver = "oracle.jdbc.OracleDriver")
                transaction(Connection.TRANSACTION_READ_COMMITTED, 1, tmp) {
                    try {
                        exec("DROP USER ExposedTest CASCADE")
                    } catch (e: Exception) { // ignore
                    exposedLogger.warn("Exception on deleting ExposedTest user")
                    }
                    exec("CREATE USER ExposedTest ACCOUNT UNLOCK IDENTIFIED BY 12345")
                    exec("grant all privileges to ExposedTest")
                }
                Unit
            }
        )

        object SQLSERVER : Jdbc(
            connection = {
                "jdbc:sqlserver://${System.getProperty("exposed.test.sqlserver.host", "192.168.99.100")}:${
                System.getProperty(
                    "exposed.test.sqlserver.port",
                    "32781"
                )
                }"
            },
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            user = "SA",
            pass = "yourStrong(!)Password"
        )

        object MARIADB : Jdbc(
            connection = {
                "jdbc:mariadb://${System.getProperty("exposed.test.mariadb.host", "192.168.99.100")}:${
                System.getProperty(
                    "exposed.test.mariadb.port",
                    "3306"
                )
                }/testdb"
            },
            driver = "org.mariadb.jdbc.Driver"
        )
    }

    sealed class Rdbc(
        val connectionFactory: () -> ConnectionFactory,
        val jdbc: Jdbc,
        beforeConnection: () -> Unit = {},
        afterTestFinished: () -> Unit = {},
    ) : TestDB(beforeConnection, afterTestFinished) {

        override val name: String = "RDBC.${super.name}"

        override fun initDatabase(config: DatabaseConfig): Database {
            registerDBIfNeeded(Jdbc.POSTGRESQL)
            return Database.rdbcConnect(
                connection = connectionFactory().create(),
                jdbcConnection = {
                    (jdbc.db.connector() as JdbcConnectionImpl).connection
                },
                databaseConfig = config
            )
        }

        object H2 : Rdbc(
            connectionFactory = {
                H2ConnectionFactory(
                    H2ConnectionConfiguration.builder().inMemory("rdbc")
                        .username(Jdbc.H2_RDBC.user)
                        .password(Jdbc.H2_RDBC.pass)
                        .property(H2ConnectionOption.DB_CLOSE_DELAY, "-1")
                        .build()
                )
            },
            jdbc = Jdbc.H2_RDBC
        )

        object POSTGRESQL : Rdbc(
            connectionFactory = {
                val enumCodec1 = EnumCodec.builder()
                    .withEnum("FooEnum", Foo::class.java)
                    .build()
                val enumCodec2 = EnumCodec.builder()
                    .withEnum("FooEnum2", Foo::class.java)
                    .build()
                val config = PostgresqlConnectionConfiguration.builder()
                    .host("localhost")
                    .port(postgresSQLProcess.port)
                    .database("template1")
                    .username(Jdbc.POSTGRESQL.user)
                    .password(Jdbc.POSTGRESQL.pass)
                    .codecRegistrar(enumCodec1)
                    .codecRegistrar(enumCodec2)
                    .build()
                PostgresqlConnectionFactory(config)
                /*ConnectionFactories.get(
                    ConnectionFactoryOptions.builder()
                        .option(DRIVER, PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER)
                        .option(HOST, "localhost")
                        .option(PORT, postgresSQLProcess.port)
                        .option(DATABASE, "template1")
                        .option(USER, Jdbc.POSTGRESQL.user)
                        .option(PASSWORD, Jdbc.POSTGRESQL.pass)
                        .build()
                )*/
            },
            jdbc = Jdbc.POSTGRESQL,
            beforeConnection = Jdbc.POSTGRESQL.beforeConnection,
            afterTestFinished = Jdbc.POSTGRESQL.afterTestFinished,
        )
    }

    companion object {
        private fun <T : Any> KClass<out T>.recursiveSealedSubclasses(): List<T> = sealedSubclasses.map { clazz ->
            clazz.takeIf { it.visibility == KVisibility.PUBLIC }?.objectInstance?.let { listOf(it) } ?: clazz.recursiveSealedSubclasses()
        }.flatten()

        private val values by lazy { TestDB::class.recursiveSealedSubclasses() }

        fun values(): List<TestDB> = values

        fun enabledInTests(): Set<TestDB> {
            val concreteDialects = System.getProperty("exposed.test.dialects", "")
                .split(",")
                .mapTo(HashSet()) { it.trim().uppercase() }
            return values().filterTo(mutableSetOf()) { it.name in concreteDialects }
        }
    }
}

private val registeredOnShutdown = HashSet<TestDB>()

private val postgresSQLProcess by lazy {
    EmbeddedPostgres.builder()
        .setPgBinaryResolver { system, _ ->
            EmbeddedPostgres::class.java.getResourceAsStream("/postgresql-$system-x86_64.txz")
        }
        .setPort(12346).start()
}

// MySQLContainer has to be extended, otherwise it leads to Kotlin compiler issues: https://github.com/testcontainers/testcontainers-java/issues/318
internal class SpecifiedMySQLContainer(val image: String) : MySQLContainer<SpecifiedMySQLContainer>(image)

private val mySQLProcess by lazy {
    SpecifiedMySQLContainer(image = "mysql:5")
        .withDatabaseName("testdb")
        .withEnv("MYSQL_ROOT_PASSWORD", "test")
        .withExposedPorts().apply {
            start()
        }
}

private fun runTestContainersMySQL(): Boolean =
    (System.getProperty("exposed.test.mysql.host") ?: System.getProperty("exposed.test.mysql8.host")).isNullOrBlank()

private fun registerDBIfNeeded(dbSettings: TestDB) {
    if (dbSettings !in registeredOnShutdown) {
        dbSettings.beforeConnection()
        Runtime.getRuntime().addShutdownHook(
            thread(false) {
                dbSettings.afterTestFinished()
                registeredOnShutdown.remove(dbSettings)
            }
        )
        registeredOnShutdown += dbSettings
        dbSettings.db = dbSettings.connect()
    }
}

@Suppress("UnnecessaryAbstractClass")
abstract class DatabaseTestsBase {
    init {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    }

    fun withDb(dbSettings: TestDB, statement: Transaction.(TestDB) -> Unit) {
        try {
            Assume.assumeTrue(dbSettings in TestDB.enabledInTests())
        } catch (e: AssumptionViolatedException) {
            exposedLogger.warn("$dbSettings is not enabled for being used in tests", e)
            throw e
        }

        registerDBIfNeeded(dbSettings)

        val database = dbSettings.db

        transaction(database.transactionManager.defaultIsolationLevel, 1, db = database) {
            statement(dbSettings)
        }
    }

    fun withDb(db: List<TestDB>? = null, excludeSettings: List<TestDB> = emptyList(), statement: Transaction.(TestDB) -> Unit) {
        val enabledInTests = TestDB.enabledInTests()
        val toTest = db?.intersect(enabledInTests) ?: (enabledInTests - excludeSettings)
        Assume.assumeTrue(toTest.isNotEmpty())
        toTest.forEach { dbSettings ->
            @Suppress("TooGenericExceptionCaught")
            try {
                withDb(dbSettings, statement)
            } catch (e: Exception) {
                throw AssertionError("Failed on ${dbSettings.name}", e)
            }
        }
    }

    fun withTables(excludeSettings: List<TestDB>, vararg tables: Table, statement: Transaction.(TestDB) -> Unit) {
        val toTest = TestDB.enabledInTests() - excludeSettings
        Assume.assumeTrue(toTest.isNotEmpty())
        toTest.forEach { testDB ->
            withDb(testDB) {
                SchemaUtils.create(*tables)
                try {
                    statement(testDB)
                    commit() // Need commit to persist data before drop tables
                } finally {
                    try {
                        SchemaUtils.drop(*tables)
                        commit()
                    } catch (_: Exception) {
                        val database = testDB.db
                        inTopLevelTransaction(database.transactionManager.defaultIsolationLevel, 1, db = database) {
                            SchemaUtils.drop(*tables)
                        }
                    }
                }
            }
        }
    }

    fun withSchemas(excludeSettings: List<TestDB>, vararg schemas: Schema, statement: Transaction.() -> Unit) {
        val toTest = TestDB.enabledInTests() - excludeSettings
        Assume.assumeTrue(toTest.isNotEmpty())
        toTest.forEach { testDB ->
            withDb(testDB) {
                SchemaUtils.createSchema(*schemas)
                try {
                    statement()
                    commit() // Need commit to persist data before drop schemas
                } finally {
                    val cascade = it != TestDB.Jdbc.SQLSERVER
                    SchemaUtils.dropSchema(*schemas, cascade = cascade)
                    commit()
                }
            }
        }
    }

    fun withTables(vararg tables: Table, statement: Transaction.(TestDB) -> Unit) =
        withTables(excludeSettings = emptyList(), tables = tables, statement = statement)

    fun withSchemas(vararg schemas: Schema, statement: Transaction.() -> Unit) =
        withSchemas(excludeSettings = emptyList(), schemas = schemas, statement = statement)

    fun addIfNotExistsIfSupported() = if (currentDialectTest.supportsIfNotExists) {
        "IF NOT EXISTS "
    } else {
        ""
    }

    protected fun prepareSchemaForTest(schemaName: String) : Schema {
        return Schema(schemaName, defaultTablespace = "USERS", temporaryTablespace = "TEMP ", quota = "20M", on = "USERS")
    }
}
