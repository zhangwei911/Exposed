package org.jetbrains.exposed.sql.tests.shared.ddl

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.currentDialectTest
import org.jetbrains.exposed.sql.tests.shared.DDLTests
import org.jetbrains.exposed.sql.tests.shared.Foo
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.jetbrains.exposed.sql.vendors.H2Dialect
import org.jetbrains.exposed.sql.vendors.MysqlDialect
import org.jetbrains.exposed.sql.vendors.PostgreSQLDialect
import org.junit.Test

class EnumerationTests : DatabaseTestsBase() {
    object EnumTable : IntIdTable("EnumTable") {
        internal var enumColumn: Column<Foo> = enumeration("enumColumn")

        internal fun initEnumColumn(sql: String, testDB: TestDB) {
            (columns as MutableList<Column<*>>).remove(enumColumn)
            enumColumn = customEnumeration(
                "enumColumn", sql,
                { value ->
                    when {
                        currentDialectTest is H2Dialect && value is Int -> Foo.values()[value]
                        else -> Foo.valueOf(value as String)
                    }
                },
                { value ->
                    when {
                        testDB == TestDB.Rdbc.POSTGRESQL -> value
                        currentDialectTest is PostgreSQLDialect -> DDLTests.PGEnum(sql, value)
                        else -> value.name
                    }
                }
            )
        }
    }

    @Test
    fun testCustomEnumeration01() {
        withDb(listOf(TestDB.Jdbc.H2, TestDB.Jdbc.MYSQL, TestDB.Jdbc.POSTGRESQL, TestDB.Jdbc.POSTGRESQLNG)) { testDB ->
            val sqlType = when (currentDialectTest) {
                is H2Dialect, is MysqlDialect -> "ENUM('Bar', 'Baz')"
                is PostgreSQLDialect -> "FooEnum"
                else -> error("Unsupported case")
            }

            class EnumEntity(id: EntityID<Int>) : IntEntity(id) {
                var enum by EnumTable.enumColumn
            }

            val EnumClass = object : IntEntityClass<EnumEntity>(EnumTable, EnumEntity::class.java) {}

            try {
                if (currentDialectTest is PostgreSQLDialect) {
                    exec("DROP TYPE IF EXISTS FooEnum;")
                    exec("CREATE TYPE FooEnum AS ENUM ('Bar', 'Baz');")
                }
                EnumTable.initEnumColumn(sqlType, testDB)
                SchemaUtils.create(EnumTable)
                EnumTable.insert {
                    it[enumColumn] = Foo.Bar
                }
                assertEquals(Foo.Bar, EnumTable.selectAll().single()[EnumTable.enumColumn])

                EnumTable.update {
                    it[enumColumn] = Foo.Baz
                }

                val entity = EnumClass.new {
                    enum = Foo.Baz
                }
                assertEquals(Foo.Baz, entity.enum)
                entity.id.value // flush entity
                assertEquals(Foo.Baz, entity.enum)
                assertEquals(Foo.Baz, EnumClass.reload(entity)!!.enum)
                entity.enum = Foo.Bar

                assertEquals(Foo.Bar, EnumClass.reload(entity, true)!!.enum)
            } finally {
                try {
                    SchemaUtils.drop(EnumTable)
                } catch (ignore: Exception) {}
            }
        }
    }

    @Test
    fun testCustomEnumerationWithDefaultValue() {
        withDb(listOf(TestDB.Jdbc.H2, TestDB.Jdbc.MYSQL, TestDB.Jdbc.POSTGRESQL, TestDB.Jdbc.POSTGRESQLNG)) { testDB ->
            val sqlType = when (currentDialectTest) {
                is H2Dialect, is MysqlDialect -> "ENUM('Bar', 'Baz')"
                is PostgreSQLDialect -> "FooEnum2"
                else -> error("Unsupported case")
            }
            try {
                if (currentDialectTest is PostgreSQLDialect) {
                    exec("DROP TYPE IF EXISTS FooEnum2;")
                    exec("CREATE TYPE FooEnum2 AS ENUM ('Bar', 'Baz');")
                }
                EnumTable.initEnumColumn(sqlType, testDB)
                with(EnumTable) {
                    EnumTable.enumColumn.default(Foo.Bar)
                }
                SchemaUtils.create(EnumTable)

                EnumTable.insert { }
                val default = EnumTable.selectAll().single()[EnumTable.enumColumn]
                assertEquals(Foo.Bar, default)
            } finally {
                try {
                    SchemaUtils.drop(EnumTable)
                } catch (ignore: Exception) {}
            }
        }
    }
}
