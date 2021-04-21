package org.jetbrains.exposed.rdbc

import io.r2dbc.spi.ConnectionMetadata
import org.jetbrains.exposed.sql.ForeignKeyConstraint
import org.jetbrains.exposed.sql.Index
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.statements.api.ExposedDatabaseMetadata
import org.jetbrains.exposed.sql.statements.api.IdentifierManagerApi
import org.jetbrains.exposed.sql.statements.api.identifiers.H2PredefinedIdentifierManager
import org.jetbrains.exposed.sql.statements.api.identifiers.PostgrePredefinedIdentifierManager
import org.jetbrains.exposed.sql.vendors.ColumnMetadata
import java.math.BigDecimal

class RdbcDatabaseMetadataImpl(/*private val options: ConnectionFactoryOptions, */val metadata: ConnectionMetadata) :
    ExposedDatabaseMetadata("") {
    override val url: String
        get() = "" // options.getValue(HOST)!!

    override val version: BigDecimal
        get() = BigDecimal(metadata.databaseVersion)

    override val databaseDialectName: String
        get() = metadata.databaseProductName

    override val databaseProductVersion: String
        get() = metadata.databaseVersion

    override val defaultIsolationLevel: Int = unsupportedByRdbcDriver()

    override val supportsAlterTableWithAddColumn: Boolean = unsupportedByRdbcDriver()

    override val supportsMultipleResultSets: Boolean = unsupportedByRdbcDriver()

    override val supportsSelectForUpdate: Boolean = unsupportedByRdbcDriver()

    override val currentScheme: String = unsupportedByRdbcDriver()

    override fun resetCurrentScheme() = unsupportedByRdbcDriver()

    override val tableNames: Map<String, List<String>> = unsupportedByRdbcDriver()

    override val schemaNames: List<String> = unsupportedByRdbcDriver()

    override fun columns(vararg tables: Table): Map<Table, List<ColumnMetadata>> = unsupportedByRdbcDriver()

    override fun existingIndices(vararg tables: Table): Map<Table, List<Index>> = unsupportedByRdbcDriver()

    override fun tableConstraints(tables: List<Table>): Map<String, List<ForeignKeyConstraint>> = unsupportedByRdbcDriver()

    override fun cleanCache() = unsupportedByRdbcDriver()

    override val identifierManager: IdentifierManagerApi
        get() = when (metadata.databaseProductName) {
            "H2" -> H2PredefinedIdentifierManager
            "PostgreSQL" -> PostgrePredefinedIdentifierManager
            else -> error("Unsupported driver ${metadata.databaseProductName}")
        }
}
