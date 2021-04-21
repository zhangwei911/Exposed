package org.jetbrains.exposed.sql.statements.api.identifiers

import org.jetbrains.exposed.sql.statements.api.IdentifierManagerApi

object H2PredefinedIdentifierManager : IdentifierManagerApi() {
    override val quoteString: String = "\""
    override val isUpperCaseIdentifiers: Boolean = true
    override val isUpperCaseQuotedIdentifiers: Boolean = false
    override val isLowerCaseIdentifiers: Boolean = false
    override val isLowerCaseQuotedIdentifiers: Boolean = false
    override val supportsMixedIdentifiers: Boolean = true
    override val supportsMixedQuotedIdentifiers: Boolean = true

    override fun dbKeywords(): List<String> = emptyList()

    override val extraNameCharacters: String = ""
    override val oracleVersion: OracleVersion = OracleVersion.NonOracle
    override val maxColumnNameLength: Int = 0
}
