package org.jetbrains.exposed.sql.statements.api.identifiers

import org.jetbrains.exposed.sql.statements.api.IdentifierManagerApi

object PostgrePredefinedIdentifierManager : IdentifierManagerApi() {
    override val quoteString: String = "\""
    override val isUpperCaseIdentifiers: Boolean = false
    override val isUpperCaseQuotedIdentifiers: Boolean = false
    override val isLowerCaseIdentifiers: Boolean = true
    override val isLowerCaseQuotedIdentifiers: Boolean = false
    override val supportsMixedIdentifiers: Boolean = false
    override val supportsMixedQuotedIdentifiers: Boolean = true

    override fun dbKeywords(): List<String> = emptyList()

    override val extraNameCharacters: String = ""
    override val oracleVersion: OracleVersion = OracleVersion.NonOracle
    override val maxColumnNameLength: Int = 63
}