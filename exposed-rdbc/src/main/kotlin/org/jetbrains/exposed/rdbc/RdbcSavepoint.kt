package org.jetbrains.exposed.rdbc

import org.jetbrains.exposed.sql.statements.api.ExposedSavepoint

class RdbcSavepoint(name: String) : ExposedSavepoint(name)
