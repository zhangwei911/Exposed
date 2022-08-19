package org.jetbrains.exposed.sql.tests.shared

internal enum class Foo {
    Bar, Baz;

    override fun toString(): String = "Foo Enum ToString: $name"

    companion object {
        const val ENUM_PG_NAME = "FooEnum"
    }
}
