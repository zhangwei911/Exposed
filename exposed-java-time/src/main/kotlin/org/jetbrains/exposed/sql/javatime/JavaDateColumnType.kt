package org.jetbrains.exposed.sql.javatime

import org.jetbrains.exposed.sql.Column
import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.IDateColumnType
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transactionScope
import org.jetbrains.exposed.sql.vendors.OracleDialect
import org.jetbrains.exposed.sql.vendors.SQLiteDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.sql.ResultSet
import java.sql.Time
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.*

internal val DEFAULT_DATE_STRING_FORMATTER by transactionScope {
    DateTimeFormatter.ISO_LOCAL_DATE.withLocale(Locale.ROOT).withZoneId(this)
}
internal val DEFAULT_DATE_TIME_STRING_FORMATTER by transactionScope {
    DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendPattern(" ")
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .toFormatter(Locale.ROOT)
        .withZoneId(this)
}
internal val SQLITE_AND_ORACLE_DATE_TIME_STRING_FORMATTER by transactionScope {
    DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .toFormatter(Locale.ROOT)
        .withZoneId(this)
}

internal val SQLITE_INSTANT_STRING_FORMATTER by transactionScope {
    SQLITE_AND_ORACLE_DATE_TIME_STRING_FORMATTER.withZone(ZoneId.of("UTC"))
}

internal val ORACLE_TIME_STRING_FORMATTER by transactionScope {
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT).withZoneId(this)
}

internal val DEFAULT_TIME_STRING_FORMATTER by transactionScope {
    DateTimeFormatter.ISO_LOCAL_TIME.withLocale(Locale.ROOT).withZoneId(this)
}

internal fun DateTimeFormatter.withZoneId(transaction: Transaction) = withZone(transaction.db.config.defaultTimeZone.toZoneId())

internal fun transactionZoneId() = TransactionManager.current().db.config.defaultTimeZone.toZoneId()

internal val LocalDate.millis get() = atStartOfDay(transactionZoneId()).toEpochSecond() * 1000

class JavaLocalDateColumnType : ColumnType(), IDateColumnType {
    override val hasTimePart: Boolean = false

    override fun sqlType(): String = "DATE"

    override fun nonNullValueToString(value: Any): String {
        val localDate = when (value) {
            is String -> return value
            is LocalDate -> value
            is java.sql.Date -> longToLocalDate(value.time)
            is java.sql.Timestamp -> longToLocalDate(value.time)
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        return "'${DEFAULT_DATE_STRING_FORMATTER.format(localDate)}'"
    }

    override fun valueFromDB(value: Any): Any = when (value) {
        is LocalDate -> value
        is java.sql.Date -> longToLocalDate(value.time)
        is java.sql.Timestamp -> longToLocalDate(value.time)
        is Int -> longToLocalDate(value.toLong())
        is Long -> longToLocalDate(value)
        is String -> when (currentDialect) {
            is SQLiteDialect -> LocalDate.parse(value)
            else -> value
        }
        else -> LocalDate.parse(value.toString())
    }

    override fun notNullValueToDB(value: Any) = when (value) {
        is LocalDate -> java.sql.Date(value.millis)
        else -> value
    }

    private fun longToLocalDate(instant: Long) = Instant.ofEpochMilli(instant).atZone(ZoneId.systemDefault()).toLocalDate()

    companion object {
        internal val INSTANCE = JavaLocalDateColumnType()
    }
}

class JavaLocalDateTimeColumnType : ColumnType(), IDateColumnType {
    override val hasTimePart: Boolean = true
    override fun sqlType(): String = currentDialect.dataTypeProvider.dateTimeType()

    override fun nonNullValueToString(value: Any): String {
        val localDateTime = when (value) {
            is String -> return value
            is LocalDateTime -> value
            is java.sql.Date -> longToLocalDateTime(value.time)
            is java.sql.Timestamp -> longToLocalDateTime(value.time, value.nanos.toLong())
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        return when (currentDialect) {
            is SQLiteDialect, is OracleDialect -> "'${SQLITE_AND_ORACLE_DATE_TIME_STRING_FORMATTER.format(localDateTime)}'"
            else -> "'${DEFAULT_DATE_TIME_STRING_FORMATTER.format(localDateTime)}'"
        }
    }

    override fun valueFromDB(value: Any): Any = when (value) {
        is LocalDateTime -> value
        is java.sql.Date -> longToLocalDateTime(value.time)
        is java.sql.Timestamp -> longToLocalDateTime(value.time / 1000, value.nanos.toLong())
        is Int -> longToLocalDateTime(value.toLong())
        is Long -> longToLocalDateTime(value)
        is String -> LocalDateTime.parse(value, DEFAULT_DATE_TIME_STRING_FORMATTER)
        else -> valueFromDB(value.toString())
    }

    override fun notNullValueToDB(value: Any): Any = when {
        value is LocalDateTime && currentDialect is SQLiteDialect ->
            SQLITE_AND_ORACLE_DATE_TIME_STRING_FORMATTER.format(value)
        value is LocalDateTime -> {
            val instant = value.atZone(ZoneId.systemDefault()).toInstant()
            java.sql.Timestamp(instant.toEpochMilli()).apply { nanos = instant.nano }
        }
        else -> value
    }

    private fun longToLocalDateTime(millis: Long) = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), transactionZoneId())
    private fun longToLocalDateTime(seconds: Long, nanos: Long) =
        LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), transactionZoneId())

    companion object {
        internal val INSTANCE = JavaLocalDateTimeColumnType()
    }
}

class JavaLocalTimeColumnType : ColumnType(), IDateColumnType {
    override val hasTimePart: Boolean = true

    override fun sqlType(): String = currentDialect.dataTypeProvider.timeType()

    override fun nonNullValueToString(value: Any): String {
        val instant = when (value) {
            is String -> return value
            is LocalTime -> value
            is Time -> value.toLocalTime()
            is java.sql.Timestamp -> value.toLocalDateTime().toLocalTime()
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        val formatter = if (currentDialect is OracleDialect) {
            ORACLE_TIME_STRING_FORMATTER
        } else {
            DEFAULT_TIME_STRING_FORMATTER
        }
        return "'${formatter.format(instant)}'"
    }

    override fun valueFromDB(value: Any): LocalTime = when (value) {
        is LocalTime -> value
        is Time -> value.toLocalTime()
        is java.sql.Timestamp -> value.toLocalDateTime().toLocalTime()
        is Int -> longToLocalTime(value.toLong())
        is Long -> longToLocalTime(value)
        is String -> LocalTime.parse(value, DEFAULT_TIME_STRING_FORMATTER)
        else -> valueFromDB(value.toString())
    }

    override fun notNullValueToDB(value: Any): Any = when (value) {
        is LocalTime -> Time.valueOf(value)
        else -> value
    }

    private fun longToLocalTime(millis: Long) = Time(millis).toLocalTime()

    companion object {
        internal val INSTANCE = JavaLocalTimeColumnType()
    }
}

class JavaInstantColumnType : ColumnType(), IDateColumnType {
    override val hasTimePart: Boolean = true
    override fun sqlType(): String = currentDialect.dataTypeProvider.dateTimeType()

    override fun nonNullValueToString(value: Any): String {
        val instant = when (value) {
            is String -> return value
            is Instant -> value
            is java.sql.Timestamp -> value.toInstant()
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        return when (currentDialect) {
            is OracleDialect -> "'${SQLITE_AND_ORACLE_DATE_TIME_STRING_FORMATTER.format(instant)}'"
            is SQLiteDialect -> "'${SQLITE_INSTANT_STRING_FORMATTER.format(instant)}'"
            else -> "'${DEFAULT_DATE_TIME_STRING_FORMATTER.format(instant)}'"
        }
    }

    override fun valueFromDB(value: Any): Instant = when (value) {
        is java.sql.Timestamp -> value.toInstant()
        is String -> Instant.parse(value)
        is Instant -> value
        else -> valueFromDB(value.toString())
    }

    override fun readObject(rs: ResultSet, index: Int): Any? {
        return if (currentDialect is SQLiteDialect) {
            rs.getString(index)?.let {
                SQLITE_INSTANT_STRING_FORMATTER.parse(it, Instant::from)
            }
        } else {
            rs.getTimestamp(index)
        }
    }

    override fun notNullValueToDB(value: Any): Any = when {
        value is Instant && currentDialect is SQLiteDialect ->
            SQLITE_INSTANT_STRING_FORMATTER.format(value)
        value is Instant ->
            java.sql.Timestamp.from(value)
        else -> value
    }

    companion object {
        internal val INSTANCE = JavaInstantColumnType()
    }
}

class JavaDurationColumnType : ColumnType() {
    override fun sqlType(): String = currentDialect.dataTypeProvider.longType()

    override fun nonNullValueToString(value: Any): String {
        val duration = when (value) {
            is String -> return value
            is Duration -> value
            is Long -> Duration.ofNanos(value)
            is Number -> Duration.ofNanos(value.toLong())
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        return "'${duration.toNanos()}'"
    }

    override fun valueFromDB(value: Any): Duration = when (value) {
        is Long -> Duration.ofNanos(value)
        is Number -> Duration.ofNanos(value.toLong())
        is String -> Duration.parse(value)
        else -> valueFromDB(value.toString())
    }

    override fun readObject(rs: ResultSet, index: Int): Any? {
        // ResultSet.getLong returns 0 instead of null
        return rs.getLong(index).takeIf { rs.getObject(index) != null }
    }

    override fun notNullValueToDB(value: Any): Any {
        if (value is Duration) {
            return value.toNanos()
        }
        return value
    }

    companion object {
        internal val INSTANCE = JavaDurationColumnType()
    }
}

/**
 * A date column to store a date.
 *
 * @param name The column name
 */
fun Table.date(name: String): Column<LocalDate> = registerColumn(name, JavaLocalDateColumnType())

/**
 * A datetime column to store both a date and a time.
 *
 * @param name The column name
 */
fun Table.datetime(name: String): Column<LocalDateTime> = registerColumn(name, JavaLocalDateTimeColumnType())

/**
 * A time column to store a time.
 *
 * Doesn't return nanos from database.
 *
 * @param name The column name
 * @author Maxim Vorotynsky
 */
fun Table.time(name: String): Column<LocalTime> = registerColumn(name, JavaLocalTimeColumnType())

/**
 * A timestamp column to store both a date and a time.
 *
 * @param name The column name
 */
fun Table.timestamp(name: String): Column<Instant> = registerColumn(name, JavaInstantColumnType())

/**
 * A date column to store a duration.
 *
 * @param name The column name
 */
fun Table.duration(name: String): Column<Duration> = registerColumn(name, JavaDurationColumnType())
