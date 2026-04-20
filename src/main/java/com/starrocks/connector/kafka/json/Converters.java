package com.starrocks.connector.kafka.json;

import com.starrocks.connector.kafka.json.JsonConverter.LogicalTypeConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static java.time.LocalDate.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.*;

public class Converters {
    private static final Duration ONE_DAY = Duration.ofDays(1);
    public static final long NANO = 1;
    public static final long MICRO = 1_000;
    public static final long MILLI = 1_000_000;
    public static final long SECOND = 1_000_000_000;

    public static LogicalTypeConverter convertDecimal() {
        return (schema, value, converter) -> {
            var decimal = toBigDecimal(value);
            return converter.nodeFactory().numberNode(decimal);
        };
    }

    public static BigDecimal toBigDecimal(Object value) {
        return switch (value) {
            case java.math.BigDecimal d -> d;
            case java.math.BigInteger i -> new BigDecimal(i);
            case java.lang.Number n -> BigDecimal.valueOf(n.doubleValue());
            default -> throw new DataException("Decimal: unsupported type " + value.getClass());
        };
    }

    /// Convert date to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Date
    /// @see io.debezium.time.Conversions#toLocalDate(Object)
    public static LogicalTypeConverter forDate() {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var date = toLocalDate(object);
            var s = ISO_LOCAL_DATE.format(date);
            return converter.nodeFactory().textNode(s);
        };
    }

    /// ignored the time part (if any) and return LocalDate in UTC
    public static LocalDate toLocalDate(Object value) {
        return switch (value) {
            // Assume the value is the epoch day number
            case java.lang.Number n -> LocalDate.ofEpochDay(n.longValue());
            case java.sql.Time t -> throw new DataException("Date: unsupported type java.sql.Time");
            case java.util.Date d -> {
                var n = Math.floorDiv(d.getTime(), ONE_DAY.toMillis());
                yield LocalDate.ofEpochDay(n);
            }
            case java.time.Instant i -> LocalDate.ofInstant(i, UTC);
            case java.time.LocalDate d -> d;
            case java.time.LocalDateTime dt -> dt.toLocalDate();
            case java.time.OffsetDateTime dt -> dt.withOffsetSameInstant(UTC)
                    .toLocalDate();
            case java.time.ZonedDateTime dt -> dt.withZoneSameInstant(UTC)
                    .toLocalDate();
            default -> throw new DataException("Date: unsupported type " + value.getClass());
        };
    }

    /// Convert time to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Time
    /// @see io.debezium.time.Conversions#toLocalTime(Object)
    public static LogicalTypeConverter forTime(long precision) {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var time = toLocalTime(object, precision);
            var s = ISO_LOCAL_TIME.format(time);
            return converter.nodeFactory().textNode(s);
        };
    }

    /// ignored the date part (if any) and return LocalTime in UTC
    public static LocalTime toLocalTime(Object value, long precision) {
        return switch (value) {
            case java.lang.Number n -> {
                var s = ONE_DAY.toSeconds() * (SECOND / precision);
                var nano = Math.floorMod(n.longValue(), s) * precision; // truncate the date part
                yield LocalTime.ofNanoOfDay(nano);
            }
            case java.sql.Date d -> throw new DataException("Time: unsupported type java.sql.Date");
            case java.sql.Timestamp ts -> {
                // use `toInstant()` to preserve nanoseconds
                var i = ts.toInstant();
                // Instant is not support get NANO_OF_DAY directly
                var nanos = Math.floorMod(i.getEpochSecond(), ONE_DAY.toSeconds()) * SECOND + i.getNano();
                yield LocalTime.ofNanoOfDay(nanos);
            }
            case java.util.Date d -> toLocalTime(d.getTime(), MILLI);
            case java.time.Instant i -> LocalTime.ofInstant(i, UTC);
            case java.time.LocalTime t -> t;
            case java.time.LocalDateTime dt -> dt.toLocalTime();
            case java.time.OffsetTime t -> t.withOffsetSameInstant(UTC)
                    .toLocalTime();
            case java.time.OffsetDateTime dt -> dt.withOffsetSameInstant(UTC)
                    .toLocalTime();
            case java.time.ZonedDateTime dt -> dt.withZoneSameInstant(UTC)
                    .toLocalTime();
            case java.time.Duration d -> LocalTime.ofNanoOfDay(d.toNanos());
            default -> throw new DataException("Time: unsupported type " + value.getClass());
        };
    }

    /// Convert timestamp/instant to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Timestamp
    /// @see io.debezium.time.Conversions#toLocalDateTime(Object)
    public static LogicalTypeConverter forTimestamp(long precision) {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var datetime = toLocalDateTime(object, precision);
            var s = ISO_LOCAL_DATE_TIME.format(datetime);
            return converter.nodeFactory().textNode(s);
        };
    }

    /// return LocalDateTime in UTC
    public static LocalDateTime toLocalDateTime(Object value, long precision) {
        return switch (value) {
            case java.lang.Number n -> {
                var mod = SECOND / precision;
                var seconds = Math.floorDiv(n.longValue(), mod);
                var nanos = Math.floorMod(n.longValue(), mod) * precision;
                yield LocalDateTime.ofEpochSecond(seconds, (int) nanos, UTC);
            }
            // use `toInstant()` to preserve nanoseconds
            case java.sql.Timestamp ts -> LocalDateTime.ofInstant(ts.toInstant(), UTC);
            case java.util.Date d -> toLocalDateTime(d.getTime(), MILLI);
            case java.time.Instant i -> LocalDateTime.ofInstant(i, UTC);
            case java.time.LocalDate d -> d.atStartOfDay();
            case java.time.LocalTime t -> t.atDate(EPOCH);
            case java.time.OffsetTime t -> t.atDate(EPOCH).withOffsetSameInstant(UTC)
                    .toLocalDateTime();
            case java.time.LocalDateTime dt -> dt;
            case java.time.OffsetDateTime dt -> dt.withOffsetSameInstant(UTC)
                    .toLocalDateTime();
            case java.time.ZonedDateTime dt -> dt.withZoneSameInstant(UTC)
                    .toLocalDateTime();
            default -> throw new DataException("Timestamp: unsupported type " + value.getClass());
        };
    }
}
