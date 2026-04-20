package com.starrocks.connector.kafka.json;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.TimeZone;

import static com.starrocks.connector.kafka.json.Converters.*;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConvertersTest {
    private static final LocalDate DATE = LocalDate.of(2024, 6, 15);
    private static final LocalTime TIME = LocalTime.of(1, 23, 45, 192837465);
    private static final LocalDateTime DATE_TIME = LocalDateTime.of(2024, 6, 15, 1, 23, 45, 192837465);

    private static final LocalDate DATE_PRE_EPOCH = LocalDate.of(1969, 12, 31);
    private static final LocalDateTime DATE_TIME_PRE_EPOCH = LocalDateTime.of(1969, 12, 31, 1, 23, 45, 192837465);

    private static final ZoneOffset UTC7 = ZoneOffset.ofHours(7);
    private static final ZoneId HCM = ZoneId.of("Asia/Ho_Chi_Minh");

    // parameterized test data
    static final String[] timezones = {"UTC", "Asia/Ho_Chi_Minh"};

    @Nested
    class ToBigDecimal {
        @Test
        void bigDecimal() {
            var bd = new BigDecimal("123.456");
            assertEquals(bd, toBigDecimal(bd));

            double d = 123.456;
            assertEquals(BigDecimal.valueOf(d), toBigDecimal(d));

            long l = 123456789L;
            assertEquals(BigDecimal.valueOf(l), toBigDecimal(l));
        }

        @Test
        void bigInteger() {
            var bi = new BigInteger("99999999999999999999");
            assertEquals(new BigDecimal(bi), toBigDecimal(bi));
        }

        @Test
        void nonBigDecimal_throws() {
            assertThrows(DataException.class, () -> toBigDecimal("not-a-decimal"));
        }
    }

    @Nested
    class ToLocalDate {
        @Test
        void epochDayNumber() {
            assertEquals(LocalDate.EPOCH, toLocalDate(0));
            assertEquals(DATE, toLocalDate(DATE.toEpochDay()));
            assertEquals(DATE_PRE_EPOCH, toLocalDate(DATE_PRE_EPOCH.toEpochDay()));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void sqlTime_throws(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            assertThrows(DataException.class, () -> toLocalDate(new java.sql.Time(0)));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void utilDate(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var date = new java.util.Date(DATE_TIME.toInstant(UTC).toEpochMilli());
            assertEquals(DATE, toLocalDate(date));

            // Non-midnight time to expose floor-division bug: 1969-12-31T01:23:45Z
            date = new java.util.Date(DATE_TIME_PRE_EPOCH.toInstant(UTC).toEpochMilli());
            assertEquals(DATE_PRE_EPOCH, toLocalDate(date));
        }

        @Test
        void instant() {
            assertEquals(DATE, toLocalDate(DATE_TIME.toInstant(UTC)));
            assertEquals(DATE_PRE_EPOCH, toLocalDate(DATE_TIME_PRE_EPOCH.toInstant(UTC)));
        }

        @Test
        void localDate() {
            assertEquals(DATE, toLocalDate(DATE));
        }

        @Test
        void localDateTime_ignoresTime() {
            assertEquals(DATE, toLocalDate(DATE_TIME));
        }

        @Test
        void offsetDateTime() {
            // UTC+0 keeps the same date
            var offsetDt = OffsetDateTime.of(DATE_TIME, UTC);
            assertEquals(DATE, toLocalDate(offsetDt));
            offsetDt = OffsetDateTime.of(DATE_TIME_PRE_EPOCH, UTC);
            assertEquals(DATE_PRE_EPOCH, toLocalDate(offsetDt));

            // UTC+7: shifts back one calendar day
            // 2024-06-15T01:23:45+07:00 → UTC 2024-06-14
            offsetDt = OffsetDateTime.of(DATE_TIME, UTC7);
            assertEquals(DATE.minusDays(1), toLocalDate(offsetDt));

            // 1969-12-31T01:23:45+07:00 → UTC 1969-12-30
            offsetDt = OffsetDateTime.of(DATE_TIME_PRE_EPOCH, UTC7);
            assertEquals(DATE_PRE_EPOCH.minusDays(1), toLocalDate(offsetDt));
        }

        @Test
        void zonedDateTime() {
            // ICT (UTC+7): shifts back one calendar day
            var dt = ZonedDateTime.of(DATE_TIME, HCM);
            assertEquals(DATE.minusDays(1), toLocalDate(dt));

            // HCM was UTC+8 in 1969: 1969-12-31T01:23:45 → UTC 1969-12-30
            dt = ZonedDateTime.of(DATE_TIME_PRE_EPOCH, HCM);
            assertEquals(DATE_PRE_EPOCH.minusDays(1), toLocalDate(dt));
        }

        @Test
        void unsupportedType_throws() {
            assertThrows(DataException.class, () -> toLocalDate("unsupported"));
        }
    }

    @Nested
    class ToLocalTime {
        @Test
        void number() {
            var nano = TIME.toNanoOfDay();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(nano / MICRO, MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(nano / MILLI, MILLI));

            var dt = DATE_TIME.withYear(1970).withMonth(1).withDayOfMonth(2);
            nano = dt.toEpochSecond(UTC) * SECOND + dt.getNano();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(nano / MICRO, MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(nano / MILLI, MILLI));

            dt = DATE_TIME_PRE_EPOCH;
            nano = dt.toEpochSecond(UTC) * SECOND + dt.getNano();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(Math.floorDiv(nano, MICRO), MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(Math.floorDiv(nano, MILLI), MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void sqlDate_throws(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            assertThrows(DataException.class, () -> toLocalTime(new java.sql.Date(0), MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void sqlTime(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var time = new java.sql.Time(TIME.toNanoOfDay() / MILLI);
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(time, MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void sqlTimestamp(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var ts = new java.sql.Timestamp(TIME.toNanoOfDay() / MILLI);
            ts.setNanos(TIME.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));

            var i = DATE_TIME.toInstant(UTC);
            ts = new java.sql.Timestamp(i.toEpochMilli());
            ts.setNanos(i.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));

            i = DATE_TIME_PRE_EPOCH.toInstant(UTC);
            ts = new java.sql.Timestamp(i.toEpochMilli());
            ts.setNanos(i.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void utilDate(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var ts = new java.util.Date(TIME.toNanoOfDay() / MILLI);
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));

            var i = DATE_TIME.toInstant(UTC);
            ts = new java.util.Date(i.toEpochMilli());
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));

            i = DATE_TIME_PRE_EPOCH.toInstant(UTC);
            ts = new java.util.Date(i.toEpochMilli());
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));
        }

        @Test
        void instant() {
            assertEquals(TIME, toLocalTime(DATE_TIME.toInstant(UTC), MILLI));
            assertEquals(TIME, toLocalTime(DATE_TIME_PRE_EPOCH.toInstant(UTC), MILLI));
        }

        @Test
        void localTime() {
            assertEquals(TIME, toLocalTime(TIME, MILLI));
        }

        @Test
        void localDateTime() {
            assertEquals(TIME, toLocalTime(DATE_TIME, MILLI));
        }

        @Test
        void offsetTime() {
            // UTC+0: no shift
            var time = OffsetTime.of(TIME, UTC);
            assertEquals(TIME, toLocalTime(time, MILLI));

            // UTC+7: 01:23:45+07:00 → 18:23:45 UTC (previous clock cycle)
            time = OffsetTime.of(TIME, UTC7);
            assertEquals(TIME.minusHours(7), toLocalTime(time, MILLI));
        }

        @Test
        void offsetDateTime() {
            // UTC+0: no shift
            var dt = OffsetDateTime.of(DATE_TIME, UTC);
            assertEquals(TIME, toLocalTime(dt, MILLI));
            dt = OffsetDateTime.of(DATE_TIME_PRE_EPOCH, UTC);
            assertEquals(TIME, toLocalTime(dt, MILLI));

            // UTC+7: time shifts back 7 hours
            dt = OffsetDateTime.of(DATE_TIME, UTC7);
            assertEquals(TIME.minusHours(7), toLocalTime(dt, MILLI));
            dt = OffsetDateTime.of(DATE_TIME_PRE_EPOCH, UTC7);
            assertEquals(TIME.minusHours(7), toLocalTime(dt, MILLI));
        }

        @Test
        void zonedDateTime() {
            var zone = ZoneId.of("Asia/Tokyo");

            var dt = ZonedDateTime.of(DATE_TIME, zone);
            assertEquals(TIME.minusHours(9), toLocalTime(dt, MILLI));

            dt = ZonedDateTime.of(DATE_TIME_PRE_EPOCH, zone);
            assertEquals(TIME.minusHours(9), toLocalTime(dt, MILLI));
        }

        @Test
        void duration() {
            var duration = Duration.ofHours(1).plusMinutes(23).plusSeconds(45).plusNanos(192837465);
            assertEquals(TIME, toLocalTime(duration, MILLI));
        }

        @Test
        void unsupportedType_throws() {
            assertThrows(DataException.class, () -> toLocalTime("unsupported", MILLI));
        }
    }

    @Nested
    class ToLocalDateTime {
        @Test
        void number() {
            var i = DATE_TIME.toInstant(UTC);
            assertEquals(DATE_TIME.truncatedTo(MILLIS), toLocalDateTime(i.toEpochMilli(), MILLI));
            assertEquals(DATE_TIME.truncatedTo(MICROS), toLocalDateTime(i.getEpochSecond() * (SECOND / MICRO) + DATE_TIME.getNano() / MICRO, MICRO));
            assertEquals(DATE_TIME, toLocalDateTime(i.getEpochSecond() * SECOND + DATE_TIME.getNano(), NANO));

            i = DATE_TIME_PRE_EPOCH.toInstant(UTC);
            assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MILLIS), toLocalDateTime(i.toEpochMilli(), MILLI));
            assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MICROS), toLocalDateTime(i.getEpochSecond() * (SECOND / MICRO) + DATE_TIME_PRE_EPOCH.getNano() / MICRO, MICRO));
            assertEquals(DATE_TIME_PRE_EPOCH, toLocalDateTime(i.getEpochSecond() * SECOND + DATE_TIME_PRE_EPOCH.getNano(), NANO));
        }

        @Test
        void instant() {
            assertEquals(DATE_TIME, toLocalDateTime(DATE_TIME.toInstant(UTC), MILLI));
            assertEquals(DATE_TIME_PRE_EPOCH, toLocalDateTime(DATE_TIME_PRE_EPOCH.toInstant(UTC), MILLI));
        }

        @Test
        void localDate_startOfDay() {
            assertEquals(DATE.atStartOfDay(), toLocalDateTime(DATE, MILLI));
        }

        @Test
        void localTime_epochDate() {
            assertEquals(LocalDateTime.of(LocalDate.EPOCH, TIME), toLocalDateTime(TIME, MILLI));
        }

        @Test
        void localDateTime() {
            assertEquals(DATE_TIME, toLocalDateTime(DATE_TIME, MILLI));
        }

        @Test
        void offsetTime() {
            // UTC+0: date stays at EPOCH
            var time = OffsetTime.of(TIME, UTC);
            assertEquals(LocalDateTime.of(LocalDate.EPOCH, TIME), toLocalDateTime(time, MILLI));

            // UTC+7: 01:23:45+07:00 → 1969-12-31T18:23:45Z (crosses midnight backwards)
            var expected = LocalDateTime.of(
                    LocalDate.of(1969, 12, 31),
                    TIME.minusHours(7)
            );
            time = OffsetTime.of(TIME, UTC7);
            assertEquals(expected, toLocalDateTime(time, MILLI));
        }

        @Test
        void offsetDateTime() {
            var dt = OffsetDateTime.of(DATE_TIME, UTC7);
            assertEquals(DATE_TIME.minusHours(7), toLocalDateTime(dt, MILLI));

            dt = OffsetDateTime.of(DATE_TIME_PRE_EPOCH, UTC7);
            assertEquals(DATE_TIME_PRE_EPOCH.minusHours(7), toLocalDateTime(dt, MILLI));
        }

        @Test
        void zonedDateTime() {
            // ICT (UTC+7): shifts back 7 hours
            var dt = ZonedDateTime.of(DATE_TIME, HCM);
            assertEquals(DATE_TIME.minusHours(7), toLocalDateTime(dt, MILLI));

            // Asia/Ho_Chi_Minh was UTC+8 in 1969, not UTC+7
            dt = ZonedDateTime.of(DATE_TIME_PRE_EPOCH, HCM);
            assertEquals(DATE_TIME_PRE_EPOCH.minusHours(8), toLocalDateTime(dt, MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void utilDate(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var date = new java.util.Date(DATE_TIME.toInstant(UTC).toEpochMilli());
            assertEquals(DATE_TIME.truncatedTo(MILLIS), toLocalDateTime(date, MILLI));

            date = new java.util.Date(DATE_TIME_PRE_EPOCH.toInstant(UTC).toEpochMilli());
            assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MILLIS), toLocalDateTime(date, MILLI));
        }

        @ParameterizedTest(name = "{0}")
        @FieldSource("com.starrocks.connector.kafka.json.ConvertersTest#timezones")
        void sqlTimestamp(String tzname) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzname));
            var ts = new java.sql.Timestamp(DATE_TIME.toInstant(UTC).toEpochMilli());
            ts.setNanos(DATE_TIME.getNano());
            assertEquals(DATE_TIME, toLocalDateTime(ts, MILLI));

            ts = new java.sql.Timestamp(DATE_TIME_PRE_EPOCH.toInstant(UTC).toEpochMilli());
            ts.setNanos(DATE_TIME_PRE_EPOCH.getNano());
            assertEquals(DATE_TIME_PRE_EPOCH, toLocalDateTime(ts, MILLI));
        }

        @Test
        void unsupportedType_throws() {
            assertThrows(DataException.class, () -> toLocalDateTime("unsupported", MILLI));
        }
    }
}
