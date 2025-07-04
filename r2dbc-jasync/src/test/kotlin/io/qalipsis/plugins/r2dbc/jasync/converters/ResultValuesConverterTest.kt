/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.r2dbc.jasync.converters

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import io.qalipsis.test.mockk.relaxedMockk
import org.joda.time.DateTimeZone
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

internal class ResultValuesConverterTest {

    val jodaToJavaConverter = ResultValuesConverter()

    @Test
    internal fun `should convert local date`() {
        val joda = org.joda.time.LocalDate(2020, 11, 6)
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(LocalDate.of(2020, 11, 6))
    }

    @Test
    internal fun `should convert local time`() {
        val joda = org.joda.time.LocalTime(11, 23, 56, 123)
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(LocalTime.of(11, 23, 56, 123_000_000))
    }

    @Test
    internal fun `should convert local date and time`() {
        val joda = org.joda.time.LocalDateTime(2020, 11, 6, 11, 23, 56, 123)
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(LocalDateTime.of(2020, 11, 6, 11, 23, 56, 123_000_000))
    }

    @Test
    internal fun `should convert date and time with offset`() {
        val joda = org.joda.time.DateTime.parse("2020-11-06T11:23:56.123+01:00")
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(
                ZonedDateTime.of(2020, 11, 6, 11, 23, 56, 123_000_000, ZoneId.ofOffset("", ZoneOffset.ofHours(1))))
    }

    @Test
    internal fun `should convert date and time with zone ID`() {
        val joda = org.joda.time.DateTime(2020, 11, 6, 11, 23, 56, 123, DateTimeZone.forID("CET"))
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(
                ZonedDateTime.of(2020, 11, 6, 11, 23, 56, 123_000_000, ZoneId.of("CET")))
    }

    @Test
    internal fun `should convert instant`() {
        val joda = org.joda.time.Instant(1604658236123)
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(Instant.ofEpochMilli(1604658236123))
    }

    @Test
    internal fun `should convert duration`() {
        val joda = org.joda.time.Duration(1604658236123)
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isEqualTo(Duration.ofMillis(1604658236123))
    }

    @Test
    internal fun `should keep null value`() {
        assertThat(jodaToJavaConverter.process(null)).isNull()
    }

    @Test
    internal fun `should keep non joda value unchanged`() {
        val joda = relaxedMockk<Any>()
        val java8 = jodaToJavaConverter.process(joda)
        assertThat(java8).isSameAs(joda)
    }

}
