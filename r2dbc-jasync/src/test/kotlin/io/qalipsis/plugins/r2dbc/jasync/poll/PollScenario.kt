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

package io.qalipsis.plugins.r2dbc.jasync.poll

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.executionprofile.regular
import io.qalipsis.api.lang.concurrentList
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.filterNotNull
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.logErrors
import io.qalipsis.api.steps.map
import io.qalipsis.api.steps.onEach
import io.qalipsis.plugins.r2dbc.jasync.dialect.Protocol
import io.qalipsis.plugins.r2dbc.jasync.r2dbcJasync
import java.time.Duration
import java.time.LocalDateTime

/**
 *
 * Scenario to demo how the poll step can work. The scenario reads the entries in a building on one side and the exits
 * on the other side.
 *
 * Records related to the same person are joined and the duration is then printed out in the console.
 *
 * @author Eric Jessé
 */
object PollScenario {

    const val minions = 5

    val receivedMessages = concurrentList<String>()

    lateinit var protocol: Protocol

    var dbPort: Int = 0

    lateinit var dbUsername: String

    lateinit var dbPassword: String

    lateinit var dbName: String

    @Scenario("r2dbc-poll")
    fun pollData() {
        scenario {
            minionsCount = minions
            profile {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }
            .start()
            .r2dbcJasync()
            .poll {
                name = "poll.in"
                protocol(protocol)
                connection {
                    port = dbPort
                    username = dbUsername
                    password = dbPassword
                    database = dbName
                }
                query("""select username, "timestamp" from buildingentries where action = ? order by "timestamp"""")
                parameters("IN")
                pollDelay(Duration.ofSeconds(1))
            }.flatten()
            .logErrors()
            .map { UserEvent(it.value["username"] as String, it.value["timestamp"] as LocalDateTime) }
            .innerJoin()
            .using { it.value.username }
            .on {
                it.r2dbcJasync()
                    .poll {
                        name = "poll.out"
                        protocol(protocol)
                        connection {
                            port = dbPort
                            username = dbUsername
                            password = dbPassword
                            database = dbName
                        }
                        query("""select username, "timestamp" from buildingentries where action = ? order by "timestamp"""")
                        parameters("OUT")
                        pollDelay(Duration.ofSeconds(1))
                    }
                    .flatten()
                    .logErrors()
                    .map {
                        UserEvent(it.value["username"] as String, it.value["timestamp"] as LocalDateTime)
                    }
                    .configure {
                        name = "poll.out.output"
                    }
            }
            .having { it.value.username }
            .filterNotNull()
            .map {
                it.first.username to Duration.between(it.first.timestamp, it.second.timestamp)
            }
            .map { "The user ${it.first} stayed ${it.second.toMinutes()} minute(s) in the building" }
            .onEach { receivedMessages.add(it) }
            .onEach { println(it) }
    }

    data class UserEvent(val username: String, val timestamp: LocalDateTime)
}
