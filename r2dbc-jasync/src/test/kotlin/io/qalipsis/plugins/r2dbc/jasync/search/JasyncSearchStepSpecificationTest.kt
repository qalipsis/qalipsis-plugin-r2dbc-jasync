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

package io.qalipsis.plugins.r2dbc.jasync.search

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.github.jasync.sql.db.SSLConfiguration
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.r2dbc.jasync.JasyncConnection
import io.qalipsis.plugins.r2dbc.jasync.dialect.Protocol
import io.qalipsis.plugins.r2dbc.jasync.r2dbcJasync
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * @author Fiodar Hmyza
 */
internal class JasyncSearchStepSpecificationTest {

    @Test
    internal fun `should add minimal specification to the step`() {
        val previousStep = DummyStepSpecification()
        val queryFactory: suspend (ctx: StepContext<*, *>, input: Int) -> String = relaxedMockk()
        previousStep.r2dbcJasync().search {
            protocol(Protocol.POSTGRESQL)
            query(queryFactory)
            connection {
                host = "my-server"
                port = 5678
                database = "my-other-database"
                username = "my-other-username"
                password = "my-other-password"
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(JasyncSearchStepSpecificationImpl::class).all {
            prop(JasyncSearchStepSpecificationImpl<*>::name).isEmpty()
            prop(JasyncSearchStepSpecificationImpl<*>::protocol).isEqualTo(Protocol.POSTGRESQL)
            prop(JasyncSearchStepSpecificationImpl<*>::queryFactory).isEqualTo(queryFactory)
            prop(JasyncSearchStepSpecificationImpl<*>::parametersFactory).isNull()
            prop(JasyncSearchStepSpecificationImpl<*>::connection).all {
                prop(JasyncConnection::host).isEqualTo("my-server")
                prop(JasyncConnection::port).isEqualTo(5678)
                prop(JasyncConnection::database).isEqualTo("my-other-database")
                prop(JasyncConnection::username).isEqualTo("my-other-username")
                prop(JasyncConnection::password).isEqualTo("my-other-password")
                prop(JasyncConnection::queryTimeout).isNull()
                prop(JasyncConnection::ssl).all {
                    prop(SSLConfiguration::mode).isEqualTo(SSLConfiguration.Mode.Disable)
                    prop(SSLConfiguration::rootCert).isNull()
                }
                prop(JasyncConnection::charset).isEqualTo(StandardCharsets.UTF_8)
                prop(JasyncConnection::maximumMessageSize).isEqualTo(16777216)
            }
            prop(JasyncSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }
    }


    @Test
    internal fun `should add a complete specification to the step with monitoring`() {
        val previousStep = DummyStepSpecification()
        val queryFactory: suspend (ctx: StepContext<*, *>, input: Int) -> String = relaxedMockk()
        val paramsFactory: suspend (ctx: StepContext<*, *>, input: Int) -> List<*> = relaxedMockk()
        val rootCert = File("root-cert")
        previousStep.r2dbcJasync().search {
            name = "my-other-step"
            protocol(Protocol.MYSQL)
            connection {
                host = "my-server"
                port = 5678
                database = "my-other-database"
                username = "my-other-username"
                password = "my-other-password"
                queryTimeout = Duration.ofSeconds(60)
                ssl = SSLConfiguration(SSLConfiguration.Mode.Prefer, rootCert)
                charset = StandardCharsets.ISO_8859_1
                maximumMessageSize = 151424
            }
            query(queryFactory)
            parameters(paramsFactory)
            monitoring {
                meters = true
                events = false
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(JasyncSearchStepSpecificationImpl::class).all {
            prop(JasyncSearchStepSpecificationImpl<*>::name).isEqualTo("my-other-step")
            prop(JasyncSearchStepSpecificationImpl<*>::protocol).isEqualTo(Protocol.MYSQL)
            prop(JasyncSearchStepSpecificationImpl<*>::queryFactory).isEqualTo(queryFactory)
            prop(JasyncSearchStepSpecificationImpl<*>::parametersFactory).isEqualTo(paramsFactory)
            prop(JasyncSearchStepSpecificationImpl<*>::connection).all {
                prop(JasyncConnection::host).isEqualTo("my-server")
                prop(JasyncConnection::port).isEqualTo(5678)
                prop(JasyncConnection::database).isEqualTo("my-other-database")
                prop(JasyncConnection::username).isEqualTo("my-other-username")
                prop(JasyncConnection::password).isEqualTo("my-other-password")
                prop(JasyncConnection::queryTimeout).isEqualTo(Duration.ofSeconds(60))
                prop(JasyncConnection::ssl).all {
                    prop(SSLConfiguration::mode).isEqualTo(SSLConfiguration.Mode.Prefer)
                    prop(SSLConfiguration::rootCert).isSameAs(rootCert)
                }
                prop(JasyncConnection::charset).isEqualTo(StandardCharsets.ISO_8859_1)
                prop(JasyncConnection::maximumMessageSize).isEqualTo(151424)
            }
            prop(JasyncSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
        }
    }
    @Test
    internal fun `should add a complete specification to the step with logger`() {
        val previousStep = DummyStepSpecification()
        val queryFactory: suspend (ctx: StepContext<*, *>, input: Int) -> String = relaxedMockk()
        val paramsFactory: suspend (ctx: StepContext<*, *>, input: Int) -> List<*> = relaxedMockk()
        val rootCert = File("root-cert")
        previousStep.r2dbcJasync().search {
            name = "my-other-step"
            protocol(Protocol.MYSQL)
            connection {
                host = "my-server"
                port = 5678
                database = "my-other-database"
                username = "my-other-username"
                password = "my-other-password"
                queryTimeout = Duration.ofSeconds(60)
                ssl = SSLConfiguration(SSLConfiguration.Mode.Prefer, rootCert)
                charset = StandardCharsets.ISO_8859_1
                maximumMessageSize = 151424
            }
            query(queryFactory)
            parameters(paramsFactory)
            monitoring {
                meters = false
                events = true
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(JasyncSearchStepSpecificationImpl::class).all {
            prop(JasyncSearchStepSpecificationImpl<*>::name).isEqualTo("my-other-step")
            prop(JasyncSearchStepSpecificationImpl<*>::protocol).isEqualTo(Protocol.MYSQL)
            prop(JasyncSearchStepSpecificationImpl<*>::queryFactory).isEqualTo(queryFactory)
            prop(JasyncSearchStepSpecificationImpl<*>::parametersFactory).isEqualTo(paramsFactory)
            prop(JasyncSearchStepSpecificationImpl<*>::connection).all {
                prop(JasyncConnection::host).isEqualTo("my-server")
                prop(JasyncConnection::port).isEqualTo(5678)
                prop(JasyncConnection::database).isEqualTo("my-other-database")
                prop(JasyncConnection::username).isEqualTo("my-other-username")
                prop(JasyncConnection::password).isEqualTo("my-other-password")
                prop(JasyncConnection::queryTimeout).isEqualTo(Duration.ofSeconds(60))
                prop(JasyncConnection::ssl).all {
                    prop(SSLConfiguration::mode).isEqualTo(SSLConfiguration.Mode.Prefer)
                    prop(SSLConfiguration::rootCert).isSameAs(rootCert)
                }
                prop(JasyncConnection::charset).isEqualTo(StandardCharsets.ISO_8859_1)
                prop(JasyncConnection::maximumMessageSize).isEqualTo(151424)
            }
            prop(JasyncSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }
    }

}
