package io.qalipsis.plugins.r2dbc.jasync.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.containsExactly
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.assertions.prop
import com.github.jasync.sql.db.SSLConfiguration
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.plugins.r2dbc.jasync.JasyncConnection
import io.qalipsis.plugins.r2dbc.jasync.dialect.Protocol
import io.qalipsis.plugins.r2dbc.jasync.r2dbcJasync
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.LocalDate

/**
 *
 * @author Eric Jessé
 */
internal class JasyncPollStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.r2dbcJasync().poll {
            name = "my-step"
            protocol(Protocol.POSTGRESQL)
            connection {
                port = 1234
                database = "my-database"
                username = "my-username"
            }
            query("This is my query")
            parameters(123, "my-param", true, LocalDate.of(2020, 11, 13))
            tieBreaker("my-tie-breaker")
            pollDelay(Duration.ofSeconds(12))
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(JasyncPollStepSpecificationImpl::class).all {
            prop(JasyncPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(JasyncPollStepSpecificationImpl::protocol).isEqualTo(Protocol.POSTGRESQL)
            prop(JasyncPollStepSpecificationImpl::connection).all {
                prop(JasyncConnection::host).isEqualTo("localhost")
                prop(JasyncConnection::port).isEqualTo(1234)
                prop(JasyncConnection::database).isEqualTo("my-database")
                prop(JasyncConnection::username).isEqualTo("my-username")
                prop(JasyncConnection::password).isNull()
                prop(JasyncConnection::queryTimeout).isNull()
                prop(JasyncConnection::ssl).all {
                    prop(SSLConfiguration::mode).isEqualTo(SSLConfiguration.Mode.Disable)
                    prop(SSLConfiguration::rootCert).isNull()
                }
                prop(JasyncConnection::charset).isEqualTo(StandardCharsets.UTF_8)
                prop(JasyncConnection::maximumMessageSize).isEqualTo(16777216)
            }
            prop(JasyncPollStepSpecificationImpl::query).isEqualTo("This is my query")
            prop(JasyncPollStepSpecificationImpl::parameters).all {
                hasSize(4)
                containsExactly(123, "my-param", true, LocalDate.of(2020, 11, 13))
            }
            prop(JasyncPollStepSpecificationImpl::tieBreaker).isEqualTo("my-tie-breaker")
            prop(JasyncPollStepSpecificationImpl::strictTieBreaker).isFalse()
            prop(JasyncPollStepSpecificationImpl::pollDelay).isEqualTo(Duration.ofSeconds(12))
            prop(JasyncPollStepSpecificationImpl::metrics).all {
                prop(JasyncPollMetricsConfiguration::recordsCount).isFalse()
            }
            prop(JasyncPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }
    }


    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        val rootCert = File("root-cert")
        scenario.r2dbcJasync().poll {
            name = "my-other-step"
            protocol(Protocol.MARIADB)
            connection {
                host = "my-server"
                port = 5678
                database = "my-other-database"
                username = "my-other-username"
                password = "my-other-password"
                queryTimeout = Duration.ofSeconds(30)
                ssl = SSLConfiguration(SSLConfiguration.Mode.Prefer, rootCert)
                charset = StandardCharsets.ISO_8859_1
                maximumMessageSize = 151424
            }
            query("This is my other query")
            parameters()
            tieBreaker("my-other-tie-breaker", true)
            pollDelay(Duration.ofSeconds(23))
            metrics {
                recordsCount = true
            }
            broadcast(123, Duration.ofSeconds(20))
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(JasyncPollStepSpecificationImpl::class).all {
            prop(JasyncPollStepSpecificationImpl::name).isEqualTo("my-other-step")
            prop(JasyncPollStepSpecificationImpl::protocol).isEqualTo(Protocol.MARIADB)
            prop(JasyncPollStepSpecificationImpl::connection).all {
                prop(JasyncConnection::host).isEqualTo("my-server")
                prop(JasyncConnection::port).isEqualTo(5678)
                prop(JasyncConnection::database).isEqualTo("my-other-database")
                prop(JasyncConnection::username).isEqualTo("my-other-username")
                prop(JasyncConnection::password).isEqualTo("my-other-password")
                prop(JasyncConnection::queryTimeout).isEqualTo(Duration.ofSeconds(30))
                prop(JasyncConnection::ssl).all {
                    prop(SSLConfiguration::mode).isEqualTo(SSLConfiguration.Mode.Prefer)
                    prop(SSLConfiguration::rootCert).isSameAs(rootCert)
                }
                prop(JasyncConnection::charset).isEqualTo(StandardCharsets.ISO_8859_1)
                prop(JasyncConnection::maximumMessageSize).isEqualTo(151424)
            }
            prop(JasyncPollStepSpecificationImpl::query).isEqualTo("This is my other query")
            prop(JasyncPollStepSpecificationImpl::parameters).hasSize(0)
            prop(JasyncPollStepSpecificationImpl::tieBreaker).isEqualTo("my-other-tie-breaker")
            prop(JasyncPollStepSpecificationImpl::strictTieBreaker).isTrue()
            prop(JasyncPollStepSpecificationImpl::pollDelay).isEqualTo(Duration.ofSeconds(23))
            prop(JasyncPollStepSpecificationImpl::metrics).all {
                prop(JasyncPollMetricsConfiguration::recordsCount).isTrue()
            }
            prop(JasyncPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }
}
