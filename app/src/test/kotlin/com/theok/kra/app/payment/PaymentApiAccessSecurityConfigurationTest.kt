package com.theok.kra.app.payment

import com.sun.net.httpserver.HttpServer
import com.theok.kra.app.testsupport.payment.PaymentApplicationFixture
import com.theok.kra.shell.payment.configuration.PaymentApiAccessMode
import com.theok.kra.shell.payment.configuration.PaymentApiAccessProperties
import com.theok.kra.shell.payment.configuration.PaymentApiAccessSecurityConfiguration
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.springframework.mock.env.MockEnvironment
import org.testcontainers.DockerClientFactory

internal class PaymentApiAccessSecurityConfigurationTest {
  @Test
  fun `trusted demo mode fails without an explicit trusted environment profile`() {
    val environment = MockEnvironment().apply { setActiveProfiles("payments") }

    assertFailsWith<IllegalStateException> {
      PaymentApiAccessSecurityConfiguration(
          PaymentApiAccessProperties(mode = PaymentApiAccessMode.TRUSTED_DEMO),
          environment,
      )
    }
  }

  @Test
  fun `default OIDC mode fails to start without an issuer configuration or decoder`() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    val fixture = PaymentApplicationFixture(includePaymentApiAccessTestJwtDecoder = false)

    try {
      val failure = assertFailsWith<Exception> { fixture.start() }
      val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }
      assertTrue(messages.any { "ReactiveJwtDecoder" in it }, failure.stackTraceToString())
    } finally {
      fixture.close()
    }
  }

  @Test
  fun `default OIDC mode starts with issuer configuration and no test decoder`() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    val issuer = startIssuer()
    val issuerUri = "http://127.0.0.1:${issuer.address.port}"
    val fixture =
        PaymentApplicationFixture(
            includePaymentApiAccessTestJwtDecoder = false,
            additionalApplicationArguments =
                listOf("--spring.security.oauth2.resourceserver.jwt.issuer-uri=$issuerUri"),
        )

    try {
      fixture.start()
    } finally {
      fixture.close()
      issuer.stop(0)
    }
  }

  private fun startIssuer(): HttpServer {
    val issuer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    issuer.createContext("/.well-known/openid-configuration") { exchange ->
      val issuerUri = "http://127.0.0.1:${issuer.address.port}"
      val payload = "{\"issuer\":\"$issuerUri\",\"jwks_uri\":\"$issuerUri/keys\"}".toByteArray()
      exchange.responseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(200, payload.size.toLong())
      exchange.responseBody.use { it.write(payload) }
    }
    issuer.createContext("/keys") { exchange ->
      val payload = "{\"keys\":[]}".toByteArray()
      exchange.responseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(200, payload.size.toLong())
      exchange.responseBody.use { it.write(payload) }
    }
    issuer.start()
    return issuer
  }
}
