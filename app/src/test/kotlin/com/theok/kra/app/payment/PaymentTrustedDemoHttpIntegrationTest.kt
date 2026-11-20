package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentApplicationFixture
import com.theok.kra.shell.payment.configuration.PaymentApiAccessMode
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.testcontainers.DockerClientFactory

/** Verifies the deliberate local/test single-owner HTTP mode. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentTrustedDemoHttpIntegrationTest {
  private val fixture =
      PaymentApplicationFixture(
          webApplicationType = WebApplicationType.REACTIVE,
          paymentApiAccessModeOverride = PaymentApiAccessMode.TRUSTED_DEMO,
          trustedDemoOwnerOverride = null,
      )
  private val http = HttpClient.newHttpClient()

  @BeforeAll
  fun startApplication() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    fixture.start()
  }

  @AfterAll fun stopApplication() = fixture.close()

  @BeforeEach fun resetDatabase() = runBlocking { fixture.database.reset() }

  @Test
  fun `trusted demo mode uses the local default owner without a bearer token`() {
    val body =
        fixture.objectMapper
            .createObjectNode()
            .put("purchaseReference", UUID.randomUUID().toString())
            .put("amountMinor", 100)
            .put("currency", "AUD")
            .toString()
    val create =
        send(
            HttpRequest.newBuilder(URI.create("${fixture.applicationUrl}/payment-requests"))
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)),
        )
    assertEquals(202, create.statusCode(), create.body())
    val paymentRequestId =
        fixture.objectMapper.readTree(create.body()).required("paymentRequestId").asText()

    val details =
        send(
            HttpRequest.newBuilder(
                    URI.create("${fixture.applicationUrl}/payment-requests/$paymentRequestId"),
                )
                .GET(),
        )
    assertEquals(200, details.statusCode(), details.body())
    assertEquals(
        paymentRequestId,
        fixture.objectMapper.readTree(details.body()).required("paymentRequestId").asText(),
    )

    val setup =
        send(
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/setup",
                    ),
                )
                .GET(),
        )
    assertEquals(202, setup.statusCode(), setup.body())

    val cancellation =
        send(
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/cancellations",
                    ),
                )
                .POST(HttpRequest.BodyPublishers.noBody()),
        )
    assertEquals(202, cancellation.statusCode(), cancellation.body())
  }

  private fun send(request: HttpRequest.Builder): HttpResponse<String> =
      http.send(request.build(), HttpResponse.BodyHandlers.ofString())
}
