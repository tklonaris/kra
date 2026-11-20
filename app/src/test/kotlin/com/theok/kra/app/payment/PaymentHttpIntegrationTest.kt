package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentApplicationFixture
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.workflow.operations.PaymentLifecycleReconciliationOperations
import com.theok.kra.shell.payment.workflow.operations.PaymentWorkflowActivityOperations
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.testcontainers.DockerClientFactory
import reactor.core.publisher.Flux

/** Verifies the payment controller's HTTP binding and problem-response contract. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentHttpIntegrationTest {
  // No access-mode override: this verifies the production default is OIDC.
  private val fixture = PaymentApplicationFixture(WebApplicationType.REACTIVE)
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
  fun `payment HTTP endpoints bind Kotlin requests and preserve idempotency`() {
    val requestId = UUID.randomUUID()
    val purchaseReference = UUID.randomUUID()
    val body =
        fixture.objectMapper
            .createObjectNode()
            .put("purchaseReference", purchaseReference.toString())
            .put("amountMinor", 100)
            .put("currency", "AUD")
            .toString()

    val first = create(OWNER_A, requestId, body)
    val firstJson = fixture.objectMapper.readTree(first.body())
    assertEquals(202, first.statusCode(), first.body())
    assertEquals("ACCEPTED", firstJson.required("status").asText())
    assertFalse(firstJson.has("stripePaymentIntentId"))
    assertFalse(firstJson.has("terminalReason"))
    assertFalse(firstJson.has("reconciliationRequired"))
    val paymentRequestId = firstJson.required("paymentRequestId").asText()

    val retry = create(OWNER_A, requestId, body)
    assertEquals(202, retry.statusCode(), retry.body())
    assertEquals(
        paymentRequestId,
        fixture.objectMapper.readTree(retry.body()).required("paymentRequestId").asText(),
    )

    val details =
        send(
            HttpRequest.newBuilder(
                    URI.create("${fixture.applicationUrl}/payment-requests/$paymentRequestId"),
                )
                .header("Authorization", "Bearer $OWNER_A")
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
                .header("Authorization", "Bearer $OWNER_A")
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
                .header("Authorization", "Bearer $OWNER_A")
                .POST(HttpRequest.BodyPublishers.noBody()),
        )
    assertEquals(202, cancellation.statusCode(), cancellation.body())

    val unavailableSetup =
        send(
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/setup",
                    ),
                )
                .header("Authorization", "Bearer $OWNER_A")
                .GET(),
        )
    assertEquals(409, unavailableSetup.statusCode(), unavailableSetup.body())
  }

  @Test
  fun `payment HTTP validation and webhook signature failures use client errors`() {
    val invalidPayment =
        fixture.objectMapper
            .createObjectNode()
            .put("purchaseReference", UUID.randomUUID().toString())
            .put("amountMinor", 100)
            .put("currency", "USD")
            .toString()
    val invalidPaymentResponse = create(OWNER_A, UUID.randomUUID(), invalidPayment)
    assertEquals(400, invalidPaymentResponse.statusCode(), invalidPaymentResponse.body())
    assertEquals(
        "request-failed",
        fixture.objectMapper.readTree(invalidPaymentResponse.body()).required("code").asText(),
    )

    val webhookBody = fixture.objectMapper.createObjectNode().toString()
    val invalidWebhookResponse =
        send(
            HttpRequest.newBuilder(
                    URI.create("${fixture.applicationUrl}/payment-requests/stripe/webhook"),
                )
                .header("Stripe-Signature", "invalid")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(webhookBody)),
        )
    assertEquals(400, invalidWebhookResponse.statusCode(), invalidWebhookResponse.body())
  }

  @Test
  fun `payment HTTP endpoints scope reads writes and idempotency to the authenticated owner`() {
    val requestId = UUID.randomUUID()
    val purchaseReference = UUID.randomUUID()
    val body =
        fixture.objectMapper
            .createObjectNode()
            .put("purchaseReference", purchaseReference.toString())
            .put("amountMinor", 100)
            .put("currency", "AUD")
            .toString()

    val ownerAPayment = create(OWNER_A, requestId, body)
    assertEquals(202, ownerAPayment.statusCode(), ownerAPayment.body())
    val paymentRequestId =
        fixture.objectMapper.readTree(ownerAPayment.body()).required("paymentRequestId").asText()

    listOf(
            HttpRequest.newBuilder(
                    URI.create("${fixture.applicationUrl}/payment-requests/$paymentRequestId"),
                )
                .header("Authorization", "Bearer $OWNER_B")
                .GET(),
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/setup",
                    ),
                )
                .header("Authorization", "Bearer $OWNER_B")
                .GET(),
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/cancellations",
                    ),
                )
                .header("Authorization", "Bearer $OWNER_B")
                .POST(HttpRequest.BodyPublishers.noBody()),
        )
        .forEach { request -> assertEquals(404, send(request).statusCode()) }

    val ownerBPayment = create(OWNER_B, requestId, body)
    assertEquals(202, ownerBPayment.statusCode(), ownerBPayment.body())
    assertFalse(
        fixture.objectMapper.readTree(ownerBPayment.body()).required("paymentRequestId").asText() ==
            paymentRequestId,
    )

    val unauthenticated =
        send(
            HttpRequest.newBuilder(
                    URI.create("${fixture.applicationUrl}/payment-requests/$paymentRequestId"),
                )
                .GET(),
        )
    assertEquals(401, unauthenticated.statusCode(), unauthenticated.body())
  }

  @Test
  fun `checkout setup is unavailable while provider reconciliation is active`() = runBlocking {
    val accepted = create(OWNER_A, UUID.randomUUID(), paymentRequestBody())
    val paymentRequestId =
        UUID.fromString(
            fixture.objectMapper.readTree(accepted.body()).required("paymentRequestId").asText(),
        )
    val stripe = PaymentWorkflowStripeFake()
    val operations =
        PaymentWorkflowActivityOperations(
            fixture.workflowTransactions,
            stripe.client,
            ClientSecretCipher(ByteArray(32)),
            PaymentLifecycleReconciliationOperations(fixture.workflowTransactions, stripe.client),
        )
    operations.initialise(paymentRequestId)
    operations.requireReconciliation(paymentRequestId, "temporary provider uncertainty")

    val setup =
        send(
            HttpRequest.newBuilder(
                    URI.create(
                        "${fixture.applicationUrl}/payment-requests/$paymentRequestId/setup",
                    ),
                )
                .header("Authorization", "Bearer $OWNER_A")
                .GET(),
        )

    assertEquals(409, setup.statusCode(), setup.body())
  }

  @Test
  fun `operational retry requires its dedicated JWT scope and enqueues failed requests`() =
      runBlocking {
        val requestId = UUID.randomUUID()
        val accepted = create(OWNER_A, requestId, paymentRequestBody())
        val paymentRequestId =
            UUID.fromString(
                fixture.objectMapper
                    .readTree(accepted.body())
                    .required("paymentRequestId")
                    .asText(),
            )
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.update(PAYMENT_REQUEST)
                      .set(PAYMENT_REQUEST.STATUS, "FAILED")
                      .where(PAYMENT_REQUEST.ID.eq(paymentRequestId)),
              )
              .single()
              .awaitSingle()
          Unit
        }

        assertEquals(401, retry(paymentRequestId, null).statusCode())
        assertEquals(403, retry(paymentRequestId, OWNER_A).statusCode())
        assertEquals(202, retry(paymentRequestId, "payment-operator").statusCode())
        assertEquals(
            1,
            fixture.database.paymentRequestOutboxCount(paymentRequestId, "PaymentRetryRequested"),
        )
      }

  private fun create(owner: String, requestId: UUID, body: String): HttpResponse<String> =
      send(
          HttpRequest.newBuilder(URI.create("${fixture.applicationUrl}/payment-requests"))
              .header("Authorization", "Bearer $owner")
              .header("Idempotency-Key", requestId.toString())
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body)),
      )

  private fun retry(paymentRequestId: UUID, bearerToken: String?): HttpResponse<String> {
    val request =
        HttpRequest.newBuilder(
                URI.create(
                    "${fixture.applicationUrl}/internal/payment-requests/$paymentRequestId/retries",
                ),
            )
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    fixture.objectMapper
                        .createObjectNode()
                        .put("reason", "operator recovery")
                        .toString(),
                ),
            )
    bearerToken?.let { request.header("Authorization", "Bearer $it") }
    return send(request)
  }

  private fun paymentRequestBody(): String =
      fixture.objectMapper
          .createObjectNode()
          .put("purchaseReference", UUID.randomUUID().toString())
          .put("amountMinor", 100)
          .put("currency", "AUD")
          .toString()

  private fun send(request: HttpRequest.Builder): HttpResponse<String> =
      http.send(request.build(), HttpResponse.BodyHandlers.ofString())

  private companion object {
    const val OWNER_A = "owner-a"
    const val OWNER_B = "owner-b"
  }
}
