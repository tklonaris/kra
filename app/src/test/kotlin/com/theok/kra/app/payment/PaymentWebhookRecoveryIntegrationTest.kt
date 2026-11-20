package com.theok.kra.app.payment

import com.stripe.StripeClient
import com.stripe.exception.ApiException
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCancelParams
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import com.theok.kra.app.testsupport.payment.PaymentWebhookTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.commands.CancelPaymentResult
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_PROVIDER_EVENT
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito
import reactor.core.publisher.Flux

/** Covers recovery of an uncertain Stripe creation from a verified Stripe webhook event. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentWebhookRecoveryIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `active webhook recovers an uncertain creation before applying its status`() = runBlocking {
    val payment = registerPaymentForTest()
    val intentId = "pi_test_${UUID.randomUUID()}"
    markCreationUncertain(payment.paymentId)
    val webhookEvent = receiveWebhookEvent(payment.paymentId, intentId)

    val stripe = Mockito.mock(StripeClient::class.java)
    val services = Mockito.mock(V1Services::class.java)
    val intents = Mockito.mock(PaymentIntentService::class.java)
    val intent = recoveredIntent(payment.paymentId, intentId, "succeeded")
    Mockito.`when`(stripe.v1()).thenReturn(services)
    Mockito.`when`(services.paymentIntents()).thenReturn(intents)
    Mockito.`when`(intents.retrieve(intentId)).thenReturn(intent)

    assertTrue(workflowOperations(stripe).applyWebhook(payment.paymentId, webhookEvent).complete)
    val row =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(
                          PAYMENT.PROVIDER_PAYMENT_ID,
                          PAYMENT.INTENT_CREATION_STATE,
                          PAYMENT.STATUS,
                          PAYMENT.CLIENT_SECRET_CIPHERTEXT,
                          PAYMENT.RECONCILIATION_REQUIRED,
                      )
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
        }

    assertEquals(intentId, row.get(PAYMENT.PROVIDER_PAYMENT_ID))
    assertEquals("KNOWN", row.get(PAYMENT.INTENT_CREATION_STATE))
    assertEquals(PaymentStatus.SUCCEEDED.name, row.get(PAYMENT.STATUS))
    assertTrue(row.get(PAYMENT.CLIENT_SECRET_CIPHERTEXT) != null)
    assertEquals(false, row.get(PAYMENT.RECONCILIATION_REQUIRED))
    assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentSucceeded"))
  }

  @Test
  fun `late webhook recovery reconciles a durable cancellation after attaching its intent`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val intentId = "pi_test_${UUID.randomUUID()}"
        markCreationUncertain(payment.paymentId)
        assertEquals(
            CancelPaymentResult.Requested,
            fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId),
        )
        val webhookEvent = receiveWebhookEvent(payment.paymentId, intentId)

        val stripe = Mockito.mock(StripeClient::class.java)
        val services = Mockito.mock(V1Services::class.java)
        val intents = Mockito.mock(PaymentIntentService::class.java)
        val recoveredIntent =
            recoveredIntent(payment.paymentId, intentId, "requires_payment_method")
        val cancelledIntent =
            PaymentIntent().apply {
              id = intentId
              status = "canceled"
              cancellationReason = "requested_by_customer"
              amount = 100
              currency = "aud"
              metadata = mapOf("kra_payment_id" to payment.paymentId.toString())
            }
        Mockito.`when`(stripe.v1()).thenReturn(services)
        Mockito.`when`(services.paymentIntents()).thenReturn(intents)
        Mockito.`when`(intents.retrieve(intentId)).thenReturn(recoveredIntent)
        Mockito.`when`(
                intents.cancel(
                    Mockito.eq(intentId),
                    Mockito.any(PaymentIntentCancelParams::class.java),
                    Mockito.any(RequestOptions::class.java),
                ),
            )
            .thenReturn(cancelledIntent)

        workflowOperations(stripe).applyWebhook(payment.paymentId, webhookEvent)
        val row =
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(PAYMENT.STATUS, PAYMENT.TERMINAL_REASON)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(payment.paymentId)),
                  )
                  .single()
                  .awaitSingle()
            }

        assertEquals(PaymentStatus.CANCELLED.name, row.get(PAYMENT.STATUS))
        assertEquals(TerminalReason.CUSTOMER_CANCELLED.name, row.get(PAYMENT.TERMINAL_REASON))
        Mockito.verify(intents)
            .cancel(
                Mockito.eq(intentId),
                Mockito.any(PaymentIntentCancelParams::class.java),
                Mockito.any(RequestOptions::class.java),
            )
        Unit
      }

  @Test
  fun `timer recovery reuses an associated webhook after a failed wake-up`() = runBlocking {
    val payment = registerPaymentForTest()
    val intentId = "pi_test_${UUID.randomUUID()}"
    markCreationUncertain(payment.paymentId)
    val webhookEvent = receiveWebhookEvent(payment.paymentId, intentId)
    val stripe = Mockito.mock(StripeClient::class.java)
    val services = Mockito.mock(V1Services::class.java)
    val intents = Mockito.mock(PaymentIntentService::class.java)
    val recoveredIntent = recoveredIntent(payment.paymentId, intentId, "requires_payment_method")
    Mockito.`when`(stripe.v1()).thenReturn(services)
    Mockito.`when`(services.paymentIntents()).thenReturn(intents)
    Mockito.`when`(intents.retrieve(intentId))
        .thenThrow(ApiException("temporary retrieval failure", null, null, 500, null))
        .thenReturn(recoveredIntent)

    assertFailsWith<ApiException> {
      workflowOperations(stripe).applyWebhook(payment.paymentId, webhookEvent)
    }
    workflowOperations(stripe)
        .requireReconciliation(payment.paymentRequestId, "webhook retrieval exhausted")

    val control = workflowOperations(stripe).reconcile(payment.paymentRequestId)

    assertTrue(!control.reconciliationRequired)
    val eventAppliedAt =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT_PROVIDER_EVENT.APPLIED_AT)
                      .from(PAYMENT_PROVIDER_EVENT)
                      .where(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
              .get(PAYMENT_PROVIDER_EVENT.APPLIED_AT)
        }
    assertTrue(eventAppliedAt != null)
    assertEquals(intentId, providerIntentId(payment.paymentId))
  }

  @Test
  fun `timer reconciliation applies the webhook attached before a refresh failure`() = runBlocking {
    val payment = registerPaymentForTest()
    val intentId = "pi_test_${UUID.randomUUID()}"
    markCreationUncertain(payment.paymentId)
    receiveWebhookEvent(payment.paymentId, intentId)
    val stripe = Mockito.mock(StripeClient::class.java)
    val services = Mockito.mock(V1Services::class.java)
    val intents = Mockito.mock(PaymentIntentService::class.java)
    val intent = recoveredIntent(payment.paymentId, intentId, "requires_payment_method")
    Mockito.`when`(stripe.v1()).thenReturn(services)
    Mockito.`when`(services.paymentIntents()).thenReturn(intents)
    Mockito.`when`(intents.retrieve(intentId))
        .thenReturn(intent)
        .thenThrow(ApiException("temporary refresh failure", null, null, 500, null))
        .thenReturn(intent)

    assertFailsWith<ApiException> {
      workflowOperations(stripe).reconcile(payment.paymentRequestId)
    }
    workflowOperations(stripe)
        .requireReconciliation(payment.paymentRequestId, "recovered webhook refresh exhausted")

    val control = workflowOperations(stripe).reconcile(payment.paymentRequestId)

    assertTrue(!control.reconciliationRequired)
    val eventAppliedAt =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT_PROVIDER_EVENT.APPLIED_AT)
                      .from(PAYMENT_PROVIDER_EVENT)
                      .where(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
              .get(PAYMENT_PROVIDER_EVENT.APPLIED_AT)
        }
    assertTrue(eventAppliedAt != null)
  }

  private suspend fun markCreationUncertain(paymentId: UUID) {
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "UNCERTAIN")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "expired-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(PAYMENT.CREATION_ATTEMPTED_AT, OffsetDateTime.now(ZoneOffset.UTC))
                  .set(PAYMENT.RECONCILIATION_REQUIRED, true)
                  .set(
                      PAYMENT.RECONCILIATION_REASON,
                      "Stripe creation outcome requires reconciliation",
                  )
                  .where(PAYMENT.ID.eq(paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }
  }

  private suspend fun receiveWebhookEvent(paymentId: UUID, intentId: String): UUID {
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body = PaymentWebhookTestSupport.paymentIntentBody(stripeEventId, paymentId, intentId)
    fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body))
    val webhookEvent = fixture.database.webhookConsumerEvent(paymentId)
    fixture.eventPersister.receive(webhookEvent)
    return webhookEvent.consumerEventId
  }

  private suspend fun providerIntentId(paymentId: UUID): String =
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.select(PAYMENT.PROVIDER_PAYMENT_ID)
                    .from(PAYMENT)
                    .where(PAYMENT.ID.eq(paymentId)),
            )
            .single()
            .awaitSingle()
            .get(PAYMENT.PROVIDER_PAYMENT_ID) ?: error("Payment $paymentId has no provider intent")
      }

  private fun recoveredIntent(paymentId: UUID, intentId: String, status: String) =
      PaymentIntent().apply {
        id = intentId
        amount = 100
        currency = "aud"
        metadata = mapOf("kra_payment_id" to paymentId.toString())
        clientSecret = "${intentId}_secret_recovered"
        this.status = status
      }
}
