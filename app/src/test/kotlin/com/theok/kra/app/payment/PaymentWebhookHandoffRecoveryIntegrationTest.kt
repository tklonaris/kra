package com.theok.kra.app.payment

import com.stripe.StripeClient
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCreateParams
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import com.theok.kra.app.testsupport.payment.PaymentWebhookTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEvent
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.delivery.WebhookAcceptance
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito
import reactor.core.publisher.Flux

/**
 * Covers durable webhook association and replacement hand-offs during uncertain intent creation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentWebhookHandoffRecoveryIntegrationTest :
    PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `late webhook recovery requires a Stripe event for the same payment`() = runBlocking {
    val payment = registerPaymentForTest()
    val otherPayment = registerPaymentForTest()
    markCreationUncertain(payment.paymentId)
    markCreationUncertain(otherPayment.paymentId)
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body =
        PaymentWebhookTestSupport.paymentIntentBody(
            stripeEventId,
            otherPayment.paymentId,
            "pi_test_${UUID.randomUUID()}",
        )
    assertEquals(
        WebhookAcceptance.Accepted,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    val delivery =
        PaymentConsumerEvent(
            consumerEventId = UUID.randomUUID(),
            paymentRequestId = payment.paymentRequestId,
            paymentId = null,
            eventType = PaymentEventType.WEBHOOK_RECEIVED,
            stripeEventId = stripeEventId,
        )
    assertFailsWith<Exception> { fixture.eventPersister.receive(delivery) }

    assertFalse(fixture.eventPersister.load(delivery.consumerEventId) != null)
  }

  @Test
  fun `uncertainty re-hands off a Stripe event consumed during creation`() = runBlocking {
    val payment = registerPaymentForTest()
    markCreationAttempting(payment.paymentId)
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body =
        PaymentWebhookTestSupport.paymentIntentBody(
            stripeEventId,
            payment.paymentId,
            "pi_test_${UUID.randomUUID()}",
        )
    assertEquals(
        WebhookAcceptance.Accepted,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    val delivery = fixture.database.webhookConsumerEvent(payment.paymentId)
    assertEquals(PaymentConsumerEventStatus.PENDING, fixture.eventPersister.receive(delivery))
    val stripe = Mockito.mock(StripeClient::class.java)
    workflowOperations(stripe).applyWebhook(payment.paymentId, delivery.consumerEventId)
    fixture.eventPersister.accepted(delivery.consumerEventId)

    workflowOperations(stripe)
        .requireReconciliation(payment.paymentRequestId, "local intent persistence failed")

    assertEquals(2, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
    Mockito.verifyNoInteractions(stripe)
  }

  @Test
  fun `successful intent persistence re-hands off a Stripe event consumed during creation`() =
      runBlocking {
        val payment = registerPaymentForTest()
        markCreationAttempting(payment.paymentId)
        val intentId = "pi_test_${UUID.randomUUID()}"
        val stripeEventId = "evt_test_${UUID.randomUUID()}"
        val body =
            PaymentWebhookTestSupport.paymentIntentBody(
                stripeEventId,
                payment.paymentId,
                intentId,
            )
        assertEquals(
            WebhookAcceptance.Accepted,
            fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
        )
        val delivery = fixture.database.webhookConsumerEvent(payment.paymentId)
        assertEquals(PaymentConsumerEventStatus.PENDING, fixture.eventPersister.receive(delivery))
        val stripe = Mockito.mock(StripeClient::class.java)
        val services = Mockito.mock(V1Services::class.java)
        val intents = Mockito.mock(PaymentIntentService::class.java)
        Mockito.`when`(stripe.v1()).thenReturn(services)
        Mockito.`when`(services.paymentIntents()).thenReturn(intents)
        Mockito.`when`(
                intents.create(
                    Mockito.any(PaymentIntentCreateParams::class.java),
                    Mockito.any(RequestOptions::class.java),
                ),
            )
            .thenReturn(
                PaymentIntent().apply {
                  id = intentId
                  clientSecret = "${intentId}_secret_created"
                  amount = 100
                  currency = "aud"
                  metadata = mapOf("kra_payment_id" to payment.paymentId.toString())
                },
            )

        workflowOperations(stripe).applyWebhook(payment.paymentId, delivery.consumerEventId)
        fixture.eventPersister.accepted(delivery.consumerEventId)
        workflowOperations(stripe).initialise(payment.paymentRequestId)

        assertEquals(2, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
      }

  private suspend fun markCreationAttempting(paymentId: UUID) {
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "in-flight-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(PAYMENT.CREATION_ATTEMPTED_AT, OffsetDateTime.now(ZoneOffset.UTC))
                  .where(PAYMENT.ID.eq(paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }
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
}
