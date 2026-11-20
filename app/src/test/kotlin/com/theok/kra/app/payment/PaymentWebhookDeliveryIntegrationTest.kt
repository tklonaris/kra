package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentWebhookTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.commands.InvalidStripeWebhook
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.WebhookAcceptance
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.jooq.impl.DSL
import org.junit.jupiter.api.TestInstance
import reactor.core.publisher.Flux

/** Verifies signed Stripe-event persistence, association, and durable saga hand-off. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentWebhookDeliveryIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `verified webhook persists its event and transactional handoff`() = runBlocking {
    val payment = registerPaymentForTest()
    val intentId = markKnownIntent(payment.paymentId)
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body =
        PaymentWebhookTestSupport.paymentIntentBody(stripeEventId, payment.paymentId, intentId)

    assertEquals(
        WebhookAcceptance.Accepted,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    assertEquals(1, fixture.database.providerEventCount(stripeEventId))
    assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
    val consumerEvent = fixture.database.webhookConsumerEvent(payment.paymentId)
    assertEquals(stripeEventId, consumerEvent.stripeEventId)
    assertEquals(
        PaymentConsumerEventStatus.PENDING,
        fixture.eventPersister.receive(consumerEvent),
    )
  }

  @Test
  fun `signed redelivery re-enqueues an unapplied Stripe event after its handoff is dead-lettered`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val intentId = markKnownIntent(payment.paymentId)
        val stripeEventId = "evt_test_${UUID.randomUUID()}"
        val body =
            PaymentWebhookTestSupport.paymentIntentBody(stripeEventId, payment.paymentId, intentId)

        assertEquals(
            WebhookAcceptance.Accepted,
            fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
        )
        val firstHandoff = fixture.database.webhookConsumerEvent(payment.paymentId)
        assertEquals(
            PaymentConsumerEventStatus.PENDING,
            fixture.eventPersister.receive(firstHandoff),
        )
        fixture.eventPersister.requestDlq(firstHandoff.consumerEventId, "workflow dispatch failed")

        assertEquals(
            WebhookAcceptance.Duplicate,
            fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
        )
        assertEquals(1, fixture.database.providerEventCount(stripeEventId))
        assertEquals(2, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
      }

  @Test
  fun `audit-only webhook is associated when a signed redelivery becomes eligible`() = runBlocking {
    val payment = registerPaymentForTest()
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body =
        PaymentWebhookTestSupport.paymentIntentBody(
            stripeEventId,
            payment.paymentId,
            "pi_test_${UUID.randomUUID()}",
        )

    assertEquals(
        WebhookAcceptance.AuditOnly,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    assertEquals(1, fixture.database.providerEventCount(stripeEventId))
    assertEquals(0, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))

    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "claimed-intent-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(PAYMENT.CREATION_ATTEMPTED_AT, DSL.currentOffsetDateTime())
                  .where(PAYMENT.ID.eq(payment.paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }

    assertEquals(
        WebhookAcceptance.Accepted,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
  }

  @Test
  fun `signed webhook with an unknown payment reference is retained as audit-only`() = runBlocking {
    val stripeEventId = "evt_test_${UUID.randomUUID()}"
    val body =
        PaymentWebhookTestSupport.paymentIntentBody(
            stripeEventId,
            UUID.randomUUID(),
            "pi_test_${UUID.randomUUID()}",
        )

    assertEquals(
        WebhookAcceptance.AuditOnly,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )
    assertEquals(1, fixture.database.providerEventCount(stripeEventId))
  }

  @Test
  fun `signed redelivery rejects reused provider event IDs with altered facts`() = runBlocking {
    val payment = registerPaymentForTest()
    val eventId = "evt_test_${UUID.randomUUID()}"
    val intentId = "pi_test_${UUID.randomUUID()}"
    val body = PaymentWebhookTestSupport.paymentIntentBody(eventId, payment.paymentId, intentId)

    assertEquals(
        WebhookAcceptance.AuditOnly,
        fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body)),
    )

    val altered =
        PaymentWebhookTestSupport.paymentIntentBody(
            eventId,
            payment.paymentId,
            intentId,
            amountMinor = 101,
        )
    assertFailsWith<IllegalStateException> {
      fixture.webhookCommands.accept(altered, PaymentWebhookTestSupport.signature(altered))
    }
    Unit
  }

  @Test
  fun `signed non-PaymentIntent webhooks are ignored without a handoff`() = runBlocking {
    val payment = registerPaymentForTest()
    val chargeEventId = "evt_test_${UUID.randomUUID()}"
    val chargeBody =
        PaymentWebhookTestSupport.webhookBody(
            stripeEventId = chargeEventId,
            eventType = "charge.succeeded",
            objectType = "charge",
            objectId = "ch_test",
            paymentId = payment.paymentId,
        )
    val mismatchedEventId = "evt_test_${UUID.randomUUID()}"
    val mismatchedBody =
        PaymentWebhookTestSupport.webhookBody(
            stripeEventId = mismatchedEventId,
            eventType = "payment_intent.succeeded",
            objectType = "charge",
            objectId = "ch_test",
            paymentId = payment.paymentId,
        )

    assertEquals(
        WebhookAcceptance.Ignored,
        fixture.webhookCommands.accept(chargeBody, PaymentWebhookTestSupport.signature(chargeBody)),
    )
    assertEquals(
        WebhookAcceptance.Ignored,
        fixture.webhookCommands.accept(
            mismatchedBody,
            PaymentWebhookTestSupport.signature(mismatchedBody),
        ),
    )
    assertEquals(0, fixture.database.providerEventCount(chargeEventId))
    assertEquals(0, fixture.database.providerEventCount(mismatchedEventId))
    assertEquals(0, fixture.database.outboxCount(payment.paymentId, "PaymentWebhookReceived"))
  }

  @Test
  fun `signed malformed webhook is rejected before persistence`() = runBlocking {
    val body =
        fixture.objectMapper
            .createObjectNode()
            .put("id", "evt_test_${UUID.randomUUID()}")
            .put("type", "payment_intent.succeeded")
            .put("livemode", false)
            .toString()

    assertFailsWith<InvalidStripeWebhook> {
      fixture.webhookCommands.accept(body, PaymentWebhookTestSupport.signature(body))
    }
    Unit
  }

  private suspend fun markKnownIntent(paymentId: UUID): String {
    val intentId = "pi_test_${UUID.randomUUID()}"
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.PROVIDER_PAYMENT_ID, intentId)
                  .set(PAYMENT.INTENT_CREATION_STATE, "KNOWN")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "known-intent-key")
                  .where(PAYMENT.ID.eq(paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }
    return intentId
  }
}
