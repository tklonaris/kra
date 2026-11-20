package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEvent
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Verifies idempotent, durable RabbitMQ consumer receipts. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentConsumerEventIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `duplicate broker delivery keeps the recorded accepted outcome`() = runBlocking {
    val event = requestAcceptedEvent(acceptPaymentRequest().paymentRequestId)

    assertEquals(PaymentConsumerEventStatus.PENDING, fixture.eventPersister.receive(event))
    fixture.eventPersister.accepted(event.consumerEventId)
    assertEquals(PaymentConsumerEventStatus.ACCEPTED, fixture.eventPersister.receive(event))
  }

  @Test
  fun `stale completion cannot overwrite a requested dead letter`() = runBlocking {
    val event = requestAcceptedEvent(acceptPaymentRequest().paymentRequestId)

    assertEquals(PaymentConsumerEventStatus.PENDING, fixture.eventPersister.receive(event))
    fixture.eventPersister.requestDlq(event.consumerEventId, "workflow dispatch failed")
    fixture.eventPersister.accepted(event.consumerEventId)

    assertEquals(PaymentConsumerEventStatus.DLQ_REQUESTED, fixture.eventPersister.receive(event))
  }

  @Test
  fun `duplicate consumer event ID rejects incompatible payload`() = runBlocking {
    val event = requestAcceptedEvent(acceptPaymentRequest().paymentRequestId)
    assertEquals(PaymentConsumerEventStatus.PENDING, fixture.eventPersister.receive(event))

    assertFailsWith<IllegalStateException> {
      fixture.eventPersister.receive(event.copy(eventType = PaymentEventType.RETRY_REQUESTED))
    }
    Unit
  }

  private fun requestAcceptedEvent(paymentRequestId: UUID) =
      PaymentConsumerEvent(
          consumerEventId = UUID.randomUUID(),
          paymentRequestId = paymentRequestId,
          paymentId = null,
          eventType = PaymentEventType.REQUEST_ACCEPTED,
      )
}
