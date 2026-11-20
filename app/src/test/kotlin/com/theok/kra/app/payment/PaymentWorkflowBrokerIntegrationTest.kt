package com.theok.kra.app.payment

import com.stripe.param.PaymentIntentCancelParams
import com.theok.kra.app.testsupport.payment.PaymentWorkflowRuntimeIntegrationTestSupport
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Exercises the real PostgreSQL, RabbitMQ, and Temporal worker path. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentWorkflowBrokerIntegrationTest :
    PaymentWorkflowRuntimeIntegrationTestSupport() {
  @Test
  fun `broker creates one durable Stripe intent and encrypts its client secret`() = runBlocking {
    val payment = acceptAndStartPaymentRequest()
    await("payment intent and encrypted client secret to be persisted") {
      paymentIntentId(payment.paymentRequestId) != null &&
          hasAvailableSetup(payment.paymentRequestId)
    }
    assertEquals(1, stripe.createCount())
    assertNotNull(paymentIntentId(payment.paymentRequestId))
    assertTrue(hasAvailableSetup(payment.paymentRequestId))
    cancelAndAwaitTerminal(payment)
  }

  @Test
  fun `broker cancellation cancels Stripe and publishes one terminal event`() = runBlocking {
    val payment = acceptAndStartPaymentRequest()
    val intentId = requireNotNull(paymentIntentId(payment.paymentRequestId))
    val cancellationEventId = UUID.randomUUID()
    requestCancellation(payment.paymentRequestId)
    fixture.publishPaymentConsumerEvent(
        cancellationEventId,
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentCancellationRequested",
    )
    awaitPaymentConsumerEventStatus(cancellationEventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.CANCELLED)
    awaitWorkflowClosed(payment.paymentRequestId)
    assertEquals(1, stripe.cancellationCount())
    val cancellation = requireNotNull(stripe.cancellationRequest(intentId))
    assertEquals(
        "kra-payment-cancel-${payment.paymentId}-customer_cancelled",
        cancellation.idempotencyKey,
    )
    assertEquals(
        PaymentIntentCancelParams.CancellationReason.REQUESTED_BY_CUSTOMER,
        cancellation.reason,
    )
    assertEquals(1, terminalOutboxCount(payment.paymentId))
    assertEquals(1, outboxCount(payment.paymentId, "PaymentCancelled"))
  }

  @Test
  fun `broker webhook completion persists the provider fact and terminal outcome`() = runBlocking {
    val payment = acceptAndStartPaymentRequest()
    val intentId = requireNotNull(paymentIntentId(payment.paymentRequestId))
    val stripeEventId = stripeEventId()
    val webhookEventId = UUID.randomUUID()
    stripe.markSucceeded(intentId)
    recordSucceededStripeEvent(stripeEventId, payment.paymentId, intentId)
    fixture.publishPaymentConsumerEvent(
        webhookEventId,
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentWebhookReceived",
        stripeEventId,
    )
    awaitPaymentConsumerEventStatus(webhookEventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.SUCCEEDED)
    awaitStripeEventApplied(stripeEventId, "succeeded")
    awaitWorkflowClosed(payment.paymentRequestId)
    assertEquals(1, terminalOutboxCount(payment.paymentId))
    assertEquals(1, outboxCount(payment.paymentId, "PaymentSucceeded"))
  }

  @Test
  fun `late cancellation safely re-enters a completed saga without another provider cancellation`() =
      runBlocking {
        val payment = acceptAndStartPaymentRequest()
        requestCancellation(payment.paymentRequestId)
        val cancellationEventId = UUID.randomUUID()
        fixture.publishPaymentConsumerEvent(
            cancellationEventId,
            payment.paymentRequestId,
            payment.paymentId,
            "PaymentCancellationRequested",
        )
        awaitPaymentConsumerEventStatus(cancellationEventId, PaymentConsumerEventStatus.ACCEPTED)
        awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.CANCELLED)
        awaitWorkflowClosed(payment.paymentRequestId)

        val delayedEventId = UUID.randomUUID()
        fixture.publishPaymentConsumerEvent(
            delayedEventId,
            payment.paymentRequestId,
            payment.paymentId,
            "PaymentCancellationRequested",
        )

        awaitPaymentConsumerEventStatus(delayedEventId, PaymentConsumerEventStatus.ACCEPTED)
        assertEquals(1, stripe.cancellationCount())
        assertEquals(
            WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
            workflowStatus(payment.paymentRequestId),
        )
      }

  @Test
  fun `terminal notifications are skipped without starting a saga`() = runBlocking {
    val request = acceptPaymentRequest()
    val terminalNotificationId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        terminalNotificationId,
        request.paymentRequestId,
        null,
        "PaymentSucceeded",
    )

    awaitPaymentConsumerEventStatus(terminalNotificationId, PaymentConsumerEventStatus.SKIPPED)
    assertNull(workflowStatus(request.paymentRequestId))
    assertNull(paymentStatus(request.paymentRequestId))
    assertEquals(0, stripe.createCount())
  }

  @Test
  fun `completed workflow re-enters for a confirming provider event`() = runBlocking {
    val payment = acceptAndStartPaymentRequest()
    val intentId = requireNotNull(paymentIntentId(payment.paymentRequestId))
    val firstStripeEventId = stripeEventId()
    stripe.markSucceeded(intentId)
    recordSucceededStripeEvent(firstStripeEventId, payment.paymentId, intentId)
    val completionEventId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        completionEventId,
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentWebhookReceived",
        firstStripeEventId,
    )
    awaitPaymentConsumerEventStatus(completionEventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.SUCCEEDED)
    awaitWorkflowClosed(payment.paymentRequestId)

    val confirmingStripeEventId = stripeEventId()
    val recoveryEventId = UUID.randomUUID()
    recordSucceededStripeEvent(confirmingStripeEventId, payment.paymentId, intentId)
    fixture.publishPaymentConsumerEvent(
        recoveryEventId,
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentWebhookReceived",
        confirmingStripeEventId,
    )
    awaitPaymentConsumerEventStatus(recoveryEventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitStripeEventApplied(confirmingStripeEventId, "succeeded")
    assertEquals(PaymentStatus.SUCCEEDED, paymentStatus(payment.paymentRequestId))
    assertEquals(1, terminalOutboxCount(payment.paymentId))
    assertEquals(1, stripe.createCount())
    assertEquals(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
        workflowStatus(payment.paymentRequestId),
    )
  }

  @Test
  fun `broker duplicate delivery and duplicate workflow start are accepted`() = runBlocking {
    val payment = acceptAndStartPaymentRequest()
    val duplicateDeliveryId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        duplicateDeliveryId,
        payment.paymentRequestId,
        null,
        "PaymentRequestAccepted",
    )
    awaitPaymentConsumerEventStatus(duplicateDeliveryId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.AWAITING_CUSTOMER)
    fixture.publishPaymentConsumerEvent(
        duplicateDeliveryId,
        payment.paymentRequestId,
        null,
        "PaymentRequestAccepted",
    )
    awaitPaymentConsumerEventStatus(duplicateDeliveryId, PaymentConsumerEventStatus.ACCEPTED)
    val duplicateWorkflowStartId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        duplicateWorkflowStartId,
        payment.paymentRequestId,
        null,
        "PaymentRequestAccepted",
    )
    awaitPaymentConsumerEventStatus(duplicateWorkflowStartId, PaymentConsumerEventStatus.ACCEPTED)
    assertEquals(1, stripe.createCount())
    assertEquals(
        PaymentConsumerEventStatus.ACCEPTED,
        paymentConsumerEventStatus(duplicateDeliveryId),
    )
    assertEquals(
        PaymentConsumerEventStatus.ACCEPTED,
        paymentConsumerEventStatus(duplicateWorkflowStartId),
    )
    cancelAndAwaitTerminal(payment)
  }
}
