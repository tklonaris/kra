package com.theok.kra.shell.payment.workflow.operations

import com.stripe.StripeClient
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventPersister
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import com.theok.kra.shell.payment.workflow.temporal.toWorkflowControlState
import java.util.UUID
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/** Thin Temporal-activity facade over focused payment workflow operations. */
@Component
@Profile("payments")
class PaymentWorkflowActivityOperations
@Autowired
constructor(
    private val workflowPersister: PaymentWorkflowPersister,
    private val creation: PaymentIntentCreationOperations,
    private val webhookRecovery: PaymentWebhookRecoveryOperations,
    private val lifecycleReconciliationOperations: PaymentLifecycleReconciliationOperations,
    private val consumerEvents: PaymentConsumerEventPersister? = null,
) {
  constructor(
      workflowPersister: PaymentWorkflowPersister,
      stripeClient: StripeClient,
      clientSecretCipher: ClientSecretCipher,
      lifecycleReconciliationOperations: PaymentLifecycleReconciliationOperations,
  ) : this(
      workflowPersister,
      PaymentIntentCreationOperations(
          workflowPersister,
          StripePaymentGateway(stripeClient),
          clientSecretCipher,
          lifecycleReconciliationOperations,
          PaymentUncertainIntentRecoveryOperations(
              workflowPersister,
              StripePaymentGateway(stripeClient),
              clientSecretCipher,
          ),
      ),
      PaymentWebhookRecoveryOperations(
          workflowPersister,
          lifecycleReconciliationOperations,
          PaymentUncertainIntentRecoveryOperations(
              workflowPersister,
              StripePaymentGateway(stripeClient),
              clientSecretCipher,
          ),
      ),
      lifecycleReconciliationOperations,
      null,
  )

  suspend fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState {
    val paymentId = workflowPersister.transaction { registerPayment(paymentRequestId) }
    return if (paymentId != null) creation.initialise(paymentId)
    else
        PaymentWorkflowControlState(
            complete = true,
            awaitingProcessingOutcome = false,
            reconciliationRequired = false,
            checkoutWait = java.time.Duration.ZERO,
            processingWait = java.time.Duration.ZERO,
        )
  }

  suspend fun applyCancellation(paymentId: UUID): PaymentWorkflowControlState =
      lifecycleReconciliationOperations.reconcileCancellation(paymentId)

  /** Applies a verified Stripe webhook event while the workflow is still running. */
  suspend fun applyWebhook(paymentId: UUID, consumerEventId: UUID): PaymentWorkflowControlState =
      webhookRecovery.applyWebhook(paymentId, consumerEventId)

  suspend fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState {
    val paymentId = workflowPersister.transaction { loadByRequestId(paymentRequestId)?.paymentId }
    return if (paymentId == null) initialise(paymentRequestId) else creation.reconcile(paymentId)
  }

  /** Resolves a unified Temporal wake-up from the durable consumer receipt. */
  suspend fun processWakeUp(
      paymentRequestId: UUID,
      consumerEventId: UUID,
  ): PaymentWorkflowControlState {
    val event =
        requireNotNull(requireNotNull(consumerEvents).load(consumerEventId)) {
          "Unknown payment wake-up $consumerEventId"
        }
    check(event.paymentRequestId == paymentRequestId) {
      "Wake-up $consumerEventId belongs to a different payment request"
    }
    return when (event.eventType) {
      PaymentEventType.REQUEST_ACCEPTED,
      PaymentEventType.RETRY_REQUESTED -> initialise(paymentRequestId)
      PaymentEventType.CANCELLATION_REQUESTED ->
          if (event.paymentId == null) initialise(paymentRequestId)
          else applyCancellation(event.paymentId)
      PaymentEventType.WEBHOOK_RECEIVED ->
          applyWebhook(
              requireNotNull(event.paymentId) {
                "Webhook wake-up $consumerEventId has no payment"
              },
              consumerEventId,
          )
      PaymentEventType.SUCCEEDED,
      PaymentEventType.CANCELLED ->
          error("Terminal notification $consumerEventId must not wake a saga")
    }
  }

  /** Returns durable nonterminal state while Temporal awaits verified webhook recovery. */
  suspend fun awaitWebhookRecovery(paymentRequestId: UUID): PaymentWorkflowControlState =
      webhookRecovery.awaitRecovery(paymentIdForRequest(paymentRequestId))

  suspend fun expire(paymentRequestId: UUID): PaymentWorkflowControlState =
      lifecycleReconciliationOperations.expire(paymentIdForRequest(paymentRequestId))

  suspend fun expireProcessingDeadline(paymentRequestId: UUID): PaymentWorkflowControlState =
      lifecycleReconciliationOperations.expireProcessingDeadline(
          paymentIdForRequest(paymentRequestId),
      )

  /** Makes unresolved provider I/O recoverable before the bounded workflow closes. */
  suspend fun requireReconciliation(
      paymentRequestId: UUID,
      reason: String,
  ): PaymentWorkflowControlState = workflowPersister.transaction {
    val payment = loadByRequestIdAfterLockingRequest(paymentRequestId)
    if (payment == null) {
      markRequestOnlyOperationalFailure(paymentRequestId, reason)
      completedControlState()
    } else {
      markReconciliationRequired(payment.paymentId, reason)
      requireNotNull(load(payment.paymentId)) { "Payment ${payment.paymentId} does not exist" }
          .toWorkflowControlState()
    }
  }

  suspend fun markOperationalFailure(paymentRequestId: UUID, reason: String) {
    workflowPersister.transaction {
      val payment = loadByRequestIdAfterLockingRequest(paymentRequestId)
      if (payment == null) markRequestOnlyOperationalFailure(paymentRequestId, reason)
      else markOperationalFailure(payment.paymentId, reason)
    }
  }

  private fun completedControlState() =
      PaymentWorkflowControlState(
          complete = true,
          awaitingProcessingOutcome = false,
          reconciliationRequired = false,
          checkoutWait = java.time.Duration.ZERO,
          processingWait = java.time.Duration.ZERO,
      )

  private suspend fun paymentIdForRequest(paymentRequestId: UUID): UUID =
      workflowPersister.transaction {
        requireNotNull(loadByRequestId(paymentRequestId)) {
              "Payment request $paymentRequestId has no payment"
            }
            .paymentId
      }
}

class TerminalPaymentOutcomeInconsistency(
    val persistedStatus: PaymentStatus,
    val requestedStatus: PaymentStatus,
) :
    RuntimeException(
        "Conflicting terminal payment outcome: persisted=$persistedStatus requested=$requestedStatus",
    ) {
  companion object {
    const val FAILURE_TYPE = "terminal-payment-outcome-inconsistency"
    const val serialVersionUID = 1L
  }
}

class RetryablePaymentTransitionConflict(paymentId: UUID) :
    RuntimeException("Payment $paymentId changed during status update") {
  private companion object {
    const val serialVersionUID = 1L
  }
}

class PaymentCreationRequiresReconciliation(paymentId: UUID, cause: Throwable? = null) :
    RuntimeException("Payment $paymentId requires Stripe creation reconciliation", cause) {
  private companion object {
    const val serialVersionUID = 1L
  }
}
