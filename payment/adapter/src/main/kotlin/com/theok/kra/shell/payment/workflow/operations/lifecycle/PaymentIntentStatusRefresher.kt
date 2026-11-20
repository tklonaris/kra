package com.theok.kra.shell.payment.workflow.operations.lifecycle

import com.stripe.model.PaymentIntent
import com.theok.kra.core.payment.domain.PaymentStatusUpdate
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.workflow.operations.RetryablePaymentTransitionConflict
import com.theok.kra.shell.payment.workflow.operations.TerminalPaymentOutcomeInconsistency
import com.theok.kra.shell.payment.workflow.operations.matchesKnownPayment
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import com.theok.kra.shell.payment.workflow.temporal.toWorkflowControlState
import java.util.UUID

/**
 * Retrieves Stripe intent facts and retries one guarded durable status application after a race.
 */
internal class PaymentIntentStatusRefresher(
    private val workflowPersister: PaymentWorkflowPersister,
    private val stripePaymentGateway: StripePaymentGateway,
    private val paymentIntentStatusUpdateApplier: PaymentIntentStatusUpdateApplier,
) {
  suspend fun refresh(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      webhookStripeEventId: String? = null,
  ): PaymentWorkflowControlState {
    val payment = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
    val intentId = payment.intentId ?: return payment.toWorkflowControlState()
    applyWithRefresh(
        paymentId,
        webhookConsumerEventId,
        intentId,
        stripePaymentGateway.retrieve(intentId),
        webhookStripeEventId = webhookStripeEventId,
    )
    return requireNotNull(load(paymentId)).toWorkflowControlState()
  }

  suspend fun expireProcessingDeadline(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
  ): PaymentWorkflowControlState {
    val snapshot = refresh(paymentId, webhookConsumerEventId)
    if (!snapshot.complete) {
      try {
        workflowPersister.transaction {
          markReconciliationRequired(paymentId, "processing deadline elapsed")
        }
      } catch (failure: Exception) {
        throw PaymentProviderStatePersistenceFailure(paymentId, failure)
      }
    }
    return requireNotNull(load(paymentId)).toWorkflowControlState()
  }

  suspend fun applyWithRefresh(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      intentId: String,
      initialIntent: PaymentIntent,
      requestedCancellationReason: TerminalReason? = null,
      webhookStripeEventId: String? = null,
  ): PaymentIntent {
    verifyPaymentIntent(paymentId, intentId, initialIntent)
    val initialResult =
        applyProviderFact(
            paymentId,
            webhookConsumerEventId,
            webhookStripeEventId,
            initialIntent,
            requestedCancellationReason,
        )
    return when (initialResult) {
      StatusUpdateApplyResult.Applied -> initialIntent
      is StatusUpdateApplyResult.ConflictingTerminalOutcome -> {
        throw TerminalPaymentOutcomeInconsistency(
            initialResult.persistedStatus,
            initialResult.requestedStatus,
        )
      }
      StatusUpdateApplyResult.RefreshRequired -> {
        val refreshedIntent = stripePaymentGateway.retrieve(intentId)
        verifyPaymentIntent(paymentId, intentId, refreshedIntent)
        when (
            val refreshedResult =
                applyProviderFact(
                    paymentId,
                    webhookConsumerEventId,
                    webhookStripeEventId,
                    refreshedIntent,
                    requestedCancellationReason,
                )
        ) {
          StatusUpdateApplyResult.Applied -> refreshedIntent
          is StatusUpdateApplyResult.ConflictingTerminalOutcome -> {
            throw TerminalPaymentOutcomeInconsistency(
                refreshedResult.persistedStatus,
                refreshedResult.requestedStatus,
            )
          }
          StatusUpdateApplyResult.RefreshRequired -> {
            throw RetryablePaymentTransitionConflict(paymentId)
          }
        }
      }
    }
  }

  private suspend fun apply(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      webhookStripeEventId: String?,
      intent: PaymentIntent,
      requestedCancellationReason: TerminalReason?,
  ): StatusUpdateApplyResult {
    val statusUpdate = intent.toStatusUpdate()
    if (statusUpdate == null) {
      workflowPersister.transaction {
        markReconciliationRequired(
            paymentId,
            "Stripe status ${intent.status} requires explicit reconciliation",
        )
      }
      return StatusUpdateApplyResult.Applied
    }
    return paymentIntentStatusUpdateApplier.apply(
        paymentId,
        webhookConsumerEventId,
        webhookStripeEventId,
        statusUpdate,
        intent.status,
        intent.cancellationReason,
        requestedCancellationReason,
    )
  }

  private suspend fun applyProviderFact(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      webhookStripeEventId: String?,
      intent: PaymentIntent,
      requestedCancellationReason: TerminalReason?,
  ): StatusUpdateApplyResult =
      try {
        apply(
            paymentId,
            webhookConsumerEventId,
            webhookStripeEventId,
            intent,
            requestedCancellationReason,
        )
      } catch (failure: Exception) {
        throw PaymentProviderStatePersistenceFailure(paymentId, failure)
      }

  private suspend fun load(paymentId: UUID): PaymentWorkflowState? = workflowPersister.transaction {
    load(paymentId)
  }

  private suspend fun verifyPaymentIntent(
      paymentId: UUID,
      intentId: String,
      intent: PaymentIntent,
  ) {
    val payment = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
    check(intent.matchesKnownPayment(intentId, payment)) {
      "Stripe payment intent does not match payment $paymentId"
    }
  }

  private fun PaymentIntent.toStatusUpdate() =
      when (status) {
        "succeeded" -> PaymentStatusUpdate.SUCCEEDED
        "canceled" -> PaymentStatusUpdate.CANCELLED
        "processing" -> PaymentStatusUpdate.PROCESSING
        "requires_action",
        "requires_confirmation",
        "requires_payment_method" -> PaymentStatusUpdate.AWAITING_CUSTOMER
        else -> null
      }
}
