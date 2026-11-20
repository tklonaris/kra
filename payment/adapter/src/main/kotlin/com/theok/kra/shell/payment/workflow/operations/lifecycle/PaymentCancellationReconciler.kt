package com.theok.kra.shell.payment.workflow.operations.lifecycle

import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.persistence.coordination.PaymentCancellationDecision
import com.theok.kra.shell.payment.persistence.coordination.PaymentCancellationOriginClaim
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.persistence.coordination.toPaymentLifecycleState
import com.theok.kra.shell.payment.workflow.operations.RetryablePaymentTransitionConflict
import com.theok.kra.shell.payment.workflow.operations.TerminalPaymentOutcomeInconsistency
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentCancellation
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import com.theok.kra.shell.payment.workflow.temporal.toWorkflowControlState
import java.time.Duration
import java.util.UUID

/** Coordinates durable cancellation decisions; Stripe I/O lives in [StripePaymentCancellation]. */
internal class PaymentCancellationReconciler(
    private val workflowPersister: PaymentWorkflowPersister,
    private val stripeCancellation: StripePaymentCancellation,
    private val paymentIntentStatusRefresher: PaymentIntentStatusRefresher,
) {
  suspend fun reconcile(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
  ): PaymentWorkflowControlState {
    val decision = workflowPersister.transaction { cancellationDecision(paymentId) }
    return when (decision) {
      is PaymentCancellationDecision.NoAction -> decision.payment.toWorkflowControlState()
      is PaymentCancellationDecision.FinalizeWithoutProviderIntent ->
          finalizeWithoutProviderIntent(paymentId, decision)
      is PaymentCancellationDecision.ReconcileUnrecordedProviderIntent ->
          requireProviderIntentReconciliation(paymentId, decision)
      is PaymentCancellationDecision.ReconcileTerminalProviderState ->
          reconcileTerminalProviderState(paymentId, webhookConsumerEventId, decision)
      is PaymentCancellationDecision.ReconcileProviderIntent ->
          reconcileProviderIntent(paymentId, webhookConsumerEventId, decision)
    }
  }

  /**
   * A locally terminal cancellation can retain an undetermined cause after a failed cancellation
   * call. Re-read Stripe once: a corroborating fact resolves it; an inconclusive verified fact is
   * handed to an operator instead of leaving the Temporal reconciliation timer in a no-op loop.
   */
  private suspend fun reconcileTerminalProviderState(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      decision: PaymentCancellationDecision.ReconcileTerminalProviderState,
  ): PaymentWorkflowControlState {
    if (decision.payment.intentId == null) {
      return completeWithOperationalFailure(
          paymentId,
          "Cancelled payment has no intent to verify its terminal cause",
      )
    }
    val refreshed = paymentIntentStatusRefresher.refresh(paymentId, webhookConsumerEventId)
    if (!refreshed.reconciliationRequired) return refreshed
    return completeWithOperationalFailure(
        paymentId,
        "Verified Stripe cancellation did not determine its terminal cause",
    )
  }

  private suspend fun completeWithOperationalFailure(
      paymentId: UUID,
      reason: String,
  ): PaymentWorkflowControlState {
    val escalated = workflowPersister.transaction {
      markOperationalFailureIfUndeterminedCancellation(paymentId, reason)
    }
    return if (escalated) {
      PaymentWorkflowControlState(
          complete = true,
          awaitingProcessingOutcome = false,
          reconciliationRequired = false,
          checkoutWait = Duration.ZERO,
          processingWait = Duration.ZERO,
      )
    } else requireNotNull(load(paymentId)).toWorkflowControlState()
  }

  private suspend fun reconcileProviderIntent(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      decision: PaymentCancellationDecision.ReconcileProviderIntent,
  ): PaymentWorkflowControlState {
    val payment = decision.payment
    val intentId = requireNotNull(payment.intentId)
    when (claimCancellationOrigin(paymentId, intentId, decision.reason)) {
      PaymentCancellationOriginClaim.NoLongerEligible ->
          return requireNotNull(load(paymentId)).toWorkflowControlState()
      PaymentCancellationOriginClaim.ConflictingOrigin ->
          error("Payment $paymentId has a conflicting cancellation origin")
      PaymentCancellationOriginClaim.Claimed,
      PaymentCancellationOriginClaim.AlreadyClaimedAndActive -> Unit
    }
    stripeCancellation.reconcileAfterClaim(
        paymentId,
        webhookConsumerEventId,
        intentId,
        decision.reason,
    )
    val reconciled = requireNotNull(load(paymentId))
    return reconciled.toWorkflowControlState()
  }

  private suspend fun finalizeWithoutProviderIntent(
      paymentId: UUID,
      decision: PaymentCancellationDecision.FinalizeWithoutProviderIntent,
  ): PaymentWorkflowControlState {
    val statusChange = decision.payment.toPaymentLifecycleState().cancelWithoutProviderIntent()
    return workflowPersister.transaction {
      when (
          val transition = finalizeWithoutProviderIntent(paymentId, statusChange, decision.reason)
      ) {
        PaymentStatusTransition.Updated -> {
          requireNotNull(load(paymentId)).toWorkflowControlState()
        }
        PaymentStatusTransition.MatchingTerminalOutcome -> {
          markCancellationResolved(paymentId)
          requireNotNull(load(paymentId)).toWorkflowControlState()
        }
        is PaymentStatusTransition.ConflictingTerminalOutcome ->
            throw TerminalPaymentOutcomeInconsistency(
                transition.persistedStatus,
                transition.requestedStatus,
            )
        is PaymentStatusTransition.RefreshRequired ->
            throw RetryablePaymentTransitionConflict(paymentId)
      }
    }
  }

  private suspend fun requireProviderIntentReconciliation(
      paymentId: UUID,
      decision: PaymentCancellationDecision.ReconcileUnrecordedProviderIntent,
  ): PaymentWorkflowControlState {
    workflowPersister.transaction {
      markReconciliationRequired(
          paymentId,
          "${decision.reason.name} requires reconciliation of unrecorded provider intent",
      )
    }
    return requireNotNull(load(paymentId)).toWorkflowControlState()
  }

  private suspend fun claimCancellationOrigin(
      paymentId: UUID,
      expectedIntentId: String,
      requested: TerminalReason,
  ): PaymentCancellationOriginClaim = workflowPersister.transaction {
    claimCancellationOrigin(paymentId, expectedIntentId, requested)
  }

  private suspend fun load(paymentId: UUID): PaymentWorkflowState? = workflowPersister.transaction {
    load(paymentId)
  }
}
