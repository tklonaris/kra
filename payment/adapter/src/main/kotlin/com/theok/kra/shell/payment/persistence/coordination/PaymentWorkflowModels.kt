package com.theok.kra.shell.payment.persistence.coordination

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID

/** Joined request/payment projection used only by workflow persistence operations. */
data class PaymentWorkflowState(
    val paymentId: UUID,
    val paymentRequestId: UUID,
    val amountMinor: Long,
    val currency: String,
    val status: PaymentStatus,
    val terminalReason: TerminalReason?,
    val intentId: String?,
    val checkoutDeadlineAt: Instant,
    val processingDeadlineAt: Instant,
    val cancellationRequested: Boolean,
    val cancellationOrigin: TerminalReason?,
    val intentCreationState: IntentCreationState,
    val creationOperationId: UUID?,
    val stripeIdempotencyKey: String?,
    val creationRequestFingerprint: String?,
    val creationAttemptedAt: OffsetDateTime?,
    val reconciliationRequired: Boolean,
    val databaseNow: Instant,
) {
  fun checkoutExpired(): Boolean = !databaseNow.isBefore(checkoutDeadlineAt)

  /** Checkout expiry applies only before processing begins. */
  fun checkoutExpiryApplies(): Boolean = status.checkoutExpiryApplies && checkoutExpired()

  /** A verified unchanged customer-waiting fact resolves ordinary provider-state uncertainty. */
  fun resolvesProviderStateReconciliationAfterVerifiedStatus(): Boolean =
      status == PaymentStatus.AWAITING_CUSTOMER && reconciliationRequired && !cancellationRequested

  /** A cancelled payment requires its provider cancellation cause to be corroborated. */
  fun requiresTerminalCancellationCauseReconciliation(): Boolean =
      status == PaymentStatus.CANCELLED &&
          terminalReason == TerminalReason.CAUSE_UNDETERMINED &&
          reconciliationRequired

  /** A verified webhook may recover only a durably uncertain, unrecorded provider intent. */
  fun mayRecoverUncertainProviderIntent(): Boolean =
      status == PaymentStatus.REQUESTED &&
          intentId == null &&
          intentCreationState == IntentCreationState.UNCERTAIN &&
          reconciliationRequired
}

/** A durable cancellation decision; broker events merely prompt this decision to be evaluated. */
sealed interface PaymentCancellationDecision {
  data class NoAction(val payment: PaymentWorkflowState) : PaymentCancellationDecision

  data class FinalizeWithoutProviderIntent(
      val payment: PaymentWorkflowState,
      val reason: TerminalReason,
  ) : PaymentCancellationDecision

  /** A provider create may have succeeded but its intent ID is not durably known yet. */
  data class ReconcileUnrecordedProviderIntent(
      val payment: PaymentWorkflowState,
      val reason: TerminalReason,
  ) : PaymentCancellationDecision

  /** A terminal cancellation must still verify its previously undetermined provider cause. */
  data class ReconcileTerminalProviderState(
      val payment: PaymentWorkflowState,
  ) : PaymentCancellationDecision

  data class ReconcileProviderIntent(
      val payment: PaymentWorkflowState,
      val reason: TerminalReason,
  ) : PaymentCancellationDecision
}

/** Durable authorization result for a cancellation attempt against one known provider intent. */
sealed interface PaymentCancellationOriginClaim {
  data object Claimed : PaymentCancellationOriginClaim

  data object AlreadyClaimedAndActive : PaymentCancellationOriginClaim

  data object NoLongerEligible : PaymentCancellationOriginClaim

  data object ConflictingOrigin : PaymentCancellationOriginClaim
}

sealed interface PaymentStatusTransition {
  data object Updated : PaymentStatusTransition

  data object MatchingTerminalOutcome : PaymentStatusTransition

  data class ConflictingTerminalOutcome(
      val persistedStatus: PaymentStatus,
      val requestedStatus: PaymentStatus,
  ) : PaymentStatusTransition

  data class RefreshRequired(val persistedStatus: PaymentStatus) : PaymentStatusTransition
}
