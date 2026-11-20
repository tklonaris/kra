package com.theok.kra.shell.payment.persistence.lifecycle

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.coordination.PaymentCancellationDecision
import com.theok.kra.shell.payment.persistence.coordination.PaymentCancellationOriginClaim
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowLoader
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.persistence.request.PaymentRequestLifecycleEventType
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Transaction-scoped local cancellation decisions, claims, and resolution markers. */
internal class PaymentCancellationPersister(
    private val dsl: DSLContext,
    private val loader: PaymentWorkflowLoader,
) {
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  suspend fun decisionFor(paymentId: UUID): PaymentCancellationDecision =
      decisionFor(requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" })

  fun decisionFor(payment: PaymentWorkflowState): PaymentCancellationDecision {
    // Once an origin has been claimed, it is the durable authorization for every later provider
    // reconciliation. A customer request can legitimately race with expiry after expiry has made
    // that claim; changing the reason would create a second Stripe idempotency namespace.
    val reason =
        payment.cancellationOrigin
            ?: when {
              payment.cancellationRequested -> TerminalReason.CUSTOMER_CANCELLED
              payment.checkoutExpiryApplies() -> TerminalReason.CHECKOUT_EXPIRED
              else -> null
            }
    return when {
      payment.requiresTerminalCancellationCauseReconciliation() ->
          PaymentCancellationDecision.ReconcileTerminalProviderState(payment)
      payment.status.terminal || reason == null -> PaymentCancellationDecision.NoAction(payment)
      payment.intentId == null && payment.intentCreationState == IntentCreationState.NOT_STARTED ->
          PaymentCancellationDecision.FinalizeWithoutProviderIntent(payment, reason)
      // A creation claim may already have authorised Stripe I/O. Do not finalise locally until
      // that durable outcome is known, otherwise an unrecorded provider intent could be orphaned.
      payment.intentId == null ->
          PaymentCancellationDecision.ReconcileUnrecordedProviderIntent(payment, reason)
      else -> PaymentCancellationDecision.ReconcileProviderIntent(payment, reason)
    }
  }

  suspend fun claimOrigin(
      paymentId: UUID,
      expectedIntentId: String?,
      origin: TerminalReason,
  ): PaymentCancellationOriginClaim {
    require(origin.isLocalCancellation) {
      "Only local cancellation reasons can be claimed"
    }
    val requestOrigin = requestStatePersister.lockRequestByPaymentId(paymentId).cancellationOrigin
    if (requestOrigin != origin) {
      return if (requestOrigin == null) PaymentCancellationOriginClaim.NoLongerEligible
      else PaymentCancellationOriginClaim.ConflictingOrigin
    }
    val claimed =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.CANCELLATION_ATTEMPTED_AT, DSL.currentOffsetDateTime())
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(
                        PAYMENT.STATUS.notIn(
                            *PaymentStatus.terminalStatuses.map(PaymentStatus::name).toTypedArray(),
                        ),
                    )
                    .and(
                        if (expectedIntentId == null) PAYMENT.PROVIDER_PAYMENT_ID.isNull
                        else PAYMENT.PROVIDER_PAYMENT_ID.eq(expectedIntentId),
                    )
                    .and(PAYMENT.CANCELLATION_ATTEMPTED_AT.isNull),
            )
            .single()
            .awaitSingle() == 1
    if (claimed) {
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.CANCELLATION_ORIGIN_CLAIMED,
      )
      return PaymentCancellationOriginClaim.Claimed
    }
    val persisted = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
    return when {
      persisted.status.terminal || persisted.intentId != expectedIntentId ->
          PaymentCancellationOriginClaim.NoLongerEligible
      persisted.cancellationOrigin == origin ->
          PaymentCancellationOriginClaim.AlreadyClaimedAndActive
      persisted.cancellationOrigin == null ->
          error("Cancellation origin claim was not applied to an eligible payment")
      else -> PaymentCancellationOriginClaim.ConflictingOrigin
    }
  }

  suspend fun resolveUndetermined(paymentId: UUID, reason: TerminalReason): Boolean {
    require(reason.resolvesUndeterminedCancellation)
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.TERMINAL_REASON, reason.name)
                    .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                    .set(PAYMENT.RECONCILIATION_REASON, null as String?)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(PaymentStatus.CANCELLED.name))
                    .and(PAYMENT.TERMINAL_REASON.eq(TerminalReason.CAUSE_UNDETERMINED.name))
                    .and(PAYMENT.RECONCILIATION_REQUIRED),
            )
            .single()
            .awaitSingle()
    if (updated != 1) return false
    val persisted = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
    val nextRequestStatus =
        request.status.nextForPaymentStatus(persisted.status, persisted.reconciliationRequired)
    requestStatePersister.transition(
        request,
        nextRequestStatus,
        PaymentRequestLifecycleEventType.CANCELLATION_CAUSE_RESOLVED,
    )
    dsl.insertPaymentLifecycleEvent(
        paymentId = paymentId,
        eventType = PaymentLifecycleEventType.CANCELLATION_CAUSE_RESOLVED,
        toStatus = PaymentStatus.CANCELLED,
        terminalReason = reason,
        reconciliationRequired = false,
    )
    return true
  }

  /**
   * Escalates only the still-unresolved cancellation protected by the request-first lock. A
   * concurrent webhook resolution acquires that same request lock, so reloading after the lock
   * prevents an older reconciliation attempt from overwriting `COMPLETED` with `FAILED`.
   */
  suspend fun markOperationalFailureIfUndeterminedCancellation(
      paymentId: UUID,
      reason: String,
  ): Boolean {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val payment = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
    if (!payment.requiresTerminalCancellationCauseReconciliation()) return false
    requestStatePersister.markOperationalFailure(request, reason)
    return true
  }

  suspend fun markResolved(paymentId: UUID) {
    Flux.from(
            dsl.update(PAYMENT)
                .set(PAYMENT.CANCELLATION_RESOLVED_AT, DSL.currentOffsetDateTime())
                .where(PAYMENT.ID.eq(paymentId))
                .and(
                    PAYMENT.STATUS.`in`(
                        *PaymentStatus.terminalStatuses.map(PaymentStatus::name).toTypedArray(),
                    ),
                )
                .and(PAYMENT.CANCELLATION_RESOLVED_AT.isNull),
        )
        .single()
        .awaitSingle()
  }
}
