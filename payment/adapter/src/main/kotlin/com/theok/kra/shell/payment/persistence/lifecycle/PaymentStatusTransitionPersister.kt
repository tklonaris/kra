package com.theok.kra.shell.payment.persistence.lifecycle

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.PaymentStatusChange
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowLoader
import com.theok.kra.shell.payment.persistence.request.LockedPaymentRequest
import com.theok.kra.shell.payment.persistence.request.PaymentRequestLifecycleEventType
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Transaction-scoped guarded status transitions and their idempotent outcome classification. */
internal class PaymentStatusTransitionPersister(
    private val dsl: DSLContext,
    private val loader: PaymentWorkflowLoader,
) {
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  suspend fun update(
      paymentId: UUID,
      statusChange: PaymentStatusChange,
      terminalReason: TerminalReason?,
  ): PaymentStatusTransition {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val currentStatus = statusChange.expectedStatus
    val nextStatus = statusChange.nextStatus
    val update =
        when (nextStatus) {
          PaymentStatus.CANCELLED -> {
            val reason =
                requireNotNull(terminalReason) { "Cancelled payments require a terminal reason" }
            dsl.update(PAYMENT)
                .set(PAYMENT.STATUS, nextStatus.name)
                .set(PAYMENT.TERMINAL_REASON, reason.name)
                .set(PAYMENT.RECONCILIATION_REQUIRED, reason == TerminalReason.CAUSE_UNDETERMINED)
                .set(
                    PAYMENT.RECONCILIATION_REASON,
                    if (reason == TerminalReason.CAUSE_UNDETERMINED)
                        "cancellation cause requires reconciliation"
                    else null as String?,
                )
                .where(PAYMENT.ID.eq(paymentId))
                .and(PAYMENT.STATUS.eq(currentStatus.name))
          }
          PaymentStatus.SUCCEEDED -> {
            dsl.update(PAYMENT)
                .set(PAYMENT.STATUS, nextStatus.name)
                .set(PAYMENT.TERMINAL_REASON, null as String?)
                .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                .set(PAYMENT.RECONCILIATION_REASON, null as String?)
                .where(PAYMENT.ID.eq(paymentId))
                .and(PAYMENT.STATUS.eq(currentStatus.name))
          }
          else -> {
            dsl.update(PAYMENT)
                .set(PAYMENT.STATUS, nextStatus.name)
                .where(PAYMENT.ID.eq(paymentId))
                .and(PAYMENT.STATUS.eq(currentStatus.name))
          }
        }
    val updated = Flux.from(update).single().awaitSingle()
    return completeTransition(paymentId, request, statusChange, terminalReason, updated)
  }

  suspend fun finalizeWithoutProviderIntent(
      paymentId: UUID,
      statusChange: PaymentStatusChange,
      origin: TerminalReason,
  ): PaymentStatusTransition {
    require(origin.isLocalCancellation) {
      "Only local cancellation reasons can be finalised without a provider intent"
    }
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val update =
        dsl.update(PAYMENT)
            .set(PAYMENT.STATUS, statusChange.nextStatus.name)
            .set(PAYMENT.TERMINAL_REASON, origin.name)
            .set(PAYMENT.CANCELLATION_ATTEMPTED_AT, DSL.currentOffsetDateTime())
            .set(PAYMENT.CANCELLATION_RESOLVED_AT, DSL.currentOffsetDateTime())
            .where(PAYMENT.ID.eq(paymentId))
            .and(PAYMENT.STATUS.eq(statusChange.expectedStatus.name))
            .and(PAYMENT.PROVIDER_PAYMENT_ID.isNull)
            .and(PAYMENT.INTENT_CREATION_STATE.eq(IntentCreationState.NOT_STARTED.name))
    val updated = Flux.from(update).single().awaitSingle()
    return completeTransition(paymentId, request, statusChange, origin, updated)
  }

  private suspend fun completeTransition(
      paymentId: UUID,
      request: LockedPaymentRequest,
      statusChange: PaymentStatusChange,
      terminalReason: TerminalReason?,
      updated: Int,
  ): PaymentStatusTransition {
    val currentStatus = statusChange.expectedStatus
    val nextStatus = statusChange.nextStatus
    val persisted = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
    if (updated == 1) {
      val nextRequestStatus =
          request.status.nextForPaymentStatus(nextStatus, persisted.reconciliationRequired)
      requestStatePersister.transition(
          request,
          nextRequestStatus,
          PaymentRequestLifecycleEventType.PAYMENT_STATUS_CHANGED,
      )
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.STATUS_CHANGED,
          fromStatus = currentStatus,
          toStatus = nextStatus,
          terminalReason = terminalReason,
          reconciliationRequired = persisted.reconciliationRequired,
      )
      return PaymentStatusTransition.Updated
    }
    return when {
      persisted.status == nextStatus && nextStatus.terminal ->
          PaymentStatusTransition.MatchingTerminalOutcome
      persisted.status.terminal ->
          PaymentStatusTransition.ConflictingTerminalOutcome(persisted.status, nextStatus)
      else -> PaymentStatusTransition.RefreshRequired(persisted.status)
    }
  }
}
