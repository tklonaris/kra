package com.theok.kra.shell.payment.workflow.operations.lifecycle

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentLifecycleState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.PaymentStatusDecision
import com.theok.kra.core.payment.domain.PaymentStatusUpdate
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.core.payment.domain.providerStatus
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersistence
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.persistence.coordination.toPaymentLifecycleState
import com.theok.kra.shell.payment.workflow.stripe.corroboratedLocalCancellationReason
import java.util.UUID

/**
 * Atomically applies a Stripe status fact, its terminal hand-off, and provider-event
 * acknowledgement.
 */
internal class PaymentIntentStatusUpdateApplier(
    private val workflowPersister: PaymentWorkflowPersister,
) {
  suspend fun apply(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      webhookStripeEventId: String?,
      statusUpdate: PaymentStatusUpdate,
      stripeStatus: String,
      providerCancellationReason: String?,
      requestedCancellationReason: TerminalReason? = null,
  ): StatusUpdateApplyResult = workflowPersister.transaction {
    val payment = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
    check(
        payment.intentId != null && payment.intentCreationState == IntentCreationState.KNOWN,
    ) {
      "Cannot apply a Stripe status before its payment intent is durably known"
    }
    val result =
        when (val decision = payment.status.providerStatus(statusUpdate)) {
          is PaymentStatusDecision.ConflictingTerminalOutcome ->
              StatusUpdateApplyResult.ConflictingTerminalOutcome(
                  decision.persistedStatus,
                  decision.requestedStatus,
              )
          PaymentStatusDecision.NoChange -> {
            if (payment.resolvesProviderStateReconciliationAfterVerifiedStatus()) {
              clearVerifiedAwaitingCustomerReconciliation(payment.paymentId)
            }
            val nextStatus = payment.status
            val terminalReason =
                terminalReason(
                    payment,
                    nextStatus,
                    requestedCancellationReason,
                    providerCancellationReason,
                )
            if (
                nextStatus == PaymentStatus.CANCELLED &&
                    terminalReason != TerminalReason.CAUSE_UNDETERMINED
            ) {
              terminalReason?.let { resolveUndeterminedCancellation(paymentId, it) }
            }
            markCancellationResolvedIfConfirmed(
                paymentId,
                payment.toPaymentLifecycleState(),
                nextStatus,
                terminalReason,
            )
            markWebhookStripeEventApplied(
                webhookConsumerEventId,
                webhookStripeEventId,
                paymentId,
                stripeStatus,
                providerCancellationReason,
            )
            StatusUpdateApplyResult.Applied
          }
          is PaymentStatusDecision.Change -> {
            val nextStatus = decision.statusChange.nextStatus
            val terminalReason =
                terminalReason(
                    payment,
                    nextStatus,
                    requestedCancellationReason,
                    providerCancellationReason,
                )
            when (
                val transition =
                    applyPaymentStatusChange(paymentId, decision.statusChange, terminalReason)
            ) {
              PaymentStatusTransition.Updated -> {
                markCancellationResolvedIfConfirmed(
                    paymentId,
                    payment.toPaymentLifecycleState(),
                    nextStatus,
                    terminalReason,
                )
                markWebhookStripeEventApplied(
                    webhookConsumerEventId,
                    webhookStripeEventId,
                    paymentId,
                    stripeStatus,
                    providerCancellationReason,
                )
                StatusUpdateApplyResult.Applied
              }
              PaymentStatusTransition.MatchingTerminalOutcome -> {
                markCancellationResolvedIfConfirmed(
                    paymentId,
                    payment.toPaymentLifecycleState(),
                    nextStatus,
                    terminalReason,
                )
                markWebhookStripeEventApplied(
                    webhookConsumerEventId,
                    webhookStripeEventId,
                    paymentId,
                    stripeStatus,
                    providerCancellationReason,
                )
                StatusUpdateApplyResult.Applied
              }
              is PaymentStatusTransition.ConflictingTerminalOutcome -> {
                StatusUpdateApplyResult.ConflictingTerminalOutcome(
                    transition.persistedStatus,
                    transition.requestedStatus,
                )
              }
              is PaymentStatusTransition.RefreshRequired -> {
                StatusUpdateApplyResult.RefreshRequired
              }
            }
          }
        }
    result
  }

  private fun terminalReason(
      payment: PaymentWorkflowState,
      nextStatus: PaymentStatus,
      requestedCancellationReason: TerminalReason?,
      providerCancellationReason: String?,
  ): TerminalReason? =
      when {
        nextStatus != PaymentStatus.CANCELLED -> null
        requestedCancellationReason != null -> requestedCancellationReason
        payment.cancellationOrigin != null ->
            corroboratedLocalCancellationReason(
                payment.cancellationOrigin,
                providerCancellationReason,
            ) ?: TerminalReason.CAUSE_UNDETERMINED
        payment.status == PaymentStatus.CANCELLED && payment.reconciliationRequired ->
            corroboratedLocalCancellationReason(
                payment.cancellationOrigin,
                providerCancellationReason,
            ) ?: TerminalReason.CAUSE_UNDETERMINED
        else -> TerminalReason.PROVIDER_CANCELLED
      }

  private suspend fun PaymentWorkflowPersistence.markWebhookStripeEventApplied(
      webhookConsumerEventId: UUID?,
      webhookStripeEventId: String?,
      paymentId: UUID,
      stripeStatus: String,
      cancellationReason: String?,
  ) {
    val stripePaymentIntentId = requireNotNull(load(paymentId)?.intentId)
    val stripeEventId =
        webhookStripeEventId
            ?: webhookConsumerEventId?.let { consumerEventId ->
              findWebhookStripeEventId(paymentId, consumerEventId, stripePaymentIntentId)
            }
    stripeEventId?.let {
      markWebhookStripeEventApplied(
          it,
          paymentId,
          stripePaymentIntentId,
          stripeStatus,
          cancellationReason,
      )
    }
  }

  private suspend fun PaymentWorkflowPersistence.markCancellationResolvedIfConfirmed(
      paymentId: UUID,
      lifecycle: PaymentLifecycleState,
      status: PaymentStatus,
      terminalReason: TerminalReason?,
  ) {
    if (lifecycle.cancellationIsResolvedBy(status, terminalReason)) {
      markCancellationResolved(paymentId)
    }
  }
}

internal sealed interface StatusUpdateApplyResult {
  data object Applied : StatusUpdateApplyResult

  data object RefreshRequired : StatusUpdateApplyResult

  data class ConflictingTerminalOutcome(
      val persistedStatus: PaymentStatus,
      val requestedStatus: PaymentStatus,
  ) : StatusUpdateApplyResult
}
