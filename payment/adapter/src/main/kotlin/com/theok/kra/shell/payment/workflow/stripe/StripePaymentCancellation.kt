package com.theok.kra.shell.payment.workflow.stripe

import com.stripe.exception.StripeException
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCancelParams
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentIntentStatusRefresher
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentProviderStatePersistenceFailure
import java.util.UUID

/** Performs Stripe payment-intent cancellation I/O and persists the resulting provider facts. */
internal class StripePaymentCancellation(
    private val workflowPersister: PaymentWorkflowPersister,
    private val stripePaymentGateway: StripePaymentGateway,
    private val paymentIntentStatusRefresher: PaymentIntentStatusRefresher,
) {
  suspend fun retrieve(intentId: String): PaymentIntent = stripePaymentGateway.retrieve(intentId)

  fun isCancellable(intent: PaymentIntent): Boolean = intent.status in CANCELLABLE_STATUSES

  suspend fun applyAlreadyCancelled(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      intentId: String,
      intent: PaymentIntent,
      claimedReason: TerminalReason?,
  ) {
    val resolvedReason =
        corroboratedLocalCancellationReason(claimedReason, intent.cancellationReason)
            ?: if (claimedReason == null) {
              TerminalReason.PROVIDER_CANCELLED
            } else {
              TerminalReason.CAUSE_UNDETERMINED
            }
    paymentIntentStatusRefresher.applyWithRefresh(
        paymentId,
        webhookConsumerEventId,
        intentId,
        intent,
        resolvedReason,
    )
  }

  /** Refreshes after durable authorization, then cancels only if the latest fact permits it. */
  suspend fun reconcileAfterClaim(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      intentId: String,
      reason: TerminalReason,
  ) {
    val current = retrieve(intentId)
    when {
      current.status == "canceled" ->
          applyAlreadyCancelled(paymentId, webhookConsumerEventId, intentId, current, reason)
      isCancellable(current) -> cancelAndApply(paymentId, webhookConsumerEventId, intentId, reason)
      else ->
          applyNonCancellableStatus(paymentId, webhookConsumerEventId, intentId, current, reason)
    }
  }

  suspend fun applyNonCancellableStatus(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      intentId: String,
      intent: PaymentIntent,
      requestedCancellationReason: TerminalReason,
  ) {
    if (intent.status !in FINAL_OR_PROCESSING_STATUSES) {
      try {
        workflowPersister.transaction {
          markReconciliationRequired(
              paymentId,
              "Stripe status ${intent.status} could not be safely cancelled",
          )
        }
      } catch (failure: Exception) {
        throw PaymentProviderStatePersistenceFailure(paymentId, failure)
      }
    } else {
      paymentIntentStatusRefresher.applyWithRefresh(
          paymentId,
          webhookConsumerEventId,
          intentId,
          intent,
          requestedCancellationReason,
      )
    }
  }

  private suspend fun cancelAndApply(
      paymentId: UUID,
      webhookConsumerEventId: UUID?,
      intentId: String,
      reason: TerminalReason,
  ) {
    val cancelled =
        try {
          stripePaymentGateway.cancel(
              intentId,
              cancellationParams(reason),
              cancellationRequestOptions(paymentId, reason),
          )
        } catch (failure: StripeException) {
          val reconciled = retrieve(intentId)
          val resolvedReason =
              if (reconciled.status == "canceled") {
                corroboratedLocalCancellationReason(reason, reconciled.cancellationReason)
                    ?: TerminalReason.CAUSE_UNDETERMINED
              } else {
                null
              }
          paymentIntentStatusRefresher.applyWithRefresh(
              paymentId,
              webhookConsumerEventId,
              intentId,
              reconciled,
              resolvedReason,
          )
          if (isCancellable(reconciled)) throw failure
          return
        }
    paymentIntentStatusRefresher.applyWithRefresh(
        paymentId,
        webhookConsumerEventId,
        intentId,
        cancelled,
        reason,
    )
  }

  private fun cancellationRequestOptions(paymentId: UUID, origin: TerminalReason): RequestOptions =
      RequestOptions.builder()
          .setIdempotencyKey("kra-payment-cancel-$paymentId-${origin.name.lowercase()}")
          .build()

  private fun cancellationParams(origin: TerminalReason): PaymentIntentCancelParams =
      PaymentIntentCancelParams.builder()
          .setCancellationReason(
              when (origin) {
                TerminalReason.CUSTOMER_CANCELLED ->
                    PaymentIntentCancelParams.CancellationReason.REQUESTED_BY_CUSTOMER
                TerminalReason.CHECKOUT_EXPIRED ->
                    PaymentIntentCancelParams.CancellationReason.ABANDONED
                else ->
                    error("Only locally claimed cancellations have Stripe cancellation parameters")
              },
          )
          .build()

  private companion object {
    val CANCELLABLE_STATUSES =
        setOf(
            "requires_payment_method",
            "requires_capture",
            "requires_confirmation",
            "requires_action",
        )
    val FINAL_OR_PROCESSING_STATUSES = setOf("succeeded", "canceled", "processing")
  }
}
