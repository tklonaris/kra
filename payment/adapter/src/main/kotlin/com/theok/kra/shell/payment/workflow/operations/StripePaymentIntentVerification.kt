package com.theok.kra.shell.payment.workflow.operations

import com.stripe.model.PaymentIntent
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState

/** Verifies that Stripe facts remain bound to the durable payment commercial terms. */
internal fun PaymentIntent.matchesCommercialTerms(payment: PaymentWorkflowState): Boolean =
    amount == payment.amountMinor &&
        currency.equals(payment.currency, ignoreCase = true) &&
        metadata["kra_payment_id"] == payment.paymentId.toString()

/** Verifies a Stripe fact against both the known provider identity and commercial terms. */
internal fun PaymentIntent.matchesKnownPayment(
    expectedIntentId: String,
    payment: PaymentWorkflowState,
): Boolean = id == expectedIntentId && matchesCommercialTerms(payment)

/** Validates a create response whose provider identity has not yet been durably recorded. */
internal fun PaymentIntent.requireCreatedIntentId(payment: PaymentWorkflowState): String {
  val intentId = requireNotNull(id) { "Stripe create response has no payment intent ID" }
  check(intentId.isNotBlank()) { "Stripe create response has a blank payment intent ID" }
  check(matchesCommercialTerms(payment)) {
    "Stripe create response does not match payment ${payment.paymentId}"
  }
  return intentId
}
