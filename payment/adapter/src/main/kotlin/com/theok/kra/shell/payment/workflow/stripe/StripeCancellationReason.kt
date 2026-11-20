package com.theok.kra.shell.payment.workflow.stripe

import com.theok.kra.core.payment.domain.TerminalReason

/** Returns a locally claimed cancellation cause only when Stripe corroborates that same cause. */
internal fun corroboratedLocalCancellationReason(
    claimedReason: TerminalReason?,
    stripeReason: String?,
): TerminalReason? = claimedReason?.takeIf {
  when (it) {
    TerminalReason.CUSTOMER_CANCELLED -> stripeReason == "requested_by_customer"
    TerminalReason.CHECKOUT_EXPIRED -> stripeReason == "abandoned"
    else -> false
  }
}
