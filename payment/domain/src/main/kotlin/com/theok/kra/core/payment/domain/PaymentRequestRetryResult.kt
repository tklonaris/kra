package com.theok.kra.core.payment.domain

/** Result of accepting an operator-requested retry for a failed payment request. */
enum class PaymentRequestRetryResult {
  ACCEPTED,
  NOT_FOUND,
  NOT_ELIGIBLE,
}
