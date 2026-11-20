package com.theok.kra.shell.payment.persistence.delivery

import java.util.UUID

/** Verified Stripe webhook fact awaiting durable payment association. */
data class PaymentStripeEvent(
    val stripeEventId: String,
    val stripePaymentIntentId: String,
    val eventType: String,
    val amountMinor: Long,
    val currency: String,
    val candidatePaymentId: UUID?,
)

enum class WebhookAcceptance {
  Accepted,
  Duplicate,
  AuditOnly,
  Ignored,
}
