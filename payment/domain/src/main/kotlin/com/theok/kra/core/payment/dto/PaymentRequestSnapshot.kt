package com.theok.kra.core.payment.dto

import com.theok.kra.core.payment.domain.PaymentRequestSetupStatus
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import java.time.Instant
import java.util.UUID

data class PaymentRequestSnapshot(
    val paymentRequestId: UUID,
    val purchaseReference: UUID,
    val amountMinor: Long,
    val currency: String,
    val status: PaymentRequestStatus,
    val checkoutDeadlineAt: Instant,
    val processingDeadlineAt: Instant,
    val payment: PaymentSnapshot?,
)

data class PaymentSnapshot(
    val paymentId: UUID,
    val status: PaymentStatus,
    val stripePaymentIntentId: String?,
    val terminalReason: TerminalReason?,
    val reconciliationRequired: Boolean,
)

/** Immutable outcome recorded when a payment request is first accepted. */
data class PaymentRequestAcceptance(
    val paymentRequestId: UUID,
    val purchaseReference: UUID,
    val amountMinor: Long,
    val currency: String,
    val status: PaymentRequestStatus,
    val checkoutDeadlineAt: Instant,
) {
  init {
    require(status == PaymentRequestStatus.ACCEPTED) {
      "A payment request acceptance must record the accepted initial status"
    }
  }
}

data class PaymentRequestSetup(
    val status: PaymentRequestSetupStatus,
    val clientSecret: String? = null,
) {
  init {
    require((status == PaymentRequestSetupStatus.AVAILABLE) == (clientSecret != null)) {
      "Only an available payment setup may contain a client secret"
    }
  }
}
