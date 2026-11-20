package com.theok.kra.shell.payment.queries

import com.theok.kra.core.payment.domain.PaymentRequestSetupStatus
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.core.payment.domain.paymentRequestSetupStatus
import com.theok.kra.core.payment.dto.PaymentRequestSetup
import com.theok.kra.core.payment.dto.PaymentRequestSnapshot
import com.theok.kra.core.payment.dto.PaymentSnapshot
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.crypto.EncryptedClientSecret
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.time.Instant
import org.jooq.Record

/** Maps payment read records to the public payment projection. */
internal fun Record.toPaymentRequestSnapshot() =
    PaymentRequestSnapshot(
        paymentRequestId = requireNotNull(get(PAYMENT_REQUEST.ID)),
        purchaseReference = requireNotNull(get(PAYMENT_REQUEST.PURCHASE_REFERENCE)),
        amountMinor = requireNotNull(get(PAYMENT_REQUEST.AMOUNT_MINOR)),
        currency = requireNotNull(get(PAYMENT_REQUEST.CURRENCY)).trim(),
        status = PaymentRequestStatus.valueOf(requireNotNull(get(PAYMENT_REQUEST.STATUS))),
        checkoutDeadlineAt = requireNotNull(get(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT)).toInstant(),
        processingDeadlineAt =
            requireNotNull(get(PAYMENT_REQUEST.PROCESSING_DEADLINE_AT)).toInstant(),
        payment = toPaymentSnapshot(),
    )

internal fun Record.toPaymentSnapshot(): PaymentSnapshot? =
    get(PAYMENT.ID)?.let { paymentId ->
      PaymentSnapshot(
          paymentId = paymentId,
          status = PaymentStatus.valueOf(requireNotNull(get(PAYMENT.STATUS))),
          stripePaymentIntentId = get(PAYMENT.PROVIDER_PAYMENT_ID),
          terminalReason = get(PAYMENT.TERMINAL_REASON)?.let(TerminalReason::valueOf),
          reconciliationRequired = requireNotNull(get(PAYMENT.RECONCILIATION_REQUIRED)),
      )
    }

internal fun Record.toSetup(now: Instant, cipher: ClientSecretCipher): PaymentRequestSetup {
  val paymentId = get(PAYMENT.ID)
  val requestStatus = PaymentRequestStatus.valueOf(requireNotNull(get(PAYMENT_REQUEST.STATUS)))
  if (requestStatus.preventsCheckoutSetup()) {
    return PaymentRequestSetup(PaymentRequestSetupStatus.UNAVAILABLE)
  }
  if (paymentId == null) {
    val checkoutDeadline = requireNotNull(get(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT)).toInstant()
    return if (now.isBefore(checkoutDeadline)) {
      PaymentRequestSetup(PaymentRequestSetupStatus.PENDING)
    } else PaymentRequestSetup(PaymentRequestSetupStatus.EXPIRED)
  }
  val status =
      paymentRequestSetupStatus(
          PaymentStatus.valueOf(requireNotNull(get(PAYMENT.STATUS))),
          requireNotNull(get(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT)).toInstant(),
          now,
      )
  if (status != PaymentRequestSetupStatus.AVAILABLE) return PaymentRequestSetup(status)

  val clientSecret =
      get(PAYMENT.CLIENT_SECRET_NONCE)
          ?.let { nonce ->
            val ciphertext = get(PAYMENT.CLIENT_SECRET_CIPHERTEXT)
            val tag = get(PAYMENT.CLIENT_SECRET_TAG)
            if (ciphertext == null || tag == null) null
            else EncryptedClientSecret(nonce, ciphertext, tag)
          }
          ?.let { cipher.decrypt(paymentId.toString(), it) }

  return if (clientSecret == null) PaymentRequestSetup(PaymentRequestSetupStatus.PENDING)
  else PaymentRequestSetup(status, clientSecret)
}
