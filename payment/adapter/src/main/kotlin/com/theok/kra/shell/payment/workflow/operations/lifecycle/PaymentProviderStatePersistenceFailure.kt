package com.theok.kra.shell.payment.workflow.operations.lifecycle

import java.util.UUID

/** A provider fact was obtained, but applying it to durable payment state failed. */
internal class PaymentProviderStatePersistenceFailure(paymentId: UUID, cause: Throwable) :
    RuntimeException("Payment $paymentId requires provider-state reconciliation", cause) {
  private companion object {
    const val serialVersionUID = 1L
  }
}
