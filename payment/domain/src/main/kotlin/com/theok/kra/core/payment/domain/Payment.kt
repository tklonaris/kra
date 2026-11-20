package com.theok.kra.core.payment.domain

import java.time.Instant
import java.util.UUID

/** Opaque, immutable identity that owns a payment and scopes customer access. */
@JvmInline
value class PaymentOwnerId(val value: String) {
  init {
    require(value.isNotBlank()) { "Payment owner ID must not be blank" }
    require(value.length <= MAX_LENGTH) {
      "Payment owner ID must not exceed $MAX_LENGTH characters"
    }
  }

  private companion object {
    const val MAX_LENGTH = 512
  }
}

enum class PaymentStatus(val terminal: Boolean = false) {
  REQUESTED,
  AWAITING_CUSTOMER,
  PROCESSING,
  SUCCEEDED(terminal = true),
  CANCELLED(terminal = true),
  ;

  /** Checkout expiry may cancel pre-processing payment work, never an in-flight payment. */
  val checkoutExpiryApplies: Boolean
    get() = this == REQUESTED || this == AWAITING_CUSTOMER

  companion object {
    val terminalStatuses = entries.filter { it.terminal }
  }
}

enum class IntentCreationState {
  NOT_STARTED,
  ATTEMPTING,
  KNOWN,
  UNCERTAIN,
  ;

  val mayHaveUnrecordedProviderIntent: Boolean
    get() = this == ATTEMPTING || this == UNCERTAIN
}

enum class TerminalReason {
  PROVIDER_CANCELLED,
  CHECKOUT_EXPIRED,
  CUSTOMER_CANCELLED,
  CAUSE_UNDETERMINED,
  ;

  val isLocalCancellation: Boolean
    get() = this == CHECKOUT_EXPIRED || this == CUSTOMER_CANCELLED

  val resolvesUndeterminedCancellation: Boolean
    get() = this != CAUSE_UNDETERMINED
}

enum class PaymentRequestSetupStatus {
  PENDING,
  AVAILABLE,
  EXPIRED,
  UNAVAILABLE,
}

/** Pure setup-read policy; query projections supply the payment facts directly. */
fun paymentRequestSetupStatus(
    status: PaymentStatus,
    checkoutDeadlineAt: Instant,
    now: Instant,
): PaymentRequestSetupStatus =
    when {
      status.terminal -> PaymentRequestSetupStatus.UNAVAILABLE
      !now.isBefore(checkoutDeadlineAt) -> PaymentRequestSetupStatus.EXPIRED
      status == PaymentStatus.AWAITING_CUSTOMER -> PaymentRequestSetupStatus.AVAILABLE
      else -> PaymentRequestSetupStatus.PENDING
    }

/** Provider-neutral payment status facts supplied by the imperative Stripe adapter. */
enum class PaymentStatusUpdate {
  AWAITING_CUSTOMER,
  PROCESSING,
  SUCCEEDED,
  CANCELLED,
}

/** A pure, domain-issued expected-state transition for persistence to apply atomically. */
sealed interface PaymentStatusChange {
  val expectedStatus: PaymentStatus
  val nextStatus: PaymentStatus
}

private data class ExpectedPaymentStatusChange(
    override val expectedStatus: PaymentStatus,
    override val nextStatus: PaymentStatus,
) : PaymentStatusChange

sealed interface PaymentStatusDecision {
  data class Change(val statusChange: PaymentStatusChange) : PaymentStatusDecision

  data object NoChange : PaymentStatusDecision

  data class ConflictingTerminalOutcome(
      val persistedStatus: PaymentStatus,
      val requestedStatus: PaymentStatus,
  ) : PaymentStatusDecision
}

/** Durable lifecycle facts needed to make cancellation decisions and apply their outcomes. */
data class PaymentLifecycleState(
    val status: PaymentStatus,
    val intentCreationState: IntentCreationState,
    val providerIntentRecorded: Boolean,
    val cancellationRequested: Boolean,
    val cancellationOrigin: TerminalReason?,
) {
  fun cancelWithoutProviderIntent(): PaymentStatusChange {
    check(status == PaymentStatus.REQUESTED) {
      "Only requested payments may be cancelled without a provider intent"
    }
    check(intentCreationState == IntentCreationState.NOT_STARTED) {
      "A payment with an attempted provider intent cannot be cancelled locally"
    }
    check(!providerIntentRecorded) {
      "A payment with a provider intent cannot be cancelled locally"
    }
    return paymentStatusChange(status, PaymentStatus.CANCELLED)
  }

  /** A locally claimed cancellation is settled only by a determinate terminal outcome. */
  fun cancellationIsResolvedBy(
      resultingStatus: PaymentStatus,
      resultingTerminalReason: TerminalReason?,
  ): Boolean =
      cancellationOrigin != null &&
          when (resultingStatus) {
            PaymentStatus.SUCCEEDED -> true
            PaymentStatus.CANCELLED ->
                resultingTerminalReason != null &&
                    resultingTerminalReason != TerminalReason.CAUSE_UNDETERMINED
            else -> false
          }
}

fun PaymentStatus.providerStatus(statusUpdate: PaymentStatusUpdate): PaymentStatusDecision =
    transitionTo(PaymentStatus.valueOf(statusUpdate.name))

fun PaymentStatus.intentCreated(): PaymentStatusChange =
    when (val decision = transitionTo(PaymentStatus.AWAITING_CUSTOMER)) {
      is PaymentStatusDecision.Change -> decision.statusChange
      PaymentStatusDecision.NoChange -> error("Payment status $this cannot await customer")
      is PaymentStatusDecision.ConflictingTerminalOutcome ->
          error("Payment status $this conflicts with awaiting customer")
    }

private fun PaymentStatus.transitionTo(requestedStatus: PaymentStatus): PaymentStatusDecision =
    when {
      terminal && requestedStatus.terminal && requestedStatus != this ->
          PaymentStatusDecision.ConflictingTerminalOutcome(this, requestedStatus)
      terminal -> PaymentStatusDecision.NoChange
      requestedStatus == PaymentStatus.AWAITING_CUSTOMER && this != PaymentStatus.REQUESTED ->
          PaymentStatusDecision.NoChange
      requestedStatus == this -> PaymentStatusDecision.NoChange
      else -> PaymentStatusDecision.Change(paymentStatusChange(this, requestedStatus))
    }

private fun paymentStatusChange(
    expectedStatus: PaymentStatus,
    nextStatus: PaymentStatus,
): PaymentStatusChange = ExpectedPaymentStatusChange(expectedStatus, nextStatus)

/** Immutable commercial terms accepted with a payment request. */
data class PaymentSpec(
    val purchaseReference: UUID,
    val amountMinor: Long,
    val currency: String,
) {
  init {
    require(amountMinor > 0) { "Payment amount must be positive" }
    require(currency == "AUD") { "Only AUD is supported" }
  }
}
