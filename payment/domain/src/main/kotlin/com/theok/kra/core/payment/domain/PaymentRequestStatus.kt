package com.theok.kra.core.payment.domain

enum class PaymentRequestStatus {
  ACCEPTED,
  CANCELLED,
  CREATING_PROVIDER_INTENT,
  WAITING_FOR_CUSTOMER,
  WAITING_FOR_PROCESSING_OUTCOME,
  RECONCILING_CANCELLATION,
  RECONCILING_PROVIDER_STATE,
  COMPLETED,
  FAILED;

  /** Status policy stays with the status value; persistence supplies the concurrency fence. */
  fun canTransitionTo(next: PaymentRequestStatus): Boolean =
      when (this) {
        ACCEPTED -> next in setOf(ACCEPTED, CANCELLED, CREATING_PROVIDER_INTENT, FAILED)
        CANCELLED -> next == CANCELLED
        CREATING_PROVIDER_INTENT ->
            next in
                setOf(
                    CREATING_PROVIDER_INTENT,
                    WAITING_FOR_CUSTOMER,
                    RECONCILING_CANCELLATION,
                    RECONCILING_PROVIDER_STATE,
                    COMPLETED,
                    FAILED,
                )
        WAITING_FOR_CUSTOMER ->
            next in
                setOf(
                    WAITING_FOR_CUSTOMER,
                    WAITING_FOR_PROCESSING_OUTCOME,
                    RECONCILING_CANCELLATION,
                    RECONCILING_PROVIDER_STATE,
                    COMPLETED,
                    FAILED,
                )
        WAITING_FOR_PROCESSING_OUTCOME ->
            next in
                setOf(
                    WAITING_FOR_PROCESSING_OUTCOME,
                    RECONCILING_CANCELLATION,
                    RECONCILING_PROVIDER_STATE,
                    COMPLETED,
                    FAILED,
                )
        RECONCILING_CANCELLATION ->
            next in setOf(RECONCILING_CANCELLATION, RECONCILING_PROVIDER_STATE, COMPLETED, FAILED)
        RECONCILING_PROVIDER_STATE ->
            next in
                setOf(
                    RECONCILING_PROVIDER_STATE,
                    WAITING_FOR_CUSTOMER,
                    RECONCILING_CANCELLATION,
                    COMPLETED,
                    FAILED,
                )
        COMPLETED ->
            next in setOf(COMPLETED, FAILED, RECONCILING_CANCELLATION, RECONCILING_PROVIDER_STATE)
        FAILED ->
            next in
                setOf(
                    FAILED,
                    CANCELLED,
                    CREATING_PROVIDER_INTENT,
                    WAITING_FOR_CUSTOMER,
                    WAITING_FOR_PROCESSING_OUTCOME,
                    RECONCILING_CANCELLATION,
                    RECONCILING_PROVIDER_STATE,
                    COMPLETED,
                )
      }

  fun requireTransitionTo(next: PaymentRequestStatus) {
    check(canTransitionTo(next)) { "Payment request status $this cannot transition to $next" }
  }

  fun allowsProviderIntentRecovery(): Boolean =
      this in
          setOf(
              CREATING_PROVIDER_INTENT,
              RECONCILING_CANCELLATION,
              RECONCILING_PROVIDER_STATE,
          )

  fun allowsPaymentRegistration(): Boolean = this == ACCEPTED || this == FAILED

  fun allowsOperationalRetry(): Boolean = this == FAILED

  fun acceptsCancellationRequest(): Boolean = this != CANCELLED && this != COMPLETED

  fun preventsCheckoutSetup(): Boolean =
      this in
          setOf(
              CANCELLED,
              RECONCILING_CANCELLATION,
              RECONCILING_PROVIDER_STATE,
              COMPLETED,
              FAILED,
          )

  fun nextForPaymentStatus(
      paymentStatus: PaymentStatus,
      reconciliationRequired: Boolean,
  ): PaymentRequestStatus =
      when (paymentStatus) {
        PaymentStatus.REQUESTED -> this
        PaymentStatus.AWAITING_CUSTOMER -> WAITING_FOR_CUSTOMER
        PaymentStatus.PROCESSING -> WAITING_FOR_PROCESSING_OUTCOME
        PaymentStatus.SUCCEEDED -> COMPLETED
        PaymentStatus.CANCELLED ->
            if (reconciliationRequired) RECONCILING_PROVIDER_STATE else COMPLETED
      }

  fun reenterForPayment(
      paymentStatus: PaymentStatus,
      reconciliationRequired: Boolean,
      cancellationRequested: Boolean,
  ): PaymentRequestStatus =
      when {
        paymentStatus.terminal -> nextForPaymentStatus(paymentStatus, reconciliationRequired)
        cancellationRequested -> RECONCILING_CANCELLATION
        paymentStatus == PaymentStatus.REQUESTED && reconciliationRequired ->
            RECONCILING_PROVIDER_STATE
        paymentStatus == PaymentStatus.REQUESTED -> CREATING_PROVIDER_INTENT
        else -> nextForPaymentStatus(paymentStatus, reconciliationRequired)
      }
}
