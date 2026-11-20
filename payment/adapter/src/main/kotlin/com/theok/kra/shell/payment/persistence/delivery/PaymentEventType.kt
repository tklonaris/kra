package com.theok.kra.shell.payment.persistence.delivery

import com.theok.kra.core.payment.domain.PaymentStatus

/** Payment integration-event contract shared by the outbox and its durable consumer receipt. */
enum class PaymentEventType(
    val eventName: String,
    val wakesSaga: Boolean,
) {
  REQUEST_ACCEPTED("PaymentRequestAccepted", true),
  RETRY_REQUESTED("PaymentRetryRequested", true),
  CANCELLATION_REQUESTED("PaymentCancellationRequested", true),
  WEBHOOK_RECEIVED("PaymentWebhookReceived", true),
  SUCCEEDED("PaymentSucceeded", false),
  CANCELLED("PaymentCancelled", false);

  companion object {
    fun fromEventName(eventName: String): PaymentEventType =
        entries.firstOrNull { it.eventName == eventName }
            ?: error("Unknown payment event type $eventName")

    fun forTerminalStatus(status: PaymentStatus): PaymentEventType? =
        when (status) {
          PaymentStatus.SUCCEEDED -> SUCCEEDED
          PaymentStatus.CANCELLED -> CANCELLED
          else -> null
        }
  }
}
