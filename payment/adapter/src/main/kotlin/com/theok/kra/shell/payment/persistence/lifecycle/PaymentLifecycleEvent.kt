package com.theok.kra.shell.payment.persistence.lifecycle

import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_LIFECYCLE_EVENT
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import reactor.core.publisher.Flux

/**
 * Immutable aggregate-history facts, distinct from provider, consumer-delivery, and outbox records.
 */
enum class PaymentLifecycleEventType {
  PAYMENT_CREATED,
  INTENT_CREATION_CLAIMED,
  INTENT_CREATED,
  INTENT_CREATION_UNCERTAIN,
  CANCELLATION_ORIGIN_CLAIMED,
  STATUS_CHANGED,
  RECONCILIATION_REQUIRED,
  RECONCILIATION_RESOLVED,
  CANCELLATION_CAUSE_RESOLVED,
}

internal suspend fun DSLContext.insertPaymentLifecycleEvent(
    paymentId: UUID,
    eventType: PaymentLifecycleEventType,
    fromStatus: PaymentStatus? = null,
    toStatus: PaymentStatus? = null,
    terminalReason: TerminalReason? = null,
    reconciliationRequired: Boolean? = null,
    reason: String? = null,
) {
  Flux.from(
          insertInto(PAYMENT_LIFECYCLE_EVENT)
              .set(PAYMENT_LIFECYCLE_EVENT.EVENT_ID, UUID.randomUUID())
              .set(PAYMENT_LIFECYCLE_EVENT.PAYMENT_ID, paymentId)
              .set(PAYMENT_LIFECYCLE_EVENT.EVENT_TYPE, eventType.name)
              .set(PAYMENT_LIFECYCLE_EVENT.FROM_STATUS, fromStatus?.name)
              .set(PAYMENT_LIFECYCLE_EVENT.TO_STATUS, toStatus?.name)
              .set(PAYMENT_LIFECYCLE_EVENT.TERMINAL_REASON, terminalReason?.name)
              .set(PAYMENT_LIFECYCLE_EVENT.RECONCILIATION_REQUIRED, reconciliationRequired)
              .set(PAYMENT_LIFECYCLE_EVENT.REASON, reason?.take(255)),
      )
      .single()
      .awaitSingle()
}
