package com.theok.kra.shell.payment.persistence.request

import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST_LIFECYCLE_EVENT
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import reactor.core.publisher.Flux

/** Appends one immutable request lifecycle fact in the enclosing transaction. */
internal suspend fun DSLContext.appendPaymentRequestLifecycleEvent(
    paymentRequestId: UUID,
    eventType: PaymentRequestLifecycleEventType,
    fromStatus: String? = null,
    toStatus: String? = null,
    reason: String? = null,
    initiatedBySubject: String? = null,
) {
  Flux.from(
          insertInto(PAYMENT_REQUEST_LIFECYCLE_EVENT)
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.ID, UUID.randomUUID())
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.PAYMENT_REQUEST_ID, paymentRequestId)
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.EVENT_TYPE, eventType.name)
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.FROM_STATUS, fromStatus)
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.TO_STATUS, toStatus)
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.REASON, reason?.take(512))
              .set(PAYMENT_REQUEST_LIFECYCLE_EVENT.INITIATED_BY_SUBJECT, initiatedBySubject),
      )
      .single()
      .awaitSingle()
}

/** Immutable audit facts for the request-owned saga lifecycle. */
internal enum class PaymentRequestLifecycleEventType {
  ACCEPTED,
  REGISTERED,
  INTENT_CREATED,
  INTENT_CREATION_UNCERTAIN,
  PAYMENT_STATUS_CHANGED,
  CANCELLATION_REQUESTED,
  CANCELLATION_CAUSE_RESOLVED,
  CANCELLED,
  CHECKOUT_EXPIRED,
  RECONCILIATION_REQUIRED,
  RECONCILIATION_RESOLVED,
  OPERATIONAL_FAILURE,
  RETRY_REQUESTED,
  REENTERED,
}
