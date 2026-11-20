package com.theok.kra.shell.payment.persistence.delivery

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_OUTBOX
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import reactor.core.publisher.Flux

/** Appends one request-scoped integration event through the transaction-bound DSL context. */
internal suspend fun DSLContext.appendPaymentOutbox(
    paymentRequestId: UUID,
    paymentId: UUID?,
    eventType: PaymentEventType,
    objectMapper: ObjectMapper,
    stripeEventId: String? = null,
    consumerEventId: UUID = UUID.randomUUID(),
) {
  val payload =
      objectMapper
          .createObjectNode()
          .put("eventId", consumerEventId.toString())
          .put("paymentRequestId", paymentRequestId.toString())
          .put("eventType", eventType.eventName)
  paymentId?.let { payload.put("paymentId", it.toString()) }
  stripeEventId?.let { payload.put("stripeEventId", it) }
  val insert =
      insertInto(PAYMENT_OUTBOX)
          .set(PAYMENT_OUTBOX.EVENT_ID, consumerEventId)
          .set(PAYMENT_OUTBOX.AGGREGATE_ID, paymentRequestId)
          .set(PAYMENT_OUTBOX.AGGREGATE_TYPE, "payment")
          .set(PAYMENT_OUTBOX.PAYMENT_REQUEST_ID, paymentRequestId)
          .set(PAYMENT_OUTBOX.PAYMENT_ID, paymentId)
          .set(PAYMENT_OUTBOX.EVENT_TYPE, eventType.eventName)
          .set(PAYMENT_OUTBOX.PROVIDER_EVENT_ID, stripeEventId)
          .set(PAYMENT_OUTBOX.PAYLOAD, payload)
  Flux.from(insert).single().awaitSingle()
}
