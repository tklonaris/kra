package com.theok.kra.shell.payment.persistence.delivery

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_CONSUMER_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_WEBHOOK_HANDOFF
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import reactor.core.publisher.Flux

/** Transaction-scoped persistence of the saga wake-up for a verified Stripe webhook event. */
internal class PaymentStripeWebhookHandoffPersister(
    private val dsl: DSLContext,
    private val objectMapper: ObjectMapper,
) {
  suspend fun append(paymentRequestId: UUID, paymentId: UUID, stripeEventId: String) {
    val nextConsumerEventId = UUID.randomUUID()
    val consumerEventId =
        if (claimInitialHandoff(paymentId, stripeEventId, nextConsumerEventId)) nextConsumerEventId
        else {
          val previousConsumerEventId = loadHandoffForUpdate(paymentId, stripeEventId)
          if (
              isSettled(previousConsumerEventId) &&
                  replaceSettledHandoff(
                      paymentId,
                      stripeEventId,
                      previousConsumerEventId,
                      nextConsumerEventId,
                  )
          ) {
            nextConsumerEventId
          } else null
        }
    if (consumerEventId != null) {
      dsl.appendPaymentOutbox(
          paymentRequestId,
          paymentId,
          PaymentEventType.WEBHOOK_RECEIVED,
          objectMapper,
          stripeEventId,
          consumerEventId,
      )
    }
  }

  private suspend fun claimInitialHandoff(
      paymentId: UUID,
      stripeEventId: String,
      consumerEventId: UUID,
  ): Boolean =
      Flux.from(
              dsl.insertInto(PAYMENT_WEBHOOK_HANDOFF)
                  .set(PAYMENT_WEBHOOK_HANDOFF.PAYMENT_ID, paymentId)
                  .set(PAYMENT_WEBHOOK_HANDOFF.PROVIDER_EVENT_ID, stripeEventId)
                  .set(PAYMENT_WEBHOOK_HANDOFF.OUTBOX_EVENT_ID, consumerEventId)
                  .onConflict(
                      PAYMENT_WEBHOOK_HANDOFF.PAYMENT_ID,
                      PAYMENT_WEBHOOK_HANDOFF.PROVIDER_EVENT_ID,
                  )
                  .doNothing(),
          )
          .single()
          .awaitSingle() == 1

  private suspend fun loadHandoffForUpdate(paymentId: UUID, stripeEventId: String): UUID =
      requireNotNull(
          Flux.from(
                  dsl.select(PAYMENT_WEBHOOK_HANDOFF.OUTBOX_EVENT_ID)
                      .from(PAYMENT_WEBHOOK_HANDOFF)
                      .where(PAYMENT_WEBHOOK_HANDOFF.PAYMENT_ID.eq(paymentId))
                      .and(PAYMENT_WEBHOOK_HANDOFF.PROVIDER_EVENT_ID.eq(stripeEventId))
                      .forUpdate(),
              )
              .next()
              .awaitSingleOrNull()
              ?.get(PAYMENT_WEBHOOK_HANDOFF.OUTBOX_EVENT_ID),
      ) {
        "Webhook hand-off conflict must retain its owner row"
      }

  private suspend fun isSettled(consumerEventId: UUID): Boolean {
    val status =
        Flux.from(
                dsl.select(PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS)
                    .from(PAYMENT_CONSUMER_EVENT)
                    .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId)),
            )
            .next()
            .awaitSingleOrNull()
            ?.get(PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS)
            ?.let(PaymentConsumerEventStatus::valueOf)
    return status != null && status != PaymentConsumerEventStatus.PENDING
  }

  private suspend fun replaceSettledHandoff(
      paymentId: UUID,
      stripeEventId: String,
      previousConsumerEventId: UUID,
      nextConsumerEventId: UUID,
  ): Boolean =
      Flux.from(
              dsl.update(PAYMENT_WEBHOOK_HANDOFF)
                  .set(PAYMENT_WEBHOOK_HANDOFF.OUTBOX_EVENT_ID, nextConsumerEventId)
                  .where(PAYMENT_WEBHOOK_HANDOFF.PAYMENT_ID.eq(paymentId))
                  .and(PAYMENT_WEBHOOK_HANDOFF.PROVIDER_EVENT_ID.eq(stripeEventId))
                  .and(PAYMENT_WEBHOOK_HANDOFF.OUTBOX_EVENT_ID.eq(previousConsumerEventId)),
          )
          .single()
          .awaitSingle() == 1
}
