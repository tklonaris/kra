package com.theok.kra.shell.payment.persistence.delivery

import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_CONSUMER_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_PROVIDER_EVENT
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Record
import reactor.core.publisher.Flux

/** Transaction-scoped lookup of Stripe events associated with a payment and consumer receipt. */
internal class PaymentStripeEventAssociationLoader(
    private val dsl: DSLContext,
) {

  suspend fun findWebhookStripeEventId(
      paymentId: UUID,
      consumerEventId: UUID,
      stripePaymentIntentId: String,
  ): String? =
      Flux.from(
              dsl.select(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)
                  .from(PAYMENT_CONSUMER_EVENT)
                  .join(PAYMENT_PROVIDER_EVENT)
                  .on(
                      PAYMENT_CONSUMER_EVENT.PROVIDER_EVENT_ID.eq(
                          PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID,
                      ),
                  )
                  .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId))
                  .and(PAYMENT_CONSUMER_EVENT.PAYMENT_ID.eq(paymentId))
                  .and(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(paymentId))
                  .and(
                      PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID.eq(stripePaymentIntentId),
                  ),
          )
          .next()
          .awaitSingleOrNull()
          ?.get(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)

  suspend fun findWebhookStripePaymentIntentId(paymentId: UUID, consumerEventId: UUID): String? =
      Flux.from(
              dsl.select(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID)
                  .from(PAYMENT_CONSUMER_EVENT)
                  .join(PAYMENT_PROVIDER_EVENT)
                  .on(
                      PAYMENT_CONSUMER_EVENT.PROVIDER_EVENT_ID.eq(
                          PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID,
                      ),
                  )
                  .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId))
                  .and(PAYMENT_CONSUMER_EVENT.PAYMENT_ID.eq(paymentId))
                  .and(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(paymentId)),
          )
          .next()
          .awaitSingleOrNull()
          ?.get(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID)

  /** Returns one already-associated Stripe event still awaiting durable application. */
  suspend fun loadUnappliedStripePaymentIntent(paymentId: UUID): UnappliedStripePaymentIntent? =
      loadUnappliedStripePaymentIntent(paymentId, null)

  /** Returns one unapplied event for an already-known Stripe intent. */
  suspend fun loadUnappliedStripePaymentIntent(
      paymentId: UUID,
      stripePaymentIntentId: String?,
  ): UnappliedStripePaymentIntent? =
      Flux.from(
              dsl.select(
                      PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID,
                      PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID,
                  )
                  .from(PAYMENT_PROVIDER_EVENT)
                  .where(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(paymentId))
                  .and(PAYMENT_PROVIDER_EVENT.APPLIED_AT.isNull)
                  .and(
                      stripePaymentIntentId?.let(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID::eq)
                          ?: org.jooq.impl.DSL.trueCondition(),
                  )
                  .orderBy(PAYMENT_PROVIDER_EVENT.RECEIVED_AT.asc()),
          )
          .next()
          .awaitSingleOrNull()
          ?.toUnappliedStripePaymentIntent()

  /** Returns associated Stripe events that were not yet applied to durable payment state. */
  suspend fun loadUnappliedStripeEventIds(paymentId: UUID): List<String> =
      Flux.from(
              dsl.select(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)
                  .from(PAYMENT_PROVIDER_EVENT)
                  .join(PAYMENT)
                  .on(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(PAYMENT.ID))
                  .where(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(paymentId))
                  .and(PAYMENT_PROVIDER_EVENT.APPLIED_AT.isNull)
                  .and(
                      PAYMENT.PROVIDER_PAYMENT_ID.isNull.or(
                          PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID.eq(
                              PAYMENT.PROVIDER_PAYMENT_ID,
                          ),
                      ),
                  ),
          )
          .map { requireNotNull(it.get(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)) }
          .collectList()
          .awaitSingle()

  private fun Record.toUnappliedStripePaymentIntent() =
      UnappliedStripePaymentIntent(
          requireNotNull(get(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)),
          requireNotNull(get(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID)),
      )
}

internal data class UnappliedStripePaymentIntent(
    val stripeEventId: String,
    val stripePaymentIntentId: String,
)
