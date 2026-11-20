package com.theok.kra.shell.payment.persistence.delivery

import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_PROVIDER_EVENT
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Transaction-scoped persistence of a Stripe event applied to durable payment state. */
internal class PaymentStripeEventApplicationPersister(
    private val dsl: DSLContext,
) {
  suspend fun markApplied(
      stripeEventId: String,
      paymentId: UUID,
      stripePaymentIntentId: String,
      stripeStatus: String,
      cancellationReason: String?,
  ) {
    check(
        Flux.from(
                dsl.update(PAYMENT_PROVIDER_EVENT)
                    .set(PAYMENT_PROVIDER_EVENT.APPLIED_AT, DSL.currentOffsetDateTime())
                    .set(PAYMENT_PROVIDER_EVENT.PROVIDER_STATUS, stripeStatus)
                    .set(PAYMENT_PROVIDER_EVENT.PROVIDER_CANCELLATION_REASON, cancellationReason)
                    .where(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID.eq(stripeEventId))
                    .and(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.eq(paymentId))
                    .and(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID.eq(stripePaymentIntentId)),
            )
            .single()
            .awaitSingle() == 1,
    ) {
      "Stripe event must belong to the payment being marked applied"
    }
  }
}
