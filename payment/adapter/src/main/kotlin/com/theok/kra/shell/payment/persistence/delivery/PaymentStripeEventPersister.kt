package com.theok.kra.shell.payment.persistence.delivery

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.platform.database.json.JooqJsonSupport
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_PROVIDER_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.time.OffsetDateTime
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Record
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

/** PostgreSQL-backed Stripe event, payment matching, and transactional worker hand-off. */
@Component
@Profile("payments")
class PaymentStripeEventPersister(
    private val jooq: TransactionalJooqExecutor,
    objectMapperProvider: ObjectProvider<ObjectMapper>,
) {
  private val objectMapper = objectMapperProvider.getIfAvailable() ?: JooqJsonSupport.objectMapper()

  suspend fun record(event: PaymentStripeEvent): WebhookAcceptance = jooq.execute { dsl ->
    if (insertStripeEvent(dsl, event)) associateAndEnqueue(dsl, event)
    else retryUnassociatedEvent(dsl, event)
  }

  /**
   * A signed Stripe redelivery may arrive after the payment's creation state becomes eligible.
   * Retrying the durable association preserves the original Stripe event without granting a broker
   * message authority to make the decision.
   */
  private suspend fun retryUnassociatedEvent(
      dsl: DSLContext,
      event: PaymentStripeEvent,
  ): WebhookAcceptance {
    val existing = loadStripeEvent(dsl, event.stripeEventId)
    check(existing.matches(event)) {
      "Stripe event ID was reused for different provider facts"
    }
    if (existing.paymentId != null) {
      // The previous broker delivery may have settled in the DLQ before applying this Stripe event.
      // Let the hand-off owner create a replacement only in that case; it remains a duplicate from
      // the HTTP caller's perspective and must not create work after the provider fact is applied.
      if (existing.appliedAt == null) {
        enqueueWebhookHandoff(
            dsl,
            event.stripeEventId,
            requireNotNull(existing.paymentRequestId),
            existing.paymentId,
        )
      }
      return WebhookAcceptance.Duplicate
    }
    return associateAndEnqueue(dsl, event)
  }

  private suspend fun associateAndEnqueue(
      dsl: DSLContext,
      event: PaymentStripeEvent,
  ): WebhookAcceptance {
    // A candidate metadata identifier is not trusted until the payment, creation state, intent,
    // amount and currency all match. Mismatches remain durable audit-only Stripe events.
    val payment = findMatchingPayment(dsl, event) ?: return WebhookAcceptance.AuditOnly
    if (!associateStripeEvent(dsl, event.stripeEventId, payment.id)) {
      return WebhookAcceptance.Duplicate
    }
    enqueueWebhookHandoff(dsl, event.stripeEventId, payment.requestId, payment.id)
    return WebhookAcceptance.Accepted
  }

  private suspend fun insertStripeEvent(dsl: DSLContext, event: PaymentStripeEvent): Boolean =
      Flux.from(
              dsl.insertInto(PAYMENT_PROVIDER_EVENT)
                  .set(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID, event.stripeEventId)
                  .set(PAYMENT_PROVIDER_EVENT.PAYMENT_ID, null as UUID?)
                  .set(
                      PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID,
                      event.stripePaymentIntentId,
                  )
                  .set(PAYMENT_PROVIDER_EVENT.EVENT_TYPE, event.eventType)
                  .set(PAYMENT_PROVIDER_EVENT.AMOUNT_MINOR, event.amountMinor)
                  .set(PAYMENT_PROVIDER_EVENT.CURRENCY, event.currency.uppercase())
                  .set(PAYMENT_PROVIDER_EVENT.CANDIDATE_PAYMENT_ID, event.candidatePaymentId)
                  .onConflict(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID)
                  .doNothing(),
          )
          .single()
          .awaitSingle() > 0

  private suspend fun associateStripeEvent(
      dsl: DSLContext,
      stripeEventId: String,
      paymentId: UUID,
  ): Boolean =
      Flux.from(
              dsl.update(PAYMENT_PROVIDER_EVENT)
                  .set(PAYMENT_PROVIDER_EVENT.PAYMENT_ID, paymentId)
                  .where(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID.eq(stripeEventId))
                  .and(PAYMENT_PROVIDER_EVENT.PAYMENT_ID.isNull),
          )
          .single()
          .awaitSingle() == 1

  private suspend fun loadStripeEvent(
      dsl: DSLContext,
      stripeEventId: String,
  ): RecordedStripeEvent =
      requireNotNull(
              Flux.from(
                      dsl.select(
                              PAYMENT_PROVIDER_EVENT.PAYMENT_ID,
                              PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID,
                              PAYMENT_PROVIDER_EVENT.EVENT_TYPE,
                              PAYMENT_PROVIDER_EVENT.AMOUNT_MINOR,
                              PAYMENT_PROVIDER_EVENT.CURRENCY,
                              PAYMENT_PROVIDER_EVENT.CANDIDATE_PAYMENT_ID,
                              PAYMENT_PROVIDER_EVENT.APPLIED_AT,
                              PAYMENT.PAYMENT_REQUEST_ID,
                          )
                          .from(PAYMENT_PROVIDER_EVENT)
                          .leftJoin(PAYMENT)
                          .on(PAYMENT.ID.eq(PAYMENT_PROVIDER_EVENT.PAYMENT_ID))
                          .where(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID.eq(stripeEventId)),
                  )
                  .next()
                  .awaitSingleOrNull(),
          ) {
            "Duplicate Stripe event must retain its durable record"
          }
          .toRecordedStripeEvent()

  private suspend fun enqueueWebhookHandoff(
      dsl: DSLContext,
      stripeEventId: String,
      paymentRequestId: UUID,
      paymentId: UUID,
  ) =
      PaymentStripeWebhookHandoffPersister(dsl, objectMapper)
          .append(
              paymentRequestId,
              paymentId,
              stripeEventId,
          )

  private suspend fun findMatchingPayment(
      dsl: DSLContext,
      event: PaymentStripeEvent,
  ): PaymentAssociation? = event.candidatePaymentId?.let { loadMatchingPayment(dsl, it, event) }

  private suspend fun loadMatchingPayment(
      dsl: DSLContext,
      id: UUID,
      event: PaymentStripeEvent,
  ): PaymentAssociation? {
    val lockedRequest =
        PaymentRequestStatePersister(dsl).findAndLockRequestByPaymentId(id) ?: return null
    val row =
        Flux.from(
                dsl.select(
                        PAYMENT.ID,
                        PAYMENT.PAYMENT_REQUEST_ID,
                        PAYMENT_REQUEST.AMOUNT_MINOR,
                        PAYMENT_REQUEST.CURRENCY,
                        PAYMENT.PROVIDER_PAYMENT_ID,
                        PAYMENT.INTENT_CREATION_STATE,
                    )
                    .from(PAYMENT)
                    .join(PAYMENT_REQUEST)
                    .on(PAYMENT_REQUEST.ID.eq(PAYMENT.PAYMENT_REQUEST_ID))
                    .where(PAYMENT.ID.eq(id))
                    .and(PAYMENT.PAYMENT_REQUEST_ID.eq(lockedRequest.id))
                    // The request lock is acquired above; now lock the payment before reading the
                    // creation state used to associate this Stripe event and append its hand-off.
                    .forUpdate()
                    .of(PAYMENT),
            )
            .next()
            .awaitSingleOrNull() ?: return null
    val recordedIntentId = row.get(PAYMENT.PROVIDER_PAYMENT_ID)
    val creationState =
        IntentCreationState.valueOf(requireNotNull(row.get(PAYMENT.INTENT_CREATION_STATE)))
    val intentMatches =
        recordedIntentId == event.stripePaymentIntentId ||
            (recordedIntentId == null && creationState.mayHaveUnrecordedProviderIntent)
    return if (
        row.get(PAYMENT_REQUEST.AMOUNT_MINOR) == event.amountMinor &&
            requireNotNull(row.get(PAYMENT_REQUEST.CURRENCY))
                .trim()
                .equals(event.currency, ignoreCase = true) &&
            intentMatches
    ) {
      PaymentAssociation(
          requireNotNull(row.get(PAYMENT.ID)),
          requireNotNull(row.get(PAYMENT.PAYMENT_REQUEST_ID)),
      )
    } else null
  }

  private data class PaymentAssociation(val id: UUID, val requestId: UUID)

  private fun Record.toRecordedStripeEvent() =
      RecordedStripeEvent(
          paymentId = get(PAYMENT_PROVIDER_EVENT.PAYMENT_ID),
          paymentRequestId = get(PAYMENT.PAYMENT_REQUEST_ID),
          stripePaymentIntentId = requireNotNull(get(PAYMENT_PROVIDER_EVENT.PROVIDER_PAYMENT_ID)),
          eventType = requireNotNull(get(PAYMENT_PROVIDER_EVENT.EVENT_TYPE)),
          amountMinor = requireNotNull(get(PAYMENT_PROVIDER_EVENT.AMOUNT_MINOR)),
          currency = requireNotNull(get(PAYMENT_PROVIDER_EVENT.CURRENCY)).trim(),
          candidatePaymentId = get(PAYMENT_PROVIDER_EVENT.CANDIDATE_PAYMENT_ID),
          appliedAt = get(PAYMENT_PROVIDER_EVENT.APPLIED_AT),
      )

  private data class RecordedStripeEvent(
      val paymentId: UUID?,
      val paymentRequestId: UUID?,
      val stripePaymentIntentId: String,
      val eventType: String,
      val amountMinor: Long,
      val currency: String,
      val candidatePaymentId: UUID?,
      val appliedAt: OffsetDateTime?,
  ) {
    fun matches(event: PaymentStripeEvent): Boolean =
        stripePaymentIntentId == event.stripePaymentIntentId &&
            eventType == event.eventType &&
            amountMinor == event.amountMinor &&
            currency.equals(event.currency, ignoreCase = true) &&
            candidatePaymentId == event.candidatePaymentId
  }
}
