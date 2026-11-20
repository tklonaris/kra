package com.theok.kra.shell.payment.persistence.delivery

import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_CONSUMER_EVENT
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Records
import org.jooq.impl.DSL
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux

enum class PaymentConsumerEventStatus {
  PENDING,
  ACCEPTED,
  SKIPPED,
  DLQ_REQUESTED,
}

data class PaymentConsumerEvent(
    val consumerEventId: UUID,
    val paymentRequestId: UUID,
    val paymentId: UUID?,
    val eventType: PaymentEventType,
    val stripeEventId: String? = null,
)

/** Persists delivery state for the feature-owned RabbitMQ consumer. */
@Component
@Profile("payments")
class PaymentConsumerEventPersister(private val jooq: TransactionalJooqExecutor) {
  @Transactional
  suspend fun receive(event: PaymentConsumerEvent): PaymentConsumerEventStatus =
      jooq.execute { dsl ->
        val inserted = insertEvent(dsl, event)
        val recorded = loadEvent(dsl, event.consumerEventId)
        if (!inserted)
            check(recorded.matches(event)) { "Consumer event ID was reused for different input" }
        val deliveryStatus = recorded.status
        if (!inserted && deliveryStatus == PaymentConsumerEventStatus.PENDING) {
          recordRedeliveryAttempt(dsl, event.consumerEventId)
        }
        deliveryStatus
      }

  @Transactional
  suspend fun accepted(consumerEventId: UUID) =
      updateStatus(consumerEventId, PaymentConsumerEventStatus.ACCEPTED)

  @Transactional
  suspend fun skipped(consumerEventId: UUID) =
      updateStatus(consumerEventId, PaymentConsumerEventStatus.SKIPPED)

  /** Loads the immutable broker fact that a Temporal wake-up refers to. */
  suspend fun load(consumerEventId: UUID): PaymentConsumerEvent? = jooq.execute { dsl ->
    loadEventOrNull(dsl, consumerEventId)?.toConsumerEvent(consumerEventId)
  }

  @Transactional
  suspend fun requestDlq(consumerEventId: UUID, failure: String?) = jooq.execute { dsl ->
    Flux.from(
            dsl.update(PAYMENT_CONSUMER_EVENT)
                .set(
                    PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS,
                    PaymentConsumerEventStatus.DLQ_REQUESTED.name,
                )
                .set(PAYMENT_CONSUMER_EVENT.LAST_FAILURE, failure?.take(1000))
                .set(PAYMENT_CONSUMER_EVENT.UPDATED_AT, DSL.currentOffsetDateTime())
                .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId))
                .and(
                    PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS.eq(
                        PaymentConsumerEventStatus.PENDING.name,
                    ),
                ),
        )
        .single()
        .awaitSingle()
  }

  private suspend fun insertEvent(dsl: DSLContext, event: PaymentConsumerEvent): Boolean =
      Flux.from(
              dsl.insertInto(PAYMENT_CONSUMER_EVENT)
                  .set(PAYMENT_CONSUMER_EVENT.EVENT_ID, event.consumerEventId)
                  .set(PAYMENT_CONSUMER_EVENT.PAYMENT_REQUEST_ID, event.paymentRequestId)
                  .set(PAYMENT_CONSUMER_EVENT.PAYMENT_ID, event.paymentId)
                  .set(PAYMENT_CONSUMER_EVENT.EVENT_TYPE, event.eventType.eventName)
                  .set(
                      PAYMENT_CONSUMER_EVENT.PROVIDER_EVENT_ID,
                      event.stripeEventId,
                  )
                  .set(
                      PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS,
                      PaymentConsumerEventStatus.PENDING.name,
                  )
                  .set(PAYMENT_CONSUMER_EVENT.ATTEMPTS, 1)
                  .onConflict(PAYMENT_CONSUMER_EVENT.EVENT_ID)
                  .doNothing(),
          )
          .single()
          .awaitSingle() > 0

  private suspend fun loadEvent(
      dsl: DSLContext,
      consumerEventId: UUID,
  ): RecordedConsumerEvent =
      loadEventOrNull(dsl, consumerEventId) ?: error("Payment consumer event was not recorded")

  private suspend fun loadEventOrNull(
      dsl: DSLContext,
      consumerEventId: UUID,
  ): RecordedConsumerEvent? =
      Flux.from(
              dsl.select(
                      PAYMENT_CONSUMER_EVENT.PAYMENT_REQUEST_ID,
                      PAYMENT_CONSUMER_EVENT.PAYMENT_ID,
                      PAYMENT_CONSUMER_EVENT.EVENT_TYPE.convertFrom(
                          PaymentEventType::fromEventName,
                      ),
                      PAYMENT_CONSUMER_EVENT.PROVIDER_EVENT_ID,
                      PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS.convertFrom(
                          PaymentConsumerEventStatus::valueOf,
                      ),
                  )
                  .from(PAYMENT_CONSUMER_EVENT)
                  .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId)),
          )
          .map(Records.mapping(::RecordedConsumerEvent))
          .next()
          .awaitSingleOrNull()

  private suspend fun recordRedeliveryAttempt(dsl: DSLContext, consumerEventId: UUID) {
    Flux.from(
            dsl.update(PAYMENT_CONSUMER_EVENT)
                .set(PAYMENT_CONSUMER_EVENT.ATTEMPTS, PAYMENT_CONSUMER_EVENT.ATTEMPTS.plus(1))
                .set(PAYMENT_CONSUMER_EVENT.UPDATED_AT, DSL.currentOffsetDateTime())
                .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId)),
        )
        .single()
        .awaitSingle()
  }

  private suspend fun updateStatus(consumerEventId: UUID, statusValue: PaymentConsumerEventStatus) =
      jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT_CONSUMER_EVENT)
                    .set(PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS, statusValue.name)
                    .set(PAYMENT_CONSUMER_EVENT.UPDATED_AT, DSL.currentOffsetDateTime())
                    .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(consumerEventId))
                    // A duplicate consumer may finish after another delivery has requested the
                    // DLQ. Only the pending owner may choose a final delivery outcome.
                    .and(
                        PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS.eq(
                            PaymentConsumerEventStatus.PENDING.name,
                        ),
                    ),
            )
            .single()
            .awaitSingle()
      }

  private data class RecordedConsumerEvent(
      val paymentRequestId: UUID,
      val paymentId: UUID?,
      val eventType: PaymentEventType,
      val stripeEventId: String?,
      val status: PaymentConsumerEventStatus,
  ) {
    fun toConsumerEvent(consumerEventId: UUID) =
        PaymentConsumerEvent(
            consumerEventId,
            paymentRequestId,
            paymentId,
            eventType,
            stripeEventId,
        )

    fun matches(event: PaymentConsumerEvent): Boolean =
        paymentRequestId == event.paymentRequestId &&
            paymentId == event.paymentId &&
            this.eventType == event.eventType &&
            stripeEventId == event.stripeEventId
  }
}
