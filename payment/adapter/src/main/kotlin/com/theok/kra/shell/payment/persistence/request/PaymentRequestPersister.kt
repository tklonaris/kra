package com.theok.kra.shell.payment.persistence.request

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentSpec
import com.theok.kra.core.payment.dto.PaymentRequestAcceptance
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.configuration.PaymentTemporalProperties
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.delivery.appendPaymentOutbox
import java.time.OffsetDateTime
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

/** Atomically accepts one commercial request and its saga wake-up. */
@Component
@Profile("payments")
class PaymentRequestPersister(
    private val jooq: TransactionalJooqExecutor,
    private val objectMapper: ObjectMapper,
    private val timing: PaymentTemporalProperties,
) {
  suspend fun accept(
      idempotencyKey: UUID,
      ownerId: PaymentOwnerId,
      spec: PaymentSpec,
  ): PaymentRequestAcceptance = jooq.execute { dsl ->
    val requestId = UUID.randomUUID()
    val acceptedAt = databaseNow(dsl)
    val checkoutDeadline =
        acceptedAt.plus(timing.checkoutWindow).withNano(acceptedAt.nano / 1_000 * 1_000)
    val processingDeadline =
        acceptedAt.plus(timing.processingWindow).withNano(acceptedAt.nano / 1_000 * 1_000)
    if (
        insertRequest(
            dsl,
            requestId,
            idempotencyKey,
            ownerId,
            spec,
            acceptedAt,
            checkoutDeadline,
            processingDeadline,
        )
    ) {
      dsl.appendPaymentRequestLifecycleEvent(
          requestId,
          PaymentRequestLifecycleEventType.ACCEPTED,
          null,
          PaymentRequestStatus.ACCEPTED.name,
      )
      dsl.appendPaymentOutbox(
          requestId,
          null,
          PaymentEventType.REQUEST_ACCEPTED,
          objectMapper,
      )
      acceptance(requestId, spec, checkoutDeadline)
    } else {
      val recorded =
          loadByIdempotencyKey(dsl, ownerId, idempotencyKey)
              ?: loadByPurchaseReference(dsl, ownerId, spec.purchaseReference)
              ?: error("Payment request uniqueness conflict without a recorded request")
      if (!recorded.matches(spec)) throw PaymentRequestPersistenceConflict()
      recorded
    }
  }

  private suspend fun insertRequest(
      dsl: DSLContext,
      requestId: UUID,
      idempotencyKey: UUID,
      ownerId: PaymentOwnerId,
      spec: PaymentSpec,
      acceptedAt: OffsetDateTime,
      checkoutDeadline: OffsetDateTime,
      processingDeadline: OffsetDateTime,
  ): Boolean =
      Flux.from(
              dsl.insertInto(PAYMENT_REQUEST)
                  .set(PAYMENT_REQUEST.ID, requestId)
                  .set(PAYMENT_REQUEST.OWNER_ID, ownerId.value)
                  .set(PAYMENT_REQUEST.IDEMPOTENCY_KEY, idempotencyKey)
                  .set(PAYMENT_REQUEST.PURCHASE_REFERENCE, spec.purchaseReference)
                  .set(PAYMENT_REQUEST.AMOUNT_MINOR, spec.amountMinor)
                  .set(PAYMENT_REQUEST.CURRENCY, spec.currency)
                  .set(PAYMENT_REQUEST.STATUS, PaymentRequestStatus.ACCEPTED.name)
                  .set(PAYMENT_REQUEST.CREATED_AT, acceptedAt)
                  .set(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT, checkoutDeadline)
                  .set(PAYMENT_REQUEST.PROCESSING_DEADLINE_AT, processingDeadline)
                  .onConflict()
                  .doNothing(),
          )
          .single()
          .awaitSingle() == 1

  private suspend fun loadByIdempotencyKey(
      dsl: DSLContext,
      ownerId: PaymentOwnerId,
      idempotencyKey: UUID,
  ): PaymentRequestAcceptance? =
      loadAcceptance(
          dsl,
          PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value)
              .and(PAYMENT_REQUEST.IDEMPOTENCY_KEY.eq(idempotencyKey)),
      )

  private suspend fun loadByPurchaseReference(
      dsl: DSLContext,
      ownerId: PaymentOwnerId,
      purchaseReference: UUID,
  ): PaymentRequestAcceptance? =
      loadAcceptance(
          dsl,
          PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value)
              .and(PAYMENT_REQUEST.PURCHASE_REFERENCE.eq(purchaseReference)),
      )

  private suspend fun loadAcceptance(
      dsl: DSLContext,
      condition: org.jooq.Condition,
  ): PaymentRequestAcceptance? =
      Flux.from(
              dsl.select(
                      PAYMENT_REQUEST.ID,
                      PAYMENT_REQUEST.PURCHASE_REFERENCE,
                      PAYMENT_REQUEST.AMOUNT_MINOR,
                      PAYMENT_REQUEST.CURRENCY,
                      PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT,
                  )
                  .from(PAYMENT_REQUEST)
                  .where(condition),
          )
          .next()
          .awaitSingleOrNull()
          ?.let(::acceptance)

  private fun acceptance(
      id: UUID,
      spec: PaymentSpec,
      checkoutDeadline: OffsetDateTime,
  ) =
      PaymentRequestAcceptance(
          paymentRequestId = id,
          purchaseReference = spec.purchaseReference,
          amountMinor = spec.amountMinor,
          currency = spec.currency,
          status = PaymentRequestStatus.ACCEPTED,
          checkoutDeadlineAt = checkoutDeadline.toInstant(),
      )

  private fun acceptance(record: Record) =
      PaymentRequestAcceptance(
          paymentRequestId = requireNotNull(record.get(PAYMENT_REQUEST.ID)),
          purchaseReference = requireNotNull(record.get(PAYMENT_REQUEST.PURCHASE_REFERENCE)),
          amountMinor = requireNotNull(record.get(PAYMENT_REQUEST.AMOUNT_MINOR)),
          currency = requireNotNull(record.get(PAYMENT_REQUEST.CURRENCY)).trim(),
          status = PaymentRequestStatus.ACCEPTED,
          checkoutDeadlineAt =
              requireNotNull(record.get(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT)).toInstant(),
      )

  private fun PaymentRequestAcceptance.matches(spec: PaymentSpec): Boolean =
      purchaseReference == spec.purchaseReference &&
          amountMinor == spec.amountMinor &&
          currency == spec.currency

  private suspend fun databaseNow(dsl: DSLContext): OffsetDateTime =
      requireNotNull(
          Flux.from(dsl.select(DSL.currentOffsetDateTime())).single().awaitSingle().value1(),
      )
}

internal class PaymentRequestPersistenceConflict : RuntimeException() {
  private companion object {
    const val serialVersionUID: Long = 1L
  }
}
