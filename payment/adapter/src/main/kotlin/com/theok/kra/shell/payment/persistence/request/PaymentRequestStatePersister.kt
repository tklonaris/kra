package com.theok.kra.shell.payment.persistence.request

import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Request-first locking and guarded request lifecycle transitions in one transaction. */
internal class PaymentRequestStatePersister(private val dsl: DSLContext) {
  suspend fun findAndLockRequestByPaymentId(paymentId: UUID): LockedPaymentRequest? =
      Flux.from(
              dsl.select(
                      PAYMENT_REQUEST.ID,
                      PAYMENT_REQUEST.STATUS,
                      PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                  )
                  .from(PAYMENT_REQUEST)
                  .join(PAYMENT)
                  .on(PAYMENT.PAYMENT_REQUEST_ID.eq(PAYMENT_REQUEST.ID))
                  .where(PAYMENT.ID.eq(paymentId))
                  .forUpdate()
                  .of(PAYMENT_REQUEST),
          )
          .next()
          .awaitSingleOrNull()
          ?.toLockedPaymentRequest()

  suspend fun lockRequestByPaymentId(paymentId: UUID): LockedPaymentRequest =
      requireNotNull(findAndLockRequestByPaymentId(paymentId)) {
        "Payment $paymentId has no payment request"
      }

  suspend fun lockRequestById(paymentRequestId: UUID): LockedPaymentRequest =
      requireNotNull(
              Flux.from(
                      dsl.select(
                              PAYMENT_REQUEST.ID,
                              PAYMENT_REQUEST.STATUS,
                              PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                          )
                          .from(PAYMENT_REQUEST)
                          .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                          .forUpdate(),
                  )
                  .next()
                  .awaitSingleOrNull(),
          ) {
            "Payment request $paymentRequestId does not exist"
          }
          .toLockedPaymentRequest()

  /**
   * Records expiry as the durable request-level cancellation claim before provider reconciliation.
   */
  suspend fun recordCheckoutExpiry(paymentId: UUID): Boolean {
    val request = lockRequestByPaymentId(paymentId)
    val paymentStatus = lockPaymentStatus(paymentId)
    if (
        !request.status.acceptsCancellationRequest() ||
            request.cancellationOrigin != null ||
            request.status == PaymentRequestStatus.RECONCILING_CANCELLATION ||
            !paymentStatus.checkoutExpiryApplies
    ) {
      return false
    }
    val recorded =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT,
                        DSL.currentOffsetDateTime(),
                    )
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                        TerminalReason.CHECKOUT_EXPIRED.name,
                    )
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_REASON,
                        TerminalReason.CHECKOUT_EXPIRED.name,
                    )
                    .set(
                        PAYMENT_REQUEST.STATUS,
                        PaymentRequestStatus.RECONCILING_CANCELLATION.name,
                    )
                    .where(PAYMENT_REQUEST.ID.eq(request.id))
                    .and(PAYMENT_REQUEST.STATUS.eq(request.status.name))
                    .and(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT.isNull)
                    .and(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT.le(DSL.currentOffsetDateTime())),
            )
            .single()
            .awaitSingle() == 1
    if (recorded) {
      dsl.appendPaymentRequestLifecycleEvent(
          request.id,
          PaymentRequestLifecycleEventType.CHECKOUT_EXPIRED,
          request.status.name,
          PaymentRequestStatus.RECONCILING_CANCELLATION.name,
          TerminalReason.CHECKOUT_EXPIRED.name,
      )
    }
    return recorded
  }

  suspend fun transition(
      request: LockedPaymentRequest,
      nextStatus: PaymentRequestStatus,
      eventType: PaymentRequestLifecycleEventType,
      reason: String? = null,
  ) {
    if (request.status == nextStatus) return
    request.status.requireTransitionTo(nextStatus)
    val transitioned =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(PAYMENT_REQUEST.STATUS, nextStatus.name)
                    .set(
                        PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON,
                        if (request.status == PaymentRequestStatus.FAILED)
                            DSL.inline(null as String?)
                        else PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON,
                    )
                    .where(PAYMENT_REQUEST.ID.eq(request.id))
                    .and(PAYMENT_REQUEST.STATUS.eq(request.status.name)),
            )
            .single()
            .awaitSingle() == 1
    check(transitioned) { "Payment request transition was not applied" }
    dsl.appendPaymentRequestLifecycleEvent(
        request.id,
        eventType,
        request.status.name,
        nextStatus.name,
        reason,
    )
  }

  suspend fun markOperationalFailure(request: LockedPaymentRequest, reason: String): Boolean {
    if (request.status == PaymentRequestStatus.FAILED) return false
    request.status.requireTransitionTo(PaymentRequestStatus.FAILED)
    val transitioned =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(PAYMENT_REQUEST.STATUS, PaymentRequestStatus.FAILED.name)
                    .set(PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON, reason.take(512))
                    .where(PAYMENT_REQUEST.ID.eq(request.id))
                    .and(PAYMENT_REQUEST.STATUS.eq(request.status.name)),
            )
            .single()
            .awaitSingle() == 1
    check(transitioned) { "Payment request operational failure transition was not applied" }
    dsl.appendPaymentRequestLifecycleEvent(
        request.id,
        PaymentRequestLifecycleEventType.OPERATIONAL_FAILURE,
        request.status.name,
        PaymentRequestStatus.FAILED.name,
        reason,
    )
    return true
  }

  suspend fun markRequestOnlyOperationalFailure(paymentRequestId: UUID, reason: String): Boolean {
    val request = lockRequestById(paymentRequestId)
    return if (
        request.status in setOf(PaymentRequestStatus.CANCELLED, PaymentRequestStatus.COMPLETED)
    ) {
      false
    } else {
      markOperationalFailure(request, reason)
    }
  }

  private suspend fun lockPaymentStatus(paymentId: UUID): PaymentStatus =
      requireNotNull(
              Flux.from(
                      dsl.select(PAYMENT.STATUS)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(paymentId))
                          .forUpdate()
                          .of(PAYMENT),
                  )
                  .next()
                  .awaitSingleOrNull(),
          ) {
            "Payment $paymentId does not exist"
          }
          .get(PAYMENT.STATUS)
          .let { PaymentStatus.valueOf(requireNotNull(it)) }

  private fun Record.toLockedPaymentRequest() =
      LockedPaymentRequest(
          requireNotNull(get(PAYMENT_REQUEST.ID)),
          PaymentRequestStatus.valueOf(requireNotNull(get(PAYMENT_REQUEST.STATUS))),
          get(PAYMENT_REQUEST.CANCELLATION_ORIGIN)?.let(TerminalReason::valueOf),
      )
}

internal data class LockedPaymentRequest(
    val id: UUID,
    val status: PaymentRequestStatus,
    val cancellationOrigin: TerminalReason?,
)
