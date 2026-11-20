package com.theok.kra.shell.payment.persistence.request

import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import com.theok.kra.shell.payment.persistence.lifecycle.insertPaymentLifecycleEvent
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Registers the realised payment once the request-owned saga begins. */
internal class PaymentRequestRegistrationPersister(private val dsl: DSLContext) {
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  suspend fun register(paymentRequestId: UUID): UUID? {
    val request =
        Flux.from(
                dsl.select(PAYMENT_REQUEST.asterisk())
                    .from(PAYMENT_REQUEST)
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .forUpdate(),
            )
            .next()
            .awaitSingleOrNull() ?: error("Payment request $paymentRequestId does not exist")
    val requestStatus =
        PaymentRequestStatus.valueOf(requireNotNull(request.get(PAYMENT_REQUEST.STATUS)))
    val cancellationOrigin =
        request.get(PAYMENT_REQUEST.CANCELLATION_ORIGIN)?.let(TerminalReason::valueOf)
    val cancellationRequested = request.get(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT) != null
    if (requestStatus == PaymentRequestStatus.CANCELLED) return null
    val existing =
        Flux.from(
                dsl.select(PAYMENT.ID, PAYMENT.STATUS, PAYMENT.RECONCILIATION_REQUIRED)
                    .from(PAYMENT)
                    .where(PAYMENT.PAYMENT_REQUEST_ID.eq(paymentRequestId))
                    .forUpdate()
                    .of(PAYMENT),
            )
            .next()
            .awaitSingleOrNull()
            ?.toExistingPayment()
    if (existing != null) {
      reenterFailedRequest(
          paymentRequestId,
          requestStatus,
          cancellationOrigin,
          cancellationRequested,
          existing,
      )
      return existing.id
    }
    if (cancelIfCheckoutExpiredBeforeRegistration(paymentRequestId, requestStatus)) {
      dsl.appendPaymentRequestLifecycleEvent(
          paymentRequestId,
          PaymentRequestLifecycleEventType.CHECKOUT_EXPIRED,
          requestStatus.name,
          PaymentRequestStatus.CANCELLED.name,
          TerminalReason.CHECKOUT_EXPIRED.name,
      )
      return null
    }
    if (!requestStatus.allowsPaymentRegistration()) return null
    requestStatus.requireTransitionTo(PaymentRequestStatus.CREATING_PROVIDER_INTENT)

    val paymentId = UUID.randomUUID()
    Flux.from(
            dsl.insertInto(PAYMENT)
                .set(PAYMENT.ID, paymentId)
                .set(PAYMENT.PAYMENT_REQUEST_ID, paymentRequestId)
                .set(PAYMENT.STATUS, "REQUESTED")
                .set(PAYMENT.INTENT_CREATION_STATE, "NOT_STARTED"),
        )
        .single()
        .awaitSingle()
    dsl.insertPaymentLifecycleEvent(paymentId, PaymentLifecycleEventType.PAYMENT_CREATED)
    val transitioned =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(
                        PAYMENT_REQUEST.STATUS,
                        PaymentRequestStatus.CREATING_PROVIDER_INTENT.name,
                    )
                    .set(PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON, null as String?)
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST.STATUS.eq(requestStatus.name)),
            )
            .single()
            .awaitSingle() == 1
    check(transitioned) { "Payment request registration transition was not applied" }
    dsl.appendPaymentRequestLifecycleEvent(
        paymentRequestId,
        PaymentRequestLifecycleEventType.REGISTERED,
        requestStatus.name,
        PaymentRequestStatus.CREATING_PROVIDER_INTENT.name,
    )
    return paymentId
  }

  private suspend fun cancelIfCheckoutExpiredBeforeRegistration(
      paymentRequestId: UUID,
      requestStatus: PaymentRequestStatus,
  ): Boolean {
    if (!requestStatus.allowsPaymentRegistration()) return false
    return Flux.from(
            dsl.update(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.STATUS, PaymentRequestStatus.CANCELLED.name)
                .set(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT, DSL.currentOffsetDateTime())
                .set(
                    PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                    TerminalReason.CHECKOUT_EXPIRED.name,
                )
                .set(
                    PAYMENT_REQUEST.CANCELLATION_REASON,
                    TerminalReason.CHECKOUT_EXPIRED.name,
                )
                .set(PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON, null as String?)
                .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                .and(PAYMENT_REQUEST.STATUS.eq(requestStatus.name))
                .and(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT.le(DSL.currentOffsetDateTime())),
        )
        .single()
        .awaitSingle() == 1
  }

  private suspend fun reenterFailedRequest(
      paymentRequestId: UUID,
      requestStatus: PaymentRequestStatus,
      cancellationOrigin: TerminalReason?,
      cancellationRequested: Boolean,
      payment: ExistingPayment,
  ) {
    if (requestStatus != PaymentRequestStatus.FAILED) return
    requestStatePersister.transition(
        LockedPaymentRequest(paymentRequestId, requestStatus, cancellationOrigin),
        requestStatus.reenterForPayment(
            payment.status,
            payment.reconciliationRequired,
            cancellationRequested,
        ),
        PaymentRequestLifecycleEventType.REENTERED,
    )
  }

  private fun Record.toExistingPayment() =
      ExistingPayment(
          requireNotNull(get(PAYMENT.ID)),
          PaymentStatus.valueOf(requireNotNull(get(PAYMENT.STATUS))),
          requireNotNull(get(PAYMENT.RECONCILIATION_REQUIRED)),
      )

  private data class ExistingPayment(
      val id: UUID,
      val status: PaymentStatus,
      val reconciliationRequired: Boolean,
  )
}
