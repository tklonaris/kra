package com.theok.kra.shell.payment.persistence.request

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.delivery.appendPaymentOutbox
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

/** Request-owned cancellation entry point; payment provider reconciliation remains saga work. */
@Component
@Profile("payments")
class PaymentRequestCancellationPersister(
    private val jooq: TransactionalJooqExecutor,
    private val objectMapper: ObjectMapper,
) {
  suspend fun requestCancellation(
      ownerId: PaymentOwnerId,
      paymentRequestId: UUID,
  ): PaymentRequestCancellationStatus = jooq.execute { dsl ->
    val cancelledBeforeRegistration = cancelBeforeRegistration(dsl, ownerId, paymentRequestId)
    if (cancelledBeforeRegistration != null) {
      dsl.appendPaymentRequestLifecycleEvent(
          paymentRequestId,
          PaymentRequestLifecycleEventType.CANCELLED,
          cancelledBeforeRegistration.name,
          PaymentRequestStatus.CANCELLED.name,
          TerminalReason.CUSTOMER_CANCELLED.name,
      )
      dsl.appendPaymentOutbox(
          paymentRequestId,
          null,
          PaymentEventType.CANCELLATION_REQUESTED,
          objectMapper,
      )
      PaymentRequestCancellationStatus.REQUESTED
    } else {
      val cancellation = recordCancellationAfterRegistration(dsl, ownerId, paymentRequestId)
      if (cancellation != null) {
        dsl.appendPaymentRequestLifecycleEvent(
            paymentRequestId,
            PaymentRequestLifecycleEventType.CANCELLATION_REQUESTED,
            cancellation.fromStatus.name,
            PaymentRequestStatus.RECONCILING_CANCELLATION.name,
            TerminalReason.CUSTOMER_CANCELLED.name,
        )
        dsl.appendPaymentOutbox(
            paymentRequestId,
            cancellation.paymentId,
            PaymentEventType.CANCELLATION_REQUESTED,
            objectMapper,
        )
        PaymentRequestCancellationStatus.REQUESTED
      } else {
        cancellationOutcome(dsl, ownerId, paymentRequestId)
      }
    }
  }

  private suspend fun cancelBeforeRegistration(
      dsl: DSLContext,
      ownerId: PaymentOwnerId,
      paymentRequestId: UUID,
  ): PaymentRequestStatus? {
    val requestStatus =
        Flux.from(
                dsl.select(PAYMENT_REQUEST.STATUS)
                    .from(PAYMENT_REQUEST)
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value))
                    .forUpdate(),
            )
            .next()
            .awaitSingleOrNull()
            ?.get(PAYMENT_REQUEST.STATUS)
            ?.let(PaymentRequestStatus::valueOf) ?: return null
    if (!requestStatus.allowsPaymentRegistration()) return null
    val cancelled =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(PAYMENT_REQUEST.STATUS, PaymentRequestStatus.CANCELLED.name)
                    .set(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT, DSL.currentOffsetDateTime())
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                        TerminalReason.CUSTOMER_CANCELLED.name,
                    )
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_REASON,
                        TerminalReason.CUSTOMER_CANCELLED.name,
                    )
                    .set(PAYMENT_REQUEST.OPERATIONAL_FAILURE_REASON, null as String?)
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value))
                    .and(PAYMENT_REQUEST.STATUS.eq(requestStatus.name))
                    .and(
                        DSL.notExists(
                            dsl.selectOne()
                                .from(PAYMENT)
                                .where(PAYMENT.PAYMENT_REQUEST_ID.eq(PAYMENT_REQUEST.ID)),
                        ),
                    ),
            )
            .single()
            .awaitSingle() == 1
    return requestStatus.takeIf { cancelled }
  }

  private suspend fun recordCancellationAfterRegistration(
      dsl: DSLContext,
      ownerId: PaymentOwnerId,
      paymentRequestId: UUID,
  ): RegisteredPaymentCancellation? {
    val request =
        Flux.from(
                dsl.select(PAYMENT_REQUEST.STATUS)
                    .from(PAYMENT_REQUEST)
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value))
                    .forUpdate(),
            )
            .next()
            .awaitSingleOrNull() ?: return null
    val requestStatus =
        PaymentRequestStatus.valueOf(requireNotNull(request.get(PAYMENT_REQUEST.STATUS)))
    if (!requestStatus.acceptsCancellationRequest()) return null
    val recorded =
        Flux.from(
                dsl.update(PAYMENT_REQUEST)
                    .set(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT, DSL.currentOffsetDateTime())
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_ORIGIN,
                        TerminalReason.CUSTOMER_CANCELLED.name,
                    )
                    .set(
                        PAYMENT_REQUEST.CANCELLATION_REASON,
                        TerminalReason.CUSTOMER_CANCELLED.name,
                    )
                    .set(
                        PAYMENT_REQUEST.STATUS,
                        PaymentRequestStatus.RECONCILING_CANCELLATION.name,
                    )
                    .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value))
                    .and(PAYMENT_REQUEST.STATUS.eq(requestStatus.name))
                    .and(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT.isNull)
                    .and(
                        DSL.exists(
                            dsl.selectOne()
                                .from(PAYMENT)
                                .where(PAYMENT.PAYMENT_REQUEST_ID.eq(PAYMENT_REQUEST.ID))
                                .and(
                                    PAYMENT.STATUS.notIn(
                                        *PaymentStatus.terminalStatuses
                                            .map(PaymentStatus::name)
                                            .toTypedArray(),
                                    ),
                                ),
                        ),
                    ),
            )
            .single()
            .awaitSingle() == 1
    return if (recorded) {
      RegisteredPaymentCancellation(
          requireNotNull(
              Flux.from(
                      dsl.select(PAYMENT.ID)
                          .from(PAYMENT)
                          .where(PAYMENT.PAYMENT_REQUEST_ID.eq(paymentRequestId))
                          .forUpdate(),
                  )
                  .next()
                  .awaitSingleOrNull()
                  ?.get(PAYMENT.ID),
          ) {
            "A registered payment must remain associated with its payment request"
          },
          requestStatus,
      )
    } else null
  }

  private suspend fun cancellationOutcome(
      dsl: DSLContext,
      ownerId: PaymentOwnerId,
      paymentRequestId: UUID,
  ): PaymentRequestCancellationStatus =
      Flux.from(
              dsl.select(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT)
                  .from(PAYMENT_REQUEST)
                  .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                  .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value)),
          )
          .next()
          .awaitSingleOrNull()
          .let { cancellationRequestedAt ->
            if (cancellationRequestedAt == null) {
              PaymentRequestCancellationStatus.NOT_FOUND
            } else if (
                cancellationRequestedAt.get(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT) != null
            ) {
              PaymentRequestCancellationStatus.ALREADY_REQUESTED
            } else PaymentRequestCancellationStatus.TERMINAL
          }
}

private data class RegisteredPaymentCancellation(
    val paymentId: UUID,
    val fromStatus: PaymentRequestStatus,
)

enum class PaymentRequestCancellationStatus {
  REQUESTED,
  ALREADY_REQUESTED,
  TERMINAL,
  NOT_FOUND,
}
