package com.theok.kra.shell.payment.persistence.lifecycle

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.request.PaymentRequestLifecycleEventType
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import reactor.core.publisher.Flux

/** Transaction-scoped durable reconciliation classification and marking. */
internal class PaymentReconciliationPersister(
    private val dsl: DSLContext,
) {
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  /** Marks reconciliation as required for an active payment outside intent-creation uncertainty. */
  suspend fun markRequired(paymentId: UUID, reason: String) {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.RECONCILIATION_REQUIRED, true)
                    .set(PAYMENT.RECONCILIATION_REASON, reason)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.RECONCILIATION_REQUIRED.isFalse)
                    .and(
                        PAYMENT.STATUS.notIn(
                            *PaymentStatus.terminalStatuses.map(PaymentStatus::name).toTypedArray(),
                        ),
                    ),
            )
            .single()
            .awaitSingle()
    if (updated == 1) {
      requestStatePersister.transition(
          request,
          PaymentRequestStatus.RECONCILING_PROVIDER_STATE,
          PaymentRequestLifecycleEventType.RECONCILIATION_REQUIRED,
          reason,
      )
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.RECONCILIATION_REQUIRED,
          reconciliationRequired = true,
          reason = reason,
      )
    }
  }

  /** Clears a resolved provider-state hold only after a verified, normal customer-waiting fact. */
  suspend fun clearVerifiedAwaitingCustomerReconciliation(paymentId: UUID): Boolean {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    if (request.status != PaymentRequestStatus.RECONCILING_PROVIDER_STATE) return false
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                    .set(PAYMENT.RECONCILIATION_REASON, null as String?)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(PaymentStatus.AWAITING_CUSTOMER.name))
                    .and(PAYMENT.INTENT_CREATION_STATE.eq(IntentCreationState.KNOWN.name))
                    .and(PAYMENT.PROVIDER_PAYMENT_ID.isNotNull)
                    .and(PAYMENT.RECONCILIATION_REQUIRED),
            )
            .single()
            .awaitSingle()
    if (updated == 1) {
      requestStatePersister.transition(
          request,
          PaymentRequestStatus.WAITING_FOR_CUSTOMER,
          PaymentRequestLifecycleEventType.RECONCILIATION_RESOLVED,
      )
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.RECONCILIATION_RESOLVED,
          fromStatus = PaymentStatus.AWAITING_CUSTOMER,
          toStatus = PaymentStatus.AWAITING_CUSTOMER,
          reconciliationRequired = false,
      )
    }
    return updated == 1
  }

  /** Returns whether this operation classified an in-flight, unrecorded intent as uncertain. */
  suspend fun markCreationUncertain(paymentId: UUID, reason: String): Boolean {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.INTENT_CREATION_STATE, IntentCreationState.UNCERTAIN.name)
                    .set(PAYMENT.RECONCILIATION_REQUIRED, true)
                    .set(PAYMENT.RECONCILIATION_REASON, reason)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(PaymentStatus.REQUESTED.name))
                    .and(PAYMENT.INTENT_CREATION_STATE.eq(IntentCreationState.ATTEMPTING.name))
                    .and(PAYMENT.PROVIDER_PAYMENT_ID.isNull),
            )
            .single()
            .awaitSingle()
    if (updated == 1) {
      requestStatePersister.transition(
          request,
          PaymentRequestStatus.RECONCILING_PROVIDER_STATE,
          PaymentRequestLifecycleEventType.INTENT_CREATION_UNCERTAIN,
          reason,
      )
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.INTENT_CREATION_UNCERTAIN,
          reconciliationRequired = true,
          reason = reason,
      )
    }
    return updated == 1
  }
}
