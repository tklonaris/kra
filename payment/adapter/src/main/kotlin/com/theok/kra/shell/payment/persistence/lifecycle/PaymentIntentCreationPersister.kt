package com.theok.kra.shell.payment.persistence.lifecycle

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.PaymentStatusChange
import com.theok.kra.shell.payment.crypto.EncryptedClientSecret
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.persistence.coordination.toPaymentWorkflowState
import com.theok.kra.shell.payment.persistence.request.PaymentRequestLifecycleEventType
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Transaction-scoped persistence for claiming and recording provider intent creation. */
internal class PaymentIntentCreationPersister(private val dsl: DSLContext) {
  private val databaseNow = DSL.currentOffsetDateTime().`as`("database_now")
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  suspend fun loadForIntentCreationForUpdate(paymentId: UUID): PaymentWorkflowState? =
      Flux.from(
              dsl.select(PAYMENT.asterisk(), PAYMENT_REQUEST.asterisk(), databaseNow)
                  .from(PAYMENT)
                  .join(PAYMENT_REQUEST)
                  .on(PAYMENT_REQUEST.ID.eq(PAYMENT.PAYMENT_REQUEST_ID))
                  .where(PAYMENT.ID.eq(paymentId))
                  .forUpdate()
                  .of(PAYMENT_REQUEST),
          )
          .next()
          .awaitSingleOrNull()
          ?.toPaymentWorkflowState(databaseNow)

  suspend fun claim(
      paymentId: UUID,
      operationId: UUID,
      idempotencyKey: String,
      fingerprint: String,
  ): Boolean {
    // Keep the request-first lock order inside the claim operation. Callers may inspect the
    // state beforehand, but that inspection must never be required for cancellation safety.
    requestStatePersister.lockRequestByPaymentId(paymentId)
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.INTENT_CREATION_STATE, IntentCreationState.ATTEMPTING.name)
                    .set(PAYMENT.CREATION_OPERATION_ID, operationId)
                    .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, idempotencyKey)
                    .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, fingerprint)
                    // This timestamp fences the lifetime of the stable Stripe idempotency key.
                    // Never renew it on a retry: Stripe may have forgotten the key after its
                    // original window even when a worker was lost before recording a response.
                    .set(
                        PAYMENT.CREATION_ATTEMPTED_AT,
                        DSL.coalesce(PAYMENT.CREATION_ATTEMPTED_AT, DSL.currentOffsetDateTime()),
                    )
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(PaymentStatus.REQUESTED.name))
                    .and(
                        DSL.exists(
                            dsl.selectOne()
                                .from(PAYMENT_REQUEST)
                                .where(PAYMENT_REQUEST.ID.eq(PAYMENT.PAYMENT_REQUEST_ID))
                                .and(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT.isNull)
                                .and(
                                    PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT.gt(
                                        DSL.currentOffsetDateTime(),
                                    ),
                                ),
                        ),
                    )
                    .and(PAYMENT.PROVIDER_PAYMENT_ID.isNull),
            )
            .single()
            .awaitSingle()
    if (updated == 1)
        dsl.insertPaymentLifecycleEvent(
            paymentId,
            PaymentLifecycleEventType.INTENT_CREATION_CLAIMED,
        )
    return updated == 1
  }

  suspend fun persistCreatedIntent(
      paymentId: UUID,
      operationId: UUID,
      intentId: String,
      envelope: EncryptedClientSecret,
      statusChange: PaymentStatusChange,
  ): Boolean {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.PROVIDER_PAYMENT_ID, intentId)
                    .set(PAYMENT.INTENT_CREATION_STATE, IntentCreationState.KNOWN.name)
                    .set(PAYMENT.STATUS, statusChange.nextStatus.name)
                    .set(PAYMENT.CLIENT_SECRET_NONCE, envelope.nonce)
                    .set(PAYMENT.CLIENT_SECRET_CIPHERTEXT, envelope.ciphertext)
                    .set(PAYMENT.CLIENT_SECRET_TAG, envelope.tag)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(statusChange.expectedStatus.name))
                    .and(PAYMENT.INTENT_CREATION_STATE.eq(IntentCreationState.ATTEMPTING.name))
                    .and(PAYMENT.CREATION_OPERATION_ID.eq(operationId))
                    .and(PAYMENT.PROVIDER_PAYMENT_ID.isNull),
            )
            .single()
            .awaitSingle()
    if (updated == 1) {
      if (request.status == PaymentRequestStatus.CREATING_PROVIDER_INTENT) {
        requestStatePersister.transition(
            request,
            PaymentRequestStatus.WAITING_FOR_CUSTOMER,
            PaymentRequestLifecycleEventType.INTENT_CREATED,
        )
      }
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.INTENT_CREATED,
          fromStatus = statusChange.expectedStatus,
          toStatus = statusChange.nextStatus,
          reconciliationRequired = false,
      )
    }
    return updated == 1
  }

  /** Attaches an intent proved by a verified webhook after the create retry window has elapsed. */
  suspend fun recoverCreatedIntent(
      paymentId: UUID,
      intentId: String,
      envelope: EncryptedClientSecret,
      statusChange: PaymentStatusChange,
  ): Boolean {
    val request = requestStatePersister.lockRequestByPaymentId(paymentId)
    if (!request.status.allowsProviderIntentRecovery()) return false
    val updated =
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.PROVIDER_PAYMENT_ID, intentId)
                    .set(PAYMENT.INTENT_CREATION_STATE, IntentCreationState.KNOWN.name)
                    .set(PAYMENT.STATUS, statusChange.nextStatus.name)
                    .set(PAYMENT.CLIENT_SECRET_NONCE, envelope.nonce)
                    .set(PAYMENT.CLIENT_SECRET_CIPHERTEXT, envelope.ciphertext)
                    .set(PAYMENT.CLIENT_SECRET_TAG, envelope.tag)
                    .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                    .set(PAYMENT.RECONCILIATION_REASON, null as String?)
                    .where(PAYMENT.ID.eq(paymentId))
                    .and(PAYMENT.STATUS.eq(statusChange.expectedStatus.name))
                    .and(PAYMENT.PROVIDER_PAYMENT_ID.isNull)
                    .and(PAYMENT.INTENT_CREATION_STATE.eq(IntentCreationState.UNCERTAIN.name))
                    .and(PAYMENT.RECONCILIATION_REQUIRED),
            )
            .single()
            .awaitSingle()
    if (updated == 1) {
      if (request.status != PaymentRequestStatus.RECONCILING_CANCELLATION) {
        requestStatePersister.transition(
            request,
            PaymentRequestStatus.WAITING_FOR_CUSTOMER,
            PaymentRequestLifecycleEventType.INTENT_CREATED,
            "recovered from verified provider event",
        )
      }
      dsl.insertPaymentLifecycleEvent(
          paymentId = paymentId,
          eventType = PaymentLifecycleEventType.INTENT_CREATED,
          fromStatus = statusChange.expectedStatus,
          toStatus = statusChange.nextStatus,
          reconciliationRequired = false,
          reason = "recovered from verified provider event",
      )
    }
    return updated == 1
  }
}
