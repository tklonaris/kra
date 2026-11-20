package com.theok.kra.shell.payment.persistence.coordination

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.payment.domain.PaymentStatusChange
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.crypto.EncryptedClientSecret
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.delivery.PaymentStripeEventApplicationPersister
import com.theok.kra.shell.payment.persistence.delivery.PaymentStripeEventAssociationLoader
import com.theok.kra.shell.payment.persistence.delivery.PaymentStripeWebhookHandoffPersister
import com.theok.kra.shell.payment.persistence.delivery.UnappliedStripePaymentIntent
import com.theok.kra.shell.payment.persistence.delivery.appendPaymentOutbox
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentCancellationPersister
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentIntentCreationPersister
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentReconciliationPersister
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentStatusTransitionPersister
import com.theok.kra.shell.payment.persistence.request.PaymentRequestRegistrationPersister
import com.theok.kra.shell.payment.persistence.request.PaymentRequestStatePersister
import java.util.UUID
import org.jooq.DSLContext
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

/** Opens short, transaction-bound workflow persistence sessions. */
@Component
@Profile("payments")
class PaymentWorkflowPersister(
    private val jooq: TransactionalJooqExecutor,
    private val objectMapper: ObjectMapper,
) {
  @Transactional
  suspend fun <T> transaction(block: suspend PaymentWorkflowPersistence.() -> T): T =
      jooq.execute { dsl ->
        PaymentWorkflowPersistence(dsl, objectMapper).block()
      }
}

/**
 * Transaction-bound workflow operations composed from narrowly scoped persistence collaborators.
 */
class PaymentWorkflowPersistence
internal constructor(
    private val dsl: DSLContext,
    private val objectMapper: ObjectMapper,
) {
  private val loader = PaymentWorkflowLoader(dsl)
  private val intentCreation = PaymentIntentCreationPersister(dsl)
  private val reconciliation = PaymentReconciliationPersister(dsl)
  private val statusTransitions = PaymentStatusTransitionPersister(dsl, loader)
  private val cancellationPersister = PaymentCancellationPersister(dsl, loader)
  private val stripeEventAssociationLoader = PaymentStripeEventAssociationLoader(dsl)
  private val stripeEventApplicationPersister = PaymentStripeEventApplicationPersister(dsl)
  private val stripeWebhookHandoffs = PaymentStripeWebhookHandoffPersister(dsl, objectMapper)
  private val registration = PaymentRequestRegistrationPersister(dsl)
  private val requestStatePersister = PaymentRequestStatePersister(dsl)

  suspend fun load(paymentId: UUID): PaymentWorkflowState? = loader.load(paymentId)

  suspend fun loadByRequestId(paymentRequestId: UUID): PaymentWorkflowState? =
      loader.loadByRequestId(paymentRequestId)

  /** Serializes the payment-presence decision with payment registration's request-first lock. */
  suspend fun loadByRequestIdAfterLockingRequest(paymentRequestId: UUID): PaymentWorkflowState? {
    requestStatePersister.lockRequestById(paymentRequestId)
    return loader.loadByRequestId(paymentRequestId)
  }

  suspend fun registerPayment(paymentRequestId: UUID): UUID? =
      registration.register(paymentRequestId)

  suspend fun loadForIntentCreationForUpdate(paymentId: UUID): PaymentWorkflowState? =
      intentCreation.loadForIntentCreationForUpdate(paymentId)

  suspend fun markCreationUncertain(paymentId: UUID, reason: String): Boolean {
    val marked = reconciliation.markCreationUncertain(paymentId, reason)
    if (marked) reenqueueUnappliedStripeEvents(paymentId)
    return marked
  }

  suspend fun claimIntentCreation(
      paymentId: UUID,
      operationId: UUID,
      idempotencyKey: String,
      fingerprint: String,
  ): Boolean = intentCreation.claim(paymentId, operationId, idempotencyKey, fingerprint)

  suspend fun persistCreatedIntent(
      paymentId: UUID,
      operationId: UUID,
      intentId: String,
      envelope: EncryptedClientSecret,
      statusChange: PaymentStatusChange,
  ): Boolean {
    val persisted =
        intentCreation.persistCreatedIntent(
            paymentId,
            operationId,
            intentId,
            envelope,
            statusChange,
        )
    if (persisted) reenqueueUnappliedStripeEvents(paymentId)
    return persisted
  }

  suspend fun recoverCreatedIntent(
      paymentId: UUID,
      intentId: String,
      envelope: EncryptedClientSecret,
      statusChange: PaymentStatusChange,
  ): Boolean = intentCreation.recoverCreatedIntent(paymentId, intentId, envelope, statusChange)

  suspend fun applyPaymentStatusChange(
      paymentId: UUID,
      statusChange: PaymentStatusChange,
      terminalReason: TerminalReason? = null,
  ): PaymentStatusTransition =
      statusTransitions.update(paymentId, statusChange, terminalReason).also { transition ->
        appendTerminalOutbox(paymentId, statusChange, transition)
      }

  suspend fun finalizeWithoutProviderIntent(
      paymentId: UUID,
      statusChange: PaymentStatusChange,
      origin: TerminalReason,
  ): PaymentStatusTransition =
      statusTransitions.finalizeWithoutProviderIntent(paymentId, statusChange, origin).also {
          transition ->
        appendTerminalOutbox(paymentId, statusChange, transition)
      }

  suspend fun cancellationDecision(paymentId: UUID): PaymentCancellationDecision {
    requestStatePersister.recordCheckoutExpiry(paymentId)
    return cancellationPersister.decisionFor(paymentId)
  }

  suspend fun claimCancellationOrigin(
      paymentId: UUID,
      expectedIntentId: String?,
      origin: TerminalReason,
  ): PaymentCancellationOriginClaim =
      cancellationPersister.claimOrigin(paymentId, expectedIntentId, origin)

  suspend fun resolveUndeterminedCancellation(paymentId: UUID, reason: TerminalReason): Boolean =
      cancellationPersister.resolveUndetermined(paymentId, reason)

  suspend fun markOperationalFailureIfUndeterminedCancellation(
      paymentId: UUID,
      reason: String,
  ): Boolean =
      cancellationPersister.markOperationalFailureIfUndeterminedCancellation(paymentId, reason)

  suspend fun findWebhookStripeEventId(
      paymentId: UUID,
      consumerEventId: UUID,
      stripePaymentIntentId: String,
  ): String? =
      stripeEventAssociationLoader.findWebhookStripeEventId(
          paymentId,
          consumerEventId,
          stripePaymentIntentId,
      )

  suspend fun findWebhookStripePaymentIntentId(paymentId: UUID, consumerEventId: UUID): String? =
      stripeEventAssociationLoader.findWebhookStripePaymentIntentId(paymentId, consumerEventId)

  internal suspend fun loadUnappliedStripePaymentIntent(
      paymentId: UUID,
      stripePaymentIntentId: String? = null,
  ): UnappliedStripePaymentIntent? =
      stripeEventAssociationLoader.loadUnappliedStripePaymentIntent(
          paymentId,
          stripePaymentIntentId,
      )

  suspend fun markWebhookStripeEventApplied(
      stripeEventId: String,
      paymentId: UUID,
      stripePaymentIntentId: String,
      stripeStatus: String,
      cancellationReason: String?,
  ) =
      stripeEventApplicationPersister.markApplied(
          stripeEventId,
          paymentId,
          stripePaymentIntentId,
          stripeStatus,
          cancellationReason,
      )

  suspend fun markCancellationResolved(paymentId: UUID) =
      cancellationPersister.markResolved(paymentId)

  suspend fun markReconciliationRequired(paymentId: UUID, reason: String) {
    if (!markCreationUncertain(paymentId, reason)) reconciliation.markRequired(paymentId, reason)
  }

  suspend fun clearVerifiedAwaitingCustomerReconciliation(paymentId: UUID): Boolean =
      reconciliation.clearVerifiedAwaitingCustomerReconciliation(paymentId)

  suspend fun markOperationalFailure(paymentId: UUID, reason: String): Boolean =
      requestStatePersister.markOperationalFailure(
          requestStatePersister.lockRequestByPaymentId(paymentId),
          reason,
      )

  suspend fun markRequestOnlyOperationalFailure(paymentRequestId: UUID, reason: String): Boolean =
      requestStatePersister.markRequestOnlyOperationalFailure(paymentRequestId, reason)

  private suspend fun reenqueueUnappliedStripeEvents(paymentId: UUID) {
    val payment = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
    stripeEventAssociationLoader.loadUnappliedStripeEventIds(paymentId).forEach { stripeEventId ->
      stripeWebhookHandoffs.append(payment.paymentRequestId, paymentId, stripeEventId)
    }
  }

  private suspend fun appendTerminalOutbox(
      paymentId: UUID,
      statusChange: PaymentStatusChange,
      transition: PaymentStatusTransition,
  ) {
    if (transition != PaymentStatusTransition.Updated) return
    PaymentEventType.forTerminalStatus(statusChange.nextStatus)?.let { eventType ->
      val payment = requireNotNull(loader.load(paymentId)) { "Payment $paymentId does not exist" }
      dsl.appendPaymentOutbox(payment.paymentRequestId, paymentId, eventType, objectMapper)
    }
  }
}
