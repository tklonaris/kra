package com.theok.kra.shell.payment.workflow.operations

import com.stripe.exception.StripeException
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCreateParams
import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.PaymentStatusChange
import com.theok.kra.core.payment.domain.intentCreated
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import com.theok.kra.shell.payment.workflow.temporal.toWorkflowControlState
import java.util.UUID
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/** Creates a Stripe intent only after recording its durable creation claim. */
@Component
@Profile("payments")
class PaymentIntentCreationOperations(
    private val workflowPersister: PaymentWorkflowPersister,
    private val stripePaymentGateway: StripePaymentGateway,
    private val clientSecretCipher: ClientSecretCipher,
    private val lifecycleReconciliationOperations: PaymentLifecycleReconciliationOperations,
    private val uncertainIntentRecovery: PaymentUncertainIntentRecoveryOperations,
) {
  suspend fun initialise(paymentId: UUID): PaymentWorkflowControlState {
    val payment = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
    if (payment.reconciliationRequired) return reconcile(payment)
    if (payment.status.terminal || payment.checkoutExpired()) {
      return payment.toWorkflowControlState()
    }
    if (payment.cancellationRequested) {
      return lifecycleReconciliationOperations.reconcileCancellation(paymentId)
    }

    if (payment.intentId == null) {
      val operation = claimCreation(paymentId)
      if (operation == null) {
        val current = requireNotNull(load(paymentId))
        return if (current.cancellationRequested || current.checkoutExpired()) {
          lifecycleReconciliationOperations.reconcileCancellation(paymentId)
        } else {
          current.toWorkflowControlState()
        }
      }
      val intent =
          try {
            stripePaymentGateway.create(
                PaymentIntentCreateParams.builder()
                    .setAmount(operation.amountMinor)
                    .setCurrency(operation.currency.lowercase())
                    .setCaptureMethod(PaymentIntentCreateParams.CaptureMethod.AUTOMATIC)
                    .addAllowedPaymentMethodType(
                        PaymentIntentCreateParams.AllowedPaymentMethodType.CARD,
                    )
                    .putMetadata("kra_payment_id", paymentId.toString())
                    .build(),
                RequestOptions.builder().setIdempotencyKey(operation.idempotencyKey).build(),
            )
          } catch (failure: StripeException) {
            workflowPersister.transaction {
              markCreationUncertain(paymentId, "Stripe create failed before its outcome was known")
            }
            throw PaymentCreationRequiresReconciliation(paymentId, failure)
          }
      persistCreatedIntentAfterStripeCreate(paymentId, intent, operation)
    }
    val current = requireNotNull(load(paymentId))
    return if (current.cancellationRequested || current.checkoutExpired()) {
      lifecycleReconciliationOperations.reconcileCancellation(paymentId)
    } else {
      current.toWorkflowControlState()
    }
  }

  private suspend fun claimCreation(paymentId: UUID): CreationOperation? =
      when (
          val result = workflowPersister.transaction {
            val claim =
                requireNotNull(loadForIntentCreationForUpdate(paymentId)) {
                  "Payment $paymentId does not exist"
                }
            when {
              claim.intentId != null -> CreationClaimResult.IntentAlreadyCreated
              claim.status != PaymentStatus.REQUESTED ||
                  claim.cancellationRequested ||
                  claim.checkoutExpired() -> CreationClaimResult.NoLongerEligible
              claim.requiresReconciliation() -> {
                markCreationUncertain(paymentId, "Stripe creation outcome requires reconciliation")
                CreationClaimResult.ReconciliationRequired
              }
              else -> {
                val fingerprint = "${claim.amountMinor}:${claim.currency}:automatic-card"
                val operationId = claim.creationOperationId ?: UUID.randomUUID()
                val idempotencyKey =
                    claim.stripeIdempotencyKey ?: "kra-payment-$paymentId-$operationId"
                check(
                    claim.creationRequestFingerprint == null ||
                        claim.creationRequestFingerprint == fingerprint,
                ) {
                  "Stripe creation parameters changed"
                }
                if (claimIntentCreation(paymentId, operationId, idempotencyKey, fingerprint)) {
                  CreationClaimResult.Operation(
                      CreationOperation(
                          operationId,
                          claim.amountMinor,
                          claim.currency,
                          idempotencyKey,
                          intentCreatedStatusChange(claim.status),
                      ),
                  )
                } else {
                  CreationClaimResult.NoLongerEligible
                }
              }
            }
          }
      ) {
        CreationClaimResult.IntentAlreadyCreated -> null
        CreationClaimResult.NoLongerEligible -> null
        CreationClaimResult.ReconciliationRequired ->
            throw PaymentCreationRequiresReconciliation(paymentId)
        is CreationClaimResult.Operation -> result.operation
      }

  private suspend fun load(paymentId: UUID): PaymentWorkflowState? = workflowPersister.transaction {
    load(paymentId)
  }

  suspend fun reconcile(paymentId: UUID): PaymentWorkflowControlState =
      reconcile(requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" })

  private suspend fun reconcile(payment: PaymentWorkflowState): PaymentWorkflowControlState =
      when {
        payment.mayRecoverUncertainProviderIntent() ->
            reconcileUncertainIntentFromDurableWebhook(payment.paymentId)
        payment.requiresTerminalCancellationCauseReconciliation() ->
            lifecycleReconciliationOperations.reconcileCancellation(payment.paymentId)
        payment.cancellationRequested ->
            lifecycleReconciliationOperations.reconcileCancellation(payment.paymentId)
        !payment.reconciliationRequired -> payment.toWorkflowControlState()
        else -> refreshReconciliation(payment.paymentId)
      }

  private suspend fun reconcileUncertainIntentFromDurableWebhook(
      paymentId: UUID,
  ): PaymentWorkflowControlState {
    val recovered = uncertainIntentRecovery.recoverFromDurableWebhook(paymentId)
    val refreshed = refreshReconciliation(paymentId, recovered?.stripeEventId)
    val reconciled =
        if (refreshed.complete) refreshed
        else lifecycleReconciliationOperations.reconcileCancellation(paymentId)
    if (recovered != null && !reconciled.reconciliationRequired) {
      workflowPersister.transaction {
        markWebhookStripeEventApplied(
            recovered.stripeEventId,
            paymentId,
            recovered.intentId,
            recovered.providerStatus,
            recovered.providerCancellationReason,
        )
      }
    }
    return reconciled
  }

  /**
   * Carries the durable provider-event identity into the same transaction that applies its Stripe
   * fact. If attachment succeeded but the first refresh failed, a later timer reconciliation finds
   * the still-unapplied event for the now-known intent and acknowledges it atomically.
   */
  private suspend fun refreshReconciliation(
      paymentId: UUID,
      recoveredStripeEventId: String? = null,
  ): PaymentWorkflowControlState {
    val stripeEventId =
        recoveredStripeEventId
            ?: workflowPersister.transaction {
              load(paymentId)?.intentId?.let { intentId ->
                loadUnappliedStripePaymentIntent(paymentId, intentId)?.stripeEventId
              }
            }
    return lifecycleReconciliationOperations.refreshIntentStatus(
        paymentId,
        webhookStripeEventId = stripeEventId,
    )
  }

  /**
   * Once Stripe has returned an intent, local encryption or persistence failure leaves its outcome
   * unknown. Record that state before returning to the activity so the bounded workflow remains
   * available for verified webhook recovery rather than becoming a failed, non-restartable run.
   */
  private suspend fun persistCreatedIntentAfterStripeCreate(
      paymentId: UUID,
      intent: PaymentIntent,
      operation: CreationOperation,
  ) {
    val persisted =
        try {
          val payment = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
          val intentId = intent.requireCreatedIntentId(payment)
          val envelope =
              clientSecretCipher.encrypt(paymentId.toString(), requireNotNull(intent.clientSecret))
          workflowPersister.transaction {
            persistCreatedIntent(
                paymentId,
                operation.operationId,
                intentId,
                envelope,
                operation.statusChange,
            )
          }
        } catch (failure: Exception) {
          recordUncertainCreation(paymentId, failure)
          throw PaymentCreationRequiresReconciliation(paymentId, failure)
        }
    if (!persisted) {
      val current = requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
      when {
        current.intentId == intent.id -> Unit
        current.intentId != null ->
            error(
                "Payment $paymentId is associated with ${current.intentId}, not created intent ${intent.id}",
            )
        else ->
            workflowPersister.transaction {
              markCreationUncertain(
                  paymentId,
                  "Stripe create succeeded but its intent ID could not be persisted",
              )
            }
      }
    }
  }

  private suspend fun recordUncertainCreation(paymentId: UUID, failure: Exception) {
    try {
      workflowPersister.transaction {
        markCreationUncertain(
            paymentId,
            "Stripe create succeeded but local intent persistence failed",
        )
      }
    } catch (reconciliationFailure: Exception) {
      failure.addSuppressed(reconciliationFailure)
    }
  }

  private fun PaymentWorkflowState.requiresReconciliation(): Boolean =
      intentCreationState == IntentCreationState.UNCERTAIN ||
          (intentCreationState == IntentCreationState.ATTEMPTING &&
              creationAttemptedAt != null &&
              creationAttemptedAt
                  .toInstant()
                  .isBefore(
                      databaseNow.minusSeconds(
                          CREATION_IDEMPOTENCY_WINDOW_HOURS * SECONDS_PER_HOUR,
                      ),
                  ))

  private fun intentCreatedStatusChange(status: PaymentStatus): PaymentStatusChange =
      status.intentCreated()

  private data class CreationOperation(
      val operationId: UUID,
      val amountMinor: Long,
      val currency: String,
      val idempotencyKey: String,
      val statusChange: PaymentStatusChange,
  )

  private sealed interface CreationClaimResult {
    data object IntentAlreadyCreated : CreationClaimResult

    data object NoLongerEligible : CreationClaimResult

    data object ReconciliationRequired : CreationClaimResult

    data class Operation(val operation: CreationOperation) : CreationClaimResult
  }

  private companion object {
    const val CREATION_IDEMPOTENCY_WINDOW_HOURS = 23L
    const val SECONDS_PER_HOUR = 60L * 60L
  }
}
