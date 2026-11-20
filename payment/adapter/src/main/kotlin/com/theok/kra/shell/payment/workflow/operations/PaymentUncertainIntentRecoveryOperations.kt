package com.theok.kra.shell.payment.workflow.operations

import com.theok.kra.core.payment.domain.PaymentStatusChange
import com.theok.kra.core.payment.domain.intentCreated
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentProviderStatePersistenceFailure
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import java.util.UUID
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/** Recovers one unrecorded Stripe intent from an already-associated, verified provider event. */
@Component
@Profile("payments")
class PaymentUncertainIntentRecoveryOperations(
    private val workflowPersister: PaymentWorkflowPersister,
    private val stripePaymentGateway: StripePaymentGateway,
    private val clientSecretCipher: ClientSecretCipher,
) {
  suspend fun recoverFromWebhook(paymentId: UUID, consumerEventId: UUID) {
    val recovery =
        workflowPersister.transaction {
          load(paymentId)?.takeIf(PaymentWorkflowState::mayRecoverUncertainProviderIntent)?.let {
              payment ->
            findWebhookStripePaymentIntentId(paymentId, consumerEventId)?.let { intentId ->
              IntentRecovery(payment, intentId, intentCreatedStatusChange(payment))
            }
          }
        } ?: return
    recover(paymentId, recovery)
  }

  internal suspend fun recoverFromDurableWebhook(paymentId: UUID): RecoveredProviderIntent? {
    val recovery =
        workflowPersister.transaction {
          load(paymentId)?.takeIf(PaymentWorkflowState::mayRecoverUncertainProviderIntent)?.let {
              payment ->
            loadUnappliedStripePaymentIntent(paymentId)?.let { event ->
              IntentRecovery(
                  payment,
                  event.stripePaymentIntentId,
                  intentCreatedStatusChange(payment),
                  event.stripeEventId,
              )
            }
          }
        } ?: return null
    return recover(paymentId, recovery)
  }

  private suspend fun recover(
      paymentId: UUID,
      recovery: IntentRecovery,
  ): RecoveredProviderIntent? {
    val intent = stripePaymentGateway.retrieve(recovery.intentId)
    check(intent.matchesKnownPayment(recovery.intentId, recovery.payment)) {
      "Verified provider event does not match payment $paymentId"
    }
    val envelope =
        clientSecretCipher.encrypt(
            paymentId.toString(),
            requireNotNull(intent.clientSecret) {
              "Recovered Stripe intent must expose a client secret"
            },
        )
    val attached =
        try {
          workflowPersister.transaction {
            recoverCreatedIntent(paymentId, recovery.intentId, envelope, recovery.statusChange)
          }
        } catch (failure: Exception) {
          throw PaymentProviderStatePersistenceFailure(paymentId, failure)
        }
    return if (attached && recovery.stripeEventId != null) {
      RecoveredProviderIntent(
          recovery.stripeEventId,
          recovery.intentId,
          intent.status,
          intent.cancellationReason,
      )
    } else {
      null
    }
  }

  private fun intentCreatedStatusChange(payment: PaymentWorkflowState): PaymentStatusChange =
      payment.status.intentCreated()

  private data class IntentRecovery(
      val payment: PaymentWorkflowState,
      val intentId: String,
      val statusChange: PaymentStatusChange,
      val stripeEventId: String? = null,
  )
}

internal data class RecoveredProviderIntent(
    val stripeEventId: String,
    val intentId: String,
    val providerStatus: String,
    val providerCancellationReason: String?,
)
