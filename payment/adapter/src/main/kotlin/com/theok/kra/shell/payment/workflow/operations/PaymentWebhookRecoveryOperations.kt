package com.theok.kra.shell.payment.workflow.operations

import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import com.theok.kra.shell.payment.workflow.temporal.toWorkflowControlState
import java.util.UUID
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/**
 * Recovers a Stripe intent from an associated Stripe event without broad searches or another
 * create.
 */
@Component
@Profile("payments")
class PaymentWebhookRecoveryOperations(
    private val workflowPersister: PaymentWorkflowPersister,
    private val lifecycleReconciliationOperations: PaymentLifecycleReconciliationOperations,
    private val uncertainIntentRecovery: PaymentUncertainIntentRecoveryOperations,
) {
  suspend fun applyWebhook(paymentId: UUID, consumerEventId: UUID): PaymentWorkflowControlState {
    uncertainIntentRecovery.recoverFromWebhook(paymentId, consumerEventId)
    return refreshAndReconcileCancellation(paymentId, consumerEventId)
  }

  suspend fun awaitRecovery(paymentId: UUID): PaymentWorkflowControlState =
      requireNotNull(load(paymentId)) { "Payment $paymentId does not exist" }
          .toWorkflowControlState()

  private suspend fun refreshAndReconcileCancellation(
      paymentId: UUID,
      consumerEventId: UUID?,
  ): PaymentWorkflowControlState {
    val refreshed =
        lifecycleReconciliationOperations.refreshIntentStatus(paymentId, consumerEventId)
    return if (refreshed.complete) refreshed
    else lifecycleReconciliationOperations.reconcileCancellation(paymentId, consumerEventId)
  }

  private suspend fun load(paymentId: UUID): PaymentWorkflowState? = workflowPersister.transaction {
    load(paymentId)
  }
}
