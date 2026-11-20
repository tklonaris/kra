package com.theok.kra.shell.payment.workflow.temporal

import com.stripe.exception.StripeException
import com.theok.kra.shell.payment.workflow.operations.PaymentCreationRequiresReconciliation
import com.theok.kra.shell.payment.workflow.operations.PaymentWorkflowActivityOperations
import com.theok.kra.shell.payment.workflow.operations.RetryablePaymentTransitionConflict
import com.theok.kra.shell.payment.workflow.operations.TerminalPaymentOutcomeInconsistency
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentProviderStatePersistenceFailure
import io.temporal.activity.Activity
import io.temporal.failure.ApplicationFailure
import java.util.UUID
import kotlinx.coroutines.runBlocking

class PaymentWorkflowActivitiesImpl(
    private val operations: PaymentWorkflowActivityOperations,
    private val maximumAttempts: Int = DEFAULT_MAXIMUM_ATTEMPTS,
) : PaymentSagaActivities {
  override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        try {
          operations.initialise(paymentRequestId)
        } catch (_: PaymentCreationRequiresReconciliation) {
          operations.awaitWebhookRecovery(paymentRequestId)
        }
      }

  override fun processWakeUp(
      paymentRequestId: UUID,
      consumerEventId: UUID,
  ): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        try {
          operations.processWakeUp(paymentRequestId, consumerEventId)
        } catch (_: PaymentCreationRequiresReconciliation) {
          operations.awaitWebhookRecovery(paymentRequestId)
        }
      }

  override fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        operations.reconcile(paymentRequestId)
      }

  override fun requireReconciliation(
      paymentRequestId: UUID,
      reason: String,
  ): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        operations.requireReconciliation(paymentRequestId, reason)
      }

  override fun expire(paymentRequestId: UUID): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        operations.expire(paymentRequestId)
      }

  override fun expireProcessingDeadline(paymentRequestId: UUID): PaymentWorkflowControlState =
      activity(paymentRequestId) {
        operations.expireProcessingDeadline(paymentRequestId)
      }

  private fun activity(
      paymentRequestId: UUID,
      block: suspend () -> PaymentWorkflowControlState,
  ): PaymentWorkflowControlState = runBlocking {
    try {
      block()
    } catch (failure: TerminalPaymentOutcomeInconsistency) {
      operations.markOperationalFailure(
          paymentRequestId,
          failure.message ?: "Conflicting terminal payment outcome",
      )
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          failure.message ?: "Conflicting terminal payment outcome",
          TerminalPaymentOutcomeInconsistency.FAILURE_TYPE,
          failure,
      )
    } catch (failure: PaymentProviderStatePersistenceFailure) {
      if (Activity.getExecutionContext().info.attempt < maximumAttempts) throw failure
      awaitDurableReconciliation(paymentRequestId, failure)
    } catch (failure: RetryablePaymentTransitionConflict) {
      if (Activity.getExecutionContext().info.attempt < maximumAttempts) throw failure
      awaitDurableReconciliation(paymentRequestId, failure)
    } catch (failure: StripeException) {
      if (Activity.getExecutionContext().info.attempt < maximumAttempts) throw failure
      awaitDurableReconciliation(paymentRequestId, failure)
    }
  }

  /**
   * A provider call may already have succeeded when a local follow-up operation fails. Keep the
   * workflow alive after making that uncertainty durable so queued or later verified webhook Stripe
   * events can recover it; failing the workflow would discard its queued signals.
   */
  private suspend fun awaitDurableReconciliation(
      paymentRequestId: UUID,
      failure: Exception,
  ): PaymentWorkflowControlState {
    val control =
        operations.requireReconciliation(
            paymentRequestId,
            "Payment workflow operation failed after $maximumAttempts attempts: ${failure.javaClass.simpleName}",
        )
    return if (control.complete) control else operations.awaitWebhookRecovery(paymentRequestId)
  }

  private companion object {
    const val DEFAULT_MAXIMUM_ATTEMPTS = 3
  }
}
