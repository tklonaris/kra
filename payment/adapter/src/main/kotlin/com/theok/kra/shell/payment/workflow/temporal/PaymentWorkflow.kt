package com.theok.kra.shell.payment.workflow.temporal

import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowState
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration
import java.util.UUID

@WorkflowInterface
interface PaymentSagaWorkflow {
  @WorkflowMethod fun run(paymentRequestId: UUID)

  /** A broker delivery is a wake-up; activities resolve its meaning from durable state. */
  @SignalMethod fun wake(consumerEventId: UUID)
}

@ActivityInterface
interface PaymentSagaActivities {
  @ActivityMethod fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState

  @ActivityMethod
  fun processWakeUp(paymentRequestId: UUID, consumerEventId: UUID): PaymentWorkflowControlState

  @ActivityMethod fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState

  @ActivityMethod
  fun requireReconciliation(paymentRequestId: UUID, reason: String): PaymentWorkflowControlState

  @ActivityMethod fun expire(paymentRequestId: UUID): PaymentWorkflowControlState

  @ActivityMethod fun expireProcessingDeadline(paymentRequestId: UUID): PaymentWorkflowControlState
}

data class PaymentWorkflowControlState(
    val complete: Boolean,
    val awaitingProcessingOutcome: Boolean,
    val reconciliationRequired: Boolean,
    val checkoutWait: Duration,
    val processingWait: Duration,
)

internal fun PaymentWorkflowState.toWorkflowControlState() =
    PaymentWorkflowControlState(
        complete = status.terminal && !reconciliationRequired,
        awaitingProcessingOutcome = status == PaymentStatus.PROCESSING,
        reconciliationRequired = reconciliationRequired,
        checkoutWait =
            Duration.between(databaseNow, checkoutDeadlineAt).coerceAtLeast(Duration.ZERO),
        processingWait =
            Duration.between(databaseNow, processingDeadlineAt).coerceAtLeast(Duration.ZERO),
    )
