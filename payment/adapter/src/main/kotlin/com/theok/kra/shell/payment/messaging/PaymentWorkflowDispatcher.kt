package com.theok.kra.shell.payment.messaging

import com.theok.kra.shell.payment.configuration.PaymentTemporalProperties
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEvent
import com.theok.kra.shell.payment.workflow.temporal.PaymentSagaWorkflow
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import java.util.UUID
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/** Client-side Temporal hand-off. Broker acknowledgement is deliberately owned by the listener. */
@Component
@Profile("payments")
@ConditionalOnProperty("payments.worker.enabled", havingValue = "true")
class PaymentWorkflowDispatcher(
    private val client: WorkflowClient,
    private val properties: PaymentTemporalProperties,
) {
  fun dispatch(event: PaymentConsumerEvent) {
    val workflow = newWorkflow(event.paymentRequestId)
    WorkflowStub.fromTyped(workflow)
        .signalWithStart("wake", arrayOf(event.consumerEventId), arrayOf(event.paymentRequestId))
  }

  private fun newWorkflow(paymentRequestId: UUID): PaymentSagaWorkflow =
      client.newWorkflowStub(
          PaymentSagaWorkflow::class.java,
          WorkflowOptions.newBuilder()
              .setWorkflowId(workflowId(paymentRequestId))
              .setTaskQueue(properties.taskQueue)
              .setWorkflowIdConflictPolicy(
                  WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
              )
              .setWorkflowIdReusePolicy(
                  WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
              )
              .build(),
      )

  private fun workflowId(paymentRequestId: UUID) = "payment-request-$paymentRequestId"
}
