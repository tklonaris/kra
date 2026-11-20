package com.theok.kra.shell.payment.workflow.temporal

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class PaymentWorkflowFailureRecoveryTest {
  @Test
  fun `exhausted non-timeout initialisation failures require durable reconciliation`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = InitialisationFailureActivities()
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val workflow = workflow(environment)
      WorkflowClient.start(workflow::run, UUID.randomUUID())
      workflow.wake(UUID.randomUUID())

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(1, activities.reconciliationRequests)
    }
  }

  @Test
  fun `exhausted wake-up and reconciliation failures remain recoverable by the timer`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = WakeUpAndReconciliationFailureActivities()
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, Duration.ZERO)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val workflow = workflow(environment)
      WorkflowClient.start(workflow::run, UUID.randomUUID())
      workflow.wake(UUID.randomUUID())

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(1, activities.reconciliationAttempts)
      assertEquals(1, activities.timerReconciliations)
    }
  }

  private class InitialisationFailureActivities : PaymentSagaActivitiesStub() {
    var reconciliationRequests = 0
      private set

    override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState =
        error("Temporary database persistence failure")

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState = initialise(paymentRequestId)

    override fun requireReconciliation(
        paymentRequestId: UUID,
        reason: String,
    ): PaymentWorkflowControlState {
      reconciliationRequests += 1
      return completedPaymentWorkflowState()
    }
  }

  private class WakeUpAndReconciliationFailureActivities : PaymentSagaActivitiesStub() {
    var reconciliationAttempts = 0
      private set

    var timerReconciliations = 0
      private set

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState = error("Temporary database outage")

    override fun requireReconciliation(
        paymentRequestId: UUID,
        reason: String,
    ): PaymentWorkflowControlState {
      reconciliationAttempts += 1
      error("Database remains unavailable")
    }

    override fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState {
      timerReconciliations += 1
      return completedPaymentWorkflowState()
    }
  }

  private fun workflow(environment: TestWorkflowEnvironment): PaymentSagaWorkflow =
      environment.workflowClient.newWorkflowStub(
          PaymentSagaWorkflow::class.java,
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build(),
      )

  private companion object {
    const val TASK_QUEUE = "payment-workflow-test"
  }
}
