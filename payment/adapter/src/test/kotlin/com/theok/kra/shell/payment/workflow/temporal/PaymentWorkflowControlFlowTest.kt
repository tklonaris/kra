package com.theok.kra.shell.payment.workflow.temporal

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class PaymentWorkflowControlFlowTest {
  @Test
  fun `signals arriving during terminal completion are drained before closure`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val workflowId = "payment-workflow-${UUID.randomUUID()}"
      val activities = FinalDrainActivities(environment.workflowClient, workflowId)
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val workflow = workflow(environment, workflowId)
      WorkflowClient.start(workflow::run, UUID.randomUUID())
      workflow.wake(UUID.randomUUID())

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(2, activities.processedWakeUps)
    }
  }

  @Test
  fun `continues as new with the same request after draining bounded wake-up history`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = ContinuingActivities()
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val paymentRequestId = UUID.randomUUID()
      val workflow = workflow(environment)
      WorkflowClient.start(workflow::run, paymentRequestId)
      repeat(101) { workflow.wake(UUID.randomUUID()) }

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(listOf(paymentRequestId), activities.initialisedRequests)
      assertEquals(101, activities.processedWakeUps)
    }
  }

  @Test
  fun `checkout expiry is addressed by payment request ID`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = ExpiryActivities()
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val paymentRequestId = UUID.randomUUID()
      val workflow = workflow(environment)
      WorkflowClient.start(workflow::run, paymentRequestId)
      workflow.wake(UUID.randomUUID())

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(paymentRequestId, activities.expiredRequestId)
    }
  }

  private class FinalDrainActivities(
      private val workflowClient: WorkflowClient,
      private val workflowId: String,
  ) : PaymentSagaActivitiesStub() {
    var processedWakeUps = 0
      private set

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState {
      processedWakeUps += 1
      if (processedWakeUps == 1) {
        workflowClient
            .newWorkflowStub(PaymentSagaWorkflow::class.java, workflowId)
            .wake(UUID.randomUUID())
      }
      return completedPaymentWorkflowState()
    }
  }

  private class ContinuingActivities : PaymentSagaActivitiesStub() {
    val initialisedRequests = mutableListOf<UUID>()
    var processedWakeUps = 0
      private set

    override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState {
      initialisedRequests += paymentRequestId
      return completedPaymentWorkflowState()
    }

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState {
      processedWakeUps += 1
      return PaymentWorkflowControlState(false, false, false, Duration.ofHours(1), Duration.ZERO)
    }
  }

  private class ExpiryActivities : PaymentSagaActivitiesStub() {
    var expiredRequestId: UUID? = null
      private set

    override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState =
        PaymentWorkflowControlState(false, false, false, Duration.ZERO, Duration.ZERO)

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState = initialise(paymentRequestId)

    override fun expire(paymentRequestId: UUID): PaymentWorkflowControlState {
      expiredRequestId = paymentRequestId
      return completedPaymentWorkflowState()
    }
  }

  private fun workflow(
      environment: TestWorkflowEnvironment,
      workflowId: String? = null,
  ): PaymentSagaWorkflow =
      environment.workflowClient.newWorkflowStub(
          PaymentSagaWorkflow::class.java,
          WorkflowOptions.newBuilder()
              .setTaskQueue(TASK_QUEUE)
              .apply {
                workflowId?.let(::setWorkflowId)
              }
              .build(),
      )

  private companion object {
    const val TASK_QUEUE = "payment-workflow-test"
  }
}
