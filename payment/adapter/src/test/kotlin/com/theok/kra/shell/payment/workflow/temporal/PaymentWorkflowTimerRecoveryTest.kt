package com.theok.kra.shell.payment.workflow.temporal

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.testing.TestWorkflowEnvironment
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class PaymentWorkflowTimerRecoveryTest {
  @Test
  fun `unresolved reconciliation is retried before the workflow completes`() {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = ReconciliationActivities()
      val worker = environment.newWorker(TASK_QUEUE)
      worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
        PaymentWorkflowImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, Duration.ZERO)
      }
      worker.registerActivitiesImplementations(activities)
      environment.start()

      val paymentRequestId = UUID.randomUUID()
      val workflow = workflow(environment)
      WorkflowClient.start(workflow::run, paymentRequestId)
      workflow.wake(UUID.randomUUID())

      WorkflowStub.fromTyped(workflow).getResult(Void::class.java)

      assertEquals(paymentRequestId, activities.reconciledRequestId)
    }
  }

  @Test
  fun `exhausted reconciliation timer activity remains recoverable`() =
      assertExhaustedTimerActivityIsRecovered(TimerFailureMode.RECONCILIATION)

  @Test
  fun `exhausted checkout timer activity remains recoverable`() =
      assertExhaustedTimerActivityIsRecovered(TimerFailureMode.CHECKOUT_EXPIRY)

  @Test
  fun `exhausted processing timer activity remains recoverable`() =
      assertExhaustedTimerActivityIsRecovered(TimerFailureMode.PROCESSING_EXPIRY)

  private fun assertExhaustedTimerActivityIsRecovered(mode: TimerFailureMode) {
    TestWorkflowEnvironment.newInstance(paymentWorkflowTestEnvironmentOptions()).use { environment
      ->
      val activities = TimerFailureActivities(mode)
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

      assertEquals(1, activities.reconciliationRequests)
      assertEquals(2, activities.timerAttempts)
    }
  }

  private class ReconciliationActivities : PaymentSagaActivitiesStub() {
    var reconciledRequestId: UUID? = null
      private set

    override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState =
        reconciliationPaymentWorkflowState()

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState = initialise(paymentRequestId)

    override fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState {
      reconciledRequestId = paymentRequestId
      return completedPaymentWorkflowState()
    }
  }

  private enum class TimerFailureMode {
    RECONCILIATION,
    CHECKOUT_EXPIRY,
    PROCESSING_EXPIRY,
  }

  private class TimerFailureActivities(private val mode: TimerFailureMode) :
      PaymentSagaActivitiesStub() {
    var reconciliationRequests = 0
      private set

    var timerAttempts = 0
      private set

    override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState = initialState()

    override fun processWakeUp(
        paymentRequestId: UUID,
        consumerEventId: UUID,
    ): PaymentWorkflowControlState = initialState()

    override fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState = timerAttempt()

    override fun requireReconciliation(
        paymentRequestId: UUID,
        reason: String,
    ): PaymentWorkflowControlState {
      reconciliationRequests += 1
      return reconciliationPaymentWorkflowState()
    }

    override fun expire(paymentRequestId: UUID): PaymentWorkflowControlState =
        if (mode == TimerFailureMode.CHECKOUT_EXPIRY) timerAttempt()
        else completedPaymentWorkflowState()

    override fun expireProcessingDeadline(paymentRequestId: UUID): PaymentWorkflowControlState =
        if (mode == TimerFailureMode.PROCESSING_EXPIRY) timerAttempt()
        else completedPaymentWorkflowState()

    private fun timerAttempt(): PaymentWorkflowControlState {
      timerAttempts += 1
      if (timerAttempts == 1) error("Temporary timer activity failure")
      return completedPaymentWorkflowState()
    }

    private fun initialState(): PaymentWorkflowControlState =
        when (mode) {
          TimerFailureMode.RECONCILIATION -> reconciliationPaymentWorkflowState()
          TimerFailureMode.CHECKOUT_EXPIRY ->
              PaymentWorkflowControlState(false, false, false, Duration.ZERO, Duration.ZERO)
          TimerFailureMode.PROCESSING_EXPIRY ->
              PaymentWorkflowControlState(false, true, false, Duration.ZERO, Duration.ZERO)
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
