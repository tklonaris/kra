package com.theok.kra.shell.payment.workflow.temporal

import com.theok.kra.shell.payment.workflow.operations.TerminalPaymentOutcomeInconsistency
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.workflow.Workflow
import java.time.Duration
import java.util.UUID

/** Temporal-only coordination; all business changes are delegated to transactional activities. */
class PaymentWorkflowImpl(
    activityStartToCloseTimeout: Duration,
    activityScheduleToCloseTimeout: Duration,
    activityMaximumAttempts: Int,
    private val reconciliationInterval: Duration = Duration.ofMinutes(2),
) : PaymentSagaWorkflow {
  private val activities =
      Workflow.newActivityStub(
          PaymentSagaActivities::class.java,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(activityStartToCloseTimeout)
              .setScheduleToCloseTimeout(activityScheduleToCloseTimeout)
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofMinutes(1))
                      .setBackoffCoefficient(2.0)
                      .setMaximumInterval(Duration.ofMinutes(2))
                      .setMaximumAttempts(activityMaximumAttempts)
                      .build(),
              )
              .build(),
      )
  private val pendingEventIds = linkedSetOf<UUID>()
  private var processedWakeUpCount = 0
  private var reconciliationCycleCount = 0

  override fun run(paymentRequestId: UUID) {
    var state =
        if (Workflow.getInfo().continuedExecutionRunId.isPresent) {
          initialiseOrRequireReconciliation(paymentRequestId)
        } else {
          Workflow.await { pendingEventIds.isNotEmpty() }
          requireNotNull(drain(paymentRequestId)) { "Payment workflow requires a wake-up" }
        }
    continueAsNewIfHistoryIsLarge(paymentRequestId, state)
    while (!state.complete || pendingEventIds.isNotEmpty()) {
      if (state.complete) {
        state = requireNotNull(drain(paymentRequestId))
        continue
      }
      val wait =
          when {
            state.reconciliationRequired -> reconciliationInterval
            state.awaitingProcessingOutcome -> state.processingWait
            else -> state.checkoutWait
          }
      if (Workflow.await(wait) { pendingEventIds.isNotEmpty() }) {
        drain(paymentRequestId)?.let { state = it }
      } else {
        state =
            when {
              state.reconciliationRequired -> {
                reconciliationCycleCount++
                runTimerActivity(paymentRequestId, state, "reconciliation") {
                  activities.reconcile(paymentRequestId)
                }
              }
              state.awaitingProcessingOutcome ->
                  runTimerActivity(paymentRequestId, state, "processing deadline") {
                    activities.expireProcessingDeadline(paymentRequestId)
                  }
              else ->
                  runTimerActivity(paymentRequestId, state, "checkout deadline") {
                    activities.expire(paymentRequestId)
                  }
            }
      }
      continueAsNewIfHistoryIsLarge(paymentRequestId, state)
    }
  }

  override fun wake(consumerEventId: UUID) {
    pendingEventIds += consumerEventId
  }

  /** Rebuilds timer state after Continue-as-New from authoritative PostgreSQL state. */
  private fun initialiseOrRequireReconciliation(
      paymentRequestId: UUID,
  ): PaymentWorkflowControlState =
      try {
        activities.initialise(paymentRequestId)
      } catch (failure: ActivityFailure) {
        requireReconciliationOrKeepActive(
            paymentRequestId,
            failure,
            "Temporal continuation initialisation exhausted: ${failure.cause?.javaClass?.simpleName ?: failure.javaClass.simpleName}",
        )
      }

  private fun ActivityFailure.isTerminalOutcomeInconsistency(): Boolean =
      (cause as? ApplicationFailure)?.type == TerminalPaymentOutcomeInconsistency.FAILURE_TYPE

  /**
   * Timer activities have no broker delivery to restart them after their bounded Temporal retry
   * policy is exhausted. Preserve the active saga and retry from durable state instead.
   */
  private fun runTimerActivity(
      paymentRequestId: UUID,
      previousState: PaymentWorkflowControlState,
      operation: String,
      action: () -> PaymentWorkflowControlState,
  ): PaymentWorkflowControlState =
      try {
        action()
      } catch (failure: ActivityFailure) {
        requireReconciliationOrKeepActive(
            paymentRequestId,
            failure,
            "Temporal $operation activity exhausted: ${failure.cause?.javaClass?.simpleName ?: failure.javaClass.simpleName}",
            previousState.copy(reconciliationRequired = true),
        )
      }

  private fun drain(paymentRequestId: UUID): PaymentWorkflowControlState? {
    var result: PaymentWorkflowControlState? = null
    while (pendingEventIds.isNotEmpty()) {
      val consumerEventId = pendingEventIds.first().also(pendingEventIds::remove)
      result = processWakeUpOrRequireReconciliation(paymentRequestId, consumerEventId)
      processedWakeUpCount++
    }
    return result
  }

  /** Keeps exhausted wake-up activities recoverable without treating a terminal conflict as one. */
  private fun processWakeUpOrRequireReconciliation(
      paymentRequestId: UUID,
      consumerEventId: UUID,
  ): PaymentWorkflowControlState =
      try {
        activities.processWakeUp(paymentRequestId, consumerEventId)
      } catch (failure: ActivityFailure) {
        requireReconciliationOrKeepActive(
            paymentRequestId,
            failure,
            "Temporal wake-up activity exhausted: ${failure.cause?.javaClass?.simpleName ?: failure.javaClass.simpleName}",
        )
      }

  private fun requireReconciliationOrKeepActive(
      paymentRequestId: UUID,
      failure: ActivityFailure,
      reason: String,
      fallback: PaymentWorkflowControlState = reconciliationControlState(),
  ): PaymentWorkflowControlState {
    if (failure.isTerminalOutcomeInconsistency()) throw failure
    return try {
      activities.requireReconciliation(paymentRequestId, reason)
    } catch (reconciliationFailure: ActivityFailure) {
      if (reconciliationFailure.isTerminalOutcomeInconsistency()) throw reconciliationFailure
      fallback
    }
  }

  private fun reconciliationControlState() =
      PaymentWorkflowControlState(
          complete = false,
          awaitingProcessingOutcome = false,
          reconciliationRequired = true,
          checkoutWait = Duration.ZERO,
          processingWait = Duration.ZERO,
      )

  private fun continueAsNewIfHistoryIsLarge(
      paymentRequestId: UUID,
      state: PaymentWorkflowControlState,
  ) {
    if (
        !state.complete &&
            pendingEventIds.isEmpty() &&
            processedWakeUpCount + reconciliationCycleCount >= MAXIMUM_EVENTS_PER_EXECUTION
    ) {
      Workflow.continueAsNew(paymentRequestId)
    }
  }

  private companion object {
    const val MAXIMUM_EVENTS_PER_EXECUTION = 100
  }
}
