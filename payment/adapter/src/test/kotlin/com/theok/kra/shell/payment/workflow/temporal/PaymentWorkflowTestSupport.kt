package com.theok.kra.shell.payment.workflow.temporal

import com.theok.kra.platform.database.json.JooqJsonSupport
import io.temporal.client.WorkflowClientOptions
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.testing.TestEnvironmentOptions
import java.time.Duration
import java.util.UUID

internal fun paymentWorkflowTestEnvironmentOptions(): TestEnvironmentOptions =
    TestEnvironmentOptions.newBuilder()
        .setWorkflowClientOptions(
            WorkflowClientOptions.newBuilder()
                .setDataConverter(
                    DefaultDataConverter.newDefaultInstance()
                        .withPayloadConverterOverrides(
                            JacksonJsonPayloadConverter(JooqJsonSupport.objectMapper()),
                        ),
                )
                .build(),
        )
        .build()

internal abstract class PaymentSagaActivitiesStub : PaymentSagaActivities {
  override fun initialise(paymentRequestId: UUID): PaymentWorkflowControlState =
      error("Unexpected initialisation")

  override fun processWakeUp(
      paymentRequestId: UUID,
      consumerEventId: UUID,
  ): PaymentWorkflowControlState = error("Unexpected wake-up")

  override fun requireReconciliation(
      paymentRequestId: UUID,
      reason: String,
  ): PaymentWorkflowControlState = error("Unexpected reconciliation failure")

  override fun reconcile(paymentRequestId: UUID): PaymentWorkflowControlState =
      error("Unexpected timer reconciliation")

  override fun expire(paymentRequestId: UUID): PaymentWorkflowControlState =
      error("Unexpected checkout expiry")

  override fun expireProcessingDeadline(paymentRequestId: UUID): PaymentWorkflowControlState =
      error("Unexpected processing deadline expiry")
}

internal fun completedPaymentWorkflowState() =
    PaymentWorkflowControlState(
        complete = true,
        awaitingProcessingOutcome = false,
        reconciliationRequired = false,
        checkoutWait = Duration.ZERO,
        processingWait = Duration.ZERO,
    )

internal fun reconciliationPaymentWorkflowState() =
    PaymentWorkflowControlState(
        complete = false,
        awaitingProcessingOutcome = false,
        reconciliationRequired = true,
        checkoutWait = Duration.ZERO,
        processingWait = Duration.ZERO,
    )
