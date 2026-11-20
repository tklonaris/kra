package com.theok.kra.shell.payment.workflow.temporal

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.shell.payment.configuration.PaymentTemporalProperties
import com.theok.kra.shell.payment.workflow.operations.PaymentWorkflowActivityOperations
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.worker.WorkerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("payments")
@ConditionalOnProperty("payments.worker.enabled", havingValue = "true")
class PaymentTemporalConfiguration {
  @Bean(destroyMethod = "shutdown")
  fun paymentTemporalService(@Value("\${temporal.address}") address: String): WorkflowServiceStubs =
      WorkflowServiceStubs.newServiceStubs(
          WorkflowServiceStubsOptions.newBuilder().setTarget(address).build(),
      )

  @Bean
  fun paymentWorkflowClient(
      paymentTemporalService: WorkflowServiceStubs,
      @Value("\${temporal.namespace}") namespace: String,
      objectMapper: ObjectMapper,
  ): WorkflowClient =
      WorkflowClient.newInstance(
          paymentTemporalService,
          WorkflowClientOptions.newBuilder()
              .setNamespace(namespace)
              .setDataConverter(
                  DefaultDataConverter.newDefaultInstance()
                      .withPayloadConverterOverrides(JacksonJsonPayloadConverter(objectMapper)),
              )
              .build(),
      )

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  fun paymentWorkerFactory(
      client: WorkflowClient,
      properties: PaymentTemporalProperties,
      operations: PaymentWorkflowActivityOperations,
  ): WorkerFactory =
      WorkerFactory.newInstance(client).also { factory ->
        val worker = factory.newWorker(properties.taskQueue)
        worker.registerWorkflowImplementationFactory(PaymentSagaWorkflow::class.java) {
          PaymentWorkflowImpl(
              properties.activityStartToCloseTimeout,
              properties.activityScheduleToCloseTimeout,
              properties.activityMaximumAttempts,
              properties.reconciliationInterval,
          )
        }
        worker.registerActivitiesImplementations(
            PaymentWorkflowActivitiesImpl(operations, properties.activityMaximumAttempts),
        )
      }
}
