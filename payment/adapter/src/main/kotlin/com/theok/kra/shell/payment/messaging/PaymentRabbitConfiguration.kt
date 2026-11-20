package com.theok.kra.shell.payment.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventPersister
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.springframework.amqp.AmqpRejectAndDontRequeueException
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.FanoutExchange
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.retry.MessageRecoverer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("payments")
@ConditionalOnProperty("payments.worker.enabled", havingValue = "true")
class PaymentRabbitConfiguration {
  @Bean fun paymentEventsExchange() = FanoutExchange("kra.payment.events", true, false)

  @Bean fun paymentDeadLetterExchange() = FanoutExchange("kra.payment.events.dlx", true, false)

  @Bean
  fun paymentWorkflowQueue() =
      Queue(
          "kra.payment.workflow.v1",
          true,
          false,
          false,
          mapOf("x-dead-letter-exchange" to "kra.payment.events.dlx"),
      )

  @Bean fun paymentWorkflowDlq() = Queue("kra.payment.workflow.v1.dlq", true)

  @Bean
  fun paymentWorkflowBinding(
      @Qualifier("paymentWorkflowQueue") queue: Queue,
      @Qualifier("paymentEventsExchange") paymentEventsExchange: FanoutExchange,
  ) = BindingBuilder.bind(queue).to(paymentEventsExchange)

  @Bean
  fun paymentWorkflowDlqBinding(
      @Qualifier("paymentWorkflowDlq") queue: Queue,
      @Qualifier("paymentDeadLetterExchange") paymentDeadLetterExchange: FanoutExchange,
  ) = BindingBuilder.bind(queue).to(paymentDeadLetterExchange)

  @Bean
  fun paymentRabbitListenerContainerFactory(
      connectionFactory: ConnectionFactory,
      eventPersister: PaymentConsumerEventPersister,
      objectMapper: ObjectMapper,
  ): SimpleRabbitListenerContainerFactory =
      SimpleRabbitListenerContainerFactory().apply {
        setConnectionFactory(connectionFactory)
        setAcknowledgeMode(AcknowledgeMode.AUTO)
        setDefaultRequeueRejected(true)
        setConcurrentConsumers(1)
        setMaxConcurrentConsumers(1)
        setAdviceChain(
            RetryInterceptorBuilder.stateless()
                .maxRetries(3)
                .backOffOptions(250, 2.0, 5_000)
                .recoverer(PaymentDlqRecoverer(eventPersister, objectMapper))
                .build(),
        )
      }
}

private class PaymentDlqRecoverer(
    private val eventPersister: PaymentConsumerEventPersister,
    private val objectMapper: ObjectMapper,
) : MessageRecoverer {
  override fun recover(message: org.springframework.amqp.core.Message, cause: Throwable) {
    paymentConsumerEventIdOrNull(message, objectMapper)?.let { consumerEventId ->
      // If this transaction fails the exception is allowed to propagate; AUTO mode requeues the
      // source message.
      runBlocking { eventPersister.requestDlq(consumerEventId, cause.message) }
    }
    throw AmqpRejectAndDontRequeueException("Payment consumer event exhausted retries", cause)
  }
}

/** Extracts a delivery identifier when a failed broker body is a valid payment event. */
fun paymentConsumerEventIdOrNull(
    message: org.springframework.amqp.core.Message,
    objectMapper: ObjectMapper,
): UUID? = runCatching {
  UUID.fromString(objectMapper.readTree(message.body).required("eventId").asText())
}
    .getOrNull()
