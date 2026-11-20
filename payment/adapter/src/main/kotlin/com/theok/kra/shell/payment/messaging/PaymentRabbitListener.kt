package com.theok.kra.shell.payment.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEvent
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventPersister
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.springframework.amqp.AmqpRejectAndDontRequeueException
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("payments")
@ConditionalOnProperty("payments.worker.enabled", havingValue = "true")
class PaymentRabbitListener(
    private val objectMapper: ObjectMapper,
    private val eventPersister: PaymentConsumerEventPersister,
    private val dispatcher: PaymentWorkflowDispatcher,
) {
  @RabbitListener(
      queues = ["kra.payment.workflow.v1"],
      containerFactory = "paymentRabbitListenerContainerFactory",
  )
  fun consume(body: String) {
    runBlocking {
      val event = parse(body)
      when (eventPersister.receive(event)) {
        PaymentConsumerEventStatus.ACCEPTED,
        PaymentConsumerEventStatus.SKIPPED -> Unit
        PaymentConsumerEventStatus.DLQ_REQUESTED ->
            throw AmqpRejectAndDontRequeueException(
                "Payment consumer event already requested for DLQ",
            )
        PaymentConsumerEventStatus.PENDING ->
            if (event.eventType.wakesSaga) {
              dispatcher.dispatch(event)
              eventPersister.accepted(event.consumerEventId)
            } else {
              eventPersister.skipped(event.consumerEventId)
            }
      }
    }
  }

  private fun parse(body: String): PaymentConsumerEvent {
    val json = objectMapper.readTree(body)
    return PaymentConsumerEvent(
        UUID.fromString(json.required("eventId").asText()),
        UUID.fromString(json.required("paymentRequestId").asText()),
        json
            .path("paymentId")
            .takeIf { !it.isMissingNode && !it.isNull }
            ?.asText()
            ?.let(UUID::fromString),
        PaymentEventType.fromEventName(json.required("eventType").asText()),
        json.path("stripeEventId").takeIf { !it.isMissingNode && !it.isNull }?.asText(),
    )
  }
}
