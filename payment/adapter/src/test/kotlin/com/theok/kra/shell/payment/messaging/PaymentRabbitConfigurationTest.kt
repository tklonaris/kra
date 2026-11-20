package com.theok.kra.shell.payment.messaging

import com.theok.kra.platform.database.json.JooqJsonSupport
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.springframework.amqp.core.Message

class PaymentRabbitConfigurationTest {
  private val objectMapper = JooqJsonSupport.objectMapper()

  @Test
  fun `extracts the consumer event id from a valid payment event`() {
    val consumerEventId = UUID.randomUUID()
    val message =
        Message(
            objectMapper
                .createObjectNode()
                .put("eventId", consumerEventId.toString())
                .toString()
                .encodeToByteArray(),
        )

    assertEquals(consumerEventId, paymentConsumerEventIdOrNull(message, objectMapper))
  }

  @Test
  fun `does not throw when a broker message is malformed`() {
    val message = Message("not-json".encodeToByteArray())

    assertNull(paymentConsumerEventIdOrNull(message, objectMapper))
  }
}
