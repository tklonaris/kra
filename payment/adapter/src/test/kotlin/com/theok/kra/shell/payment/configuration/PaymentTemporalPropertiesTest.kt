package com.theok.kra.shell.payment.configuration

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertFailsWith

class PaymentTemporalPropertiesTest {
  @Test
  fun `requires a task queue and positive processing window`() {
    assertFailsWith<IllegalArgumentException> {
      properties(taskQueue = " ")
    }
    assertFailsWith<IllegalArgumentException> {
      properties(processingWindow = Duration.ZERO)
    }
    assertFailsWith<IllegalArgumentException> {
      properties(reconciliationInterval = Duration.ZERO)
    }
  }

  private fun properties(
      taskQueue: String = "payment-test",
      processingWindow: Duration = Duration.ofMinutes(60),
      reconciliationInterval: Duration = Duration.ofMinutes(2),
  ) =
      PaymentTemporalProperties(
          taskQueue = taskQueue,
          checkoutWindow = Duration.ofMinutes(30),
          processingWindow = processingWindow,
          reconciliationInterval = reconciliationInterval,
          activityStartToCloseTimeout = Duration.ofMinutes(2),
          activityScheduleToCloseTimeout = Duration.ofMinutes(10),
      )
}
