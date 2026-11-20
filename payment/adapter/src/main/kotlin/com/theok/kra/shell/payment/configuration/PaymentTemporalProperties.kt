package com.theok.kra.shell.payment.configuration

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("payments.temporal")
data class PaymentTemporalProperties(
    val taskQueue: String,
    val checkoutWindow: Duration = Duration.ofMinutes(30),
    val processingWindow: Duration = Duration.ofMinutes(60),
    val reconciliationInterval: Duration = Duration.ofMinutes(2),
    val activityStartToCloseTimeout: Duration = Duration.ofMinutes(2),
    val activityScheduleToCloseTimeout: Duration = Duration.ofMinutes(10),
    val activityMaximumAttempts: Int = 3,
) {
  init {
    require(taskQueue.isNotBlank()) { "Payment Temporal task queue must not be blank" }
    require(!checkoutWindow.isNegative && !checkoutWindow.isZero) {
      "Checkout window must be positive"
    }
    require(!processingWindow.isNegative && !processingWindow.isZero) {
      "Processing window must be positive"
    }
    require(!reconciliationInterval.isNegative && !reconciliationInterval.isZero) {
      "Reconciliation interval must be positive"
    }
    require(!activityStartToCloseTimeout.isNegative && !activityStartToCloseTimeout.isZero) {
      "Activity start-to-close timeout must be positive"
    }
    require(activityScheduleToCloseTimeout >= activityStartToCloseTimeout) {
      "Activity schedule-to-close timeout must not be shorter than one attempt"
    }
    require(activityMaximumAttempts > 0) { "Activity maximum attempts must be positive" }
  }
}
