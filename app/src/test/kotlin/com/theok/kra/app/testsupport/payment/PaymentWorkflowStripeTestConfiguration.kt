package com.theok.kra.app.testsupport.payment

import com.stripe.StripeClient
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary

@TestConfiguration(proxyBeanMethods = false)
internal class PaymentWorkflowStripeTestConfiguration {
  @Bean fun paymentWorkflowStripeFake() = PaymentWorkflowStripeFake()

  @Bean
  @Primary
  fun paymentWorkflowStripeClient(fake: PaymentWorkflowStripeFake): StripeClient = fake.client
}
