package com.theok.kra.shell.payment.configuration.stripe

import com.stripe.StripeClient
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import java.util.concurrent.Executors
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("payments")
class PaymentStripeConfiguration {
  @Bean
  fun clientSecretCipher(properties: PaymentStripeProperties): ClientSecretCipher =
      ClientSecretCipher(properties.decodedEncryptionKey())

  @Bean
  fun stripeClient(properties: PaymentStripeProperties): StripeClient =
      StripeClient.builder().setApiKey(properties.secretKey).build()

  @Bean(destroyMethod = "close")
  fun stripeBlockingIoDispatcher(properties: PaymentStripeProperties): ExecutorCoroutineDispatcher =
      Executors.newFixedThreadPool(properties.blockingIoConcurrency) { runnable ->
            Thread(runnable, "stripe-payment-io").apply { isDaemon = true }
          }
          .asCoroutineDispatcher()

  @Bean
  fun stripePaymentGateway(
      stripeClient: StripeClient,
      stripeBlockingIoDispatcher: ExecutorCoroutineDispatcher,
  ): StripePaymentGateway = StripePaymentGateway(stripeClient, stripeBlockingIoDispatcher)
}
