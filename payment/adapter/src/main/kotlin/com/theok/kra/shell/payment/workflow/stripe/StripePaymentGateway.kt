package com.theok.kra.shell.payment.workflow.stripe

import com.stripe.StripeClient
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCancelParams
import com.stripe.param.PaymentIntentCreateParams
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Isolates Stripe's synchronous Java client from workflow orchestration.
 *
 * Production wiring supplies a bounded dispatcher; the default is retained for direct unit-test
 * construction only.
 */
class StripePaymentGateway(
    private val client: StripeClient,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
  suspend fun create(
      params: PaymentIntentCreateParams,
      options: RequestOptions,
  ): PaymentIntent =
      withContext(dispatcher) { client.v1().paymentIntents().create(params, options) }

  suspend fun retrieve(intentId: String): PaymentIntent =
      withContext(dispatcher) { client.v1().paymentIntents().retrieve(intentId) }

  suspend fun cancel(
      intentId: String,
      params: PaymentIntentCancelParams,
      options: RequestOptions,
  ): PaymentIntent =
      withContext(dispatcher) { client.v1().paymentIntents().cancel(intentId, params, options) }
}
