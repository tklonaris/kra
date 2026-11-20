package com.theok.kra.app.testsupport.payment

import com.stripe.StripeClient
import com.stripe.exception.ApiException
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCancelParams
import com.stripe.param.PaymentIntentCreateParams
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.mockito.Mockito

/** Deterministic, in-memory Stripe boundary for payment-worker integration tests. */
internal class PaymentWorkflowStripeFake(
    private val afterCreate: ((PaymentIntent) -> Unit)? = null,
) {
  private val stateLock = ReentrantLock()
  private var state = StripeState()

  private val services = Mockito.mock(V1Services::class.java)
  private val intents = Mockito.mock(PaymentIntentService::class.java)
  val client: StripeClient = Mockito.mock(StripeClient::class.java)

  init {
    Mockito.`when`(client.v1()).thenReturn(services)
    Mockito.`when`(services.paymentIntents()).thenReturn(intents)
    Mockito.`when`(
            intents.create(
                Mockito.any(PaymentIntentCreateParams::class.java),
                Mockito.any(RequestOptions::class.java),
            ),
        )
        .thenAnswer { invocation -> create(invocation.getArgument(0), invocation.getArgument(1)) }
    Mockito.`when`(intents.retrieve(Mockito.anyString())).thenAnswer { invocation ->
      retrieve(invocation.getArgument(0))
    }
    Mockito.`when`(
            intents.cancel(
                Mockito.anyString(),
                Mockito.any(PaymentIntentCancelParams::class.java),
                Mockito.any(RequestOptions::class.java),
            ),
        )
        .thenAnswer { invocation ->
          cancel(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2))
        }
  }

  fun reset() {
    stateLock.withLock { state = StripeState() }
    Mockito.clearInvocations(client, services, intents)
  }

  fun markSucceeded(intentId: String) = updateIntent(intentId) { copy(status = "succeeded") }

  fun markProcessing(intentId: String) = updateIntent(intentId) { copy(status = "processing") }

  fun createCount(): Int = stateLock.withLock { state.createdByIdempotencyKey.size }

  fun cancellationCount(): Int = stateLock.withLock { state.cancellationCount }

  fun cancellationRequest(intentId: String): CancellationRequest? = stateLock.withLock {
    state.cancellationsByIdempotencyKey.values.firstOrNull { it.intentId == intentId }
  }

  fun failNextCancellation() {
    stateLock.withLock {
      state = state.copy(cancellationFailuresRemaining = state.cancellationFailuresRemaining + 1)
    }
  }

  fun setAmount(intentId: String, amount: Long) = updateIntent(intentId) { copy(amount = amount) }

  fun setStatus(intentId: String, status: String) = updateIntent(intentId) { copy(status = status) }

  private fun create(params: PaymentIntentCreateParams, options: RequestOptions): PaymentIntent {
    val key = requireNotNull(options.idempotencyKey) { "Stripe create must use an idempotency key" }
    val metadata = params.metadata.orEmpty().toMap()
    val fingerprint =
        CreateParameters(
            params.amount,
            params.currency,
            params.captureMethod,
            params.allowedPaymentMethodTypes.orEmpty().toList(),
            metadata,
        )
    val outcome = stateLock.withLock {
      val current = state
      val existing = current.createdByIdempotencyKey[key]
      if (existing != null) {
        check(existing.parameters == fingerprint) {
          "Stripe idempotency key was reused with different parameters"
        }
        CreateOutcome(requireNotNull(current.intentsById[existing.intentId]), false)
      } else {
        val number = current.nextIntentNumber + 1
        val intent =
            StoredPaymentIntent(
                id = "pi_test_$number",
                clientSecret = "pi_test_${number}_secret_test",
                amount = params.amount,
                currency = params.currency,
                metadata = metadata,
                status = "requires_payment_method",
            )
        state =
            current.copy(
                createdByIdempotencyKey =
                    current.createdByIdempotencyKey +
                        (key to CreatedIntent(fingerprint, intent.id)),
                intentsById = current.intentsById + (intent.id to intent),
                nextIntentNumber = number,
            )
        CreateOutcome(intent, true)
      }
    }
    val response = outcome.intent.toPaymentIntent()
    if (outcome.created) afterCreate?.invoke(response)
    return response
  }

  private fun retrieve(intentId: String): PaymentIntent = stateLock.withLock {
    state.requireIntent(intentId).toPaymentIntent()
  }

  private fun cancel(
      intentId: String,
      params: PaymentIntentCancelParams,
      options: RequestOptions,
  ): PaymentIntent {
    val key =
        requireNotNull(options.idempotencyKey) {
          "Stripe cancellation must use an idempotency key"
        }
    val reason =
        requireNotNull(params.cancellationReason) {
          "Stripe cancellation must include a reason"
        }
    return stateLock.withLock {
      val current = state
      if (current.cancellationFailuresRemaining > 0) {
        state =
            current.copy(cancellationFailuresRemaining = current.cancellationFailuresRemaining - 1)
        throw ApiException("temporary cancellation failure", null, null, 500, null)
      }
      val existing = current.cancellationsByIdempotencyKey[key]
      if (existing != null) {
        check(existing.intentId == intentId && existing.reason == reason) {
          "Stripe cancellation idempotency key was reused with different parameters"
        }
        current.requireIntent(intentId).toPaymentIntent()
      } else {
        val intent = current.requireIntent(intentId)
        val cancelledIntent = intent.copy(status = "canceled", cancellationReason = reason.value)
        state =
            current.copy(
                cancellationsByIdempotencyKey =
                    current.cancellationsByIdempotencyKey +
                        (key to CancellationRequest(intentId, key, reason)),
                intentsById = current.intentsById + (intentId to cancelledIntent),
                cancellationCount = current.cancellationCount + 1,
            )
        cancelledIntent.toPaymentIntent()
      }
    }
  }

  private fun updateIntent(
      intentId: String,
      update: StoredPaymentIntent.() -> StoredPaymentIntent,
  ) {
    stateLock.withLock {
      val current = state
      val intent = current.requireIntent(intentId)
      state = current.copy(intentsById = current.intentsById + (intentId to intent.update()))
    }
  }

  private data class StripeState(
      val createdByIdempotencyKey: Map<String, CreatedIntent> = emptyMap(),
      val cancellationsByIdempotencyKey: Map<String, CancellationRequest> = emptyMap(),
      val intentsById: Map<String, StoredPaymentIntent> = emptyMap(),
      val nextIntentNumber: Long = 0,
      val cancellationCount: Int = 0,
      val cancellationFailuresRemaining: Int = 0,
  ) {
    fun requireIntent(intentId: String): StoredPaymentIntent =
        requireNotNull(intentsById[intentId]) { "Unknown test Stripe PaymentIntent $intentId" }
  }

  private data class CreatedIntent(val parameters: CreateParameters, val intentId: String)

  private data class CreateOutcome(val intent: StoredPaymentIntent, val created: Boolean)

  private data class StoredPaymentIntent(
      val id: String,
      val clientSecret: String,
      val amount: Long?,
      val currency: String?,
      val metadata: Map<String, String>,
      val status: String,
      val cancellationReason: String? = null,
  ) {
    fun toPaymentIntent() =
        PaymentIntent().apply {
          id = this@StoredPaymentIntent.id
          clientSecret = this@StoredPaymentIntent.clientSecret
          amount = this@StoredPaymentIntent.amount
          currency = this@StoredPaymentIntent.currency
          metadata = this@StoredPaymentIntent.metadata
          status = this@StoredPaymentIntent.status
          cancellationReason = this@StoredPaymentIntent.cancellationReason
        }
  }

  data class CancellationRequest(
      val intentId: String,
      val idempotencyKey: String,
      val reason: PaymentIntentCancelParams.CancellationReason,
  )

  private data class CreateParameters(
      val amount: Long?,
      val currency: String?,
      val captureMethod: PaymentIntentCreateParams.CaptureMethod?,
      val allowedPaymentMethodTypes: List<PaymentIntentCreateParams.AllowedPaymentMethodType>,
      val metadata: Map<String, String>,
  )
}
