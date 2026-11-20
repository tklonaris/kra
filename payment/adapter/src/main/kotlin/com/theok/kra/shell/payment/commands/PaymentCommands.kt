package com.theok.kra.shell.payment.commands

import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.payment.domain.PaymentRequestRetryResult
import com.theok.kra.core.payment.domain.PaymentSpec
import com.theok.kra.core.payment.dto.PaymentRequestAcceptance
import com.theok.kra.shell.payment.persistence.request.PaymentRequestCancellationPersister
import com.theok.kra.shell.payment.persistence.request.PaymentRequestCancellationStatus
import com.theok.kra.shell.payment.persistence.request.PaymentRequestPersistenceConflict
import com.theok.kra.shell.payment.persistence.request.PaymentRequestPersister
import com.theok.kra.shell.payment.persistence.request.PaymentRequestRetryPersister
import java.util.UUID
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
@Profile("payments")
class PaymentCommands(
    private val requestPersister: PaymentRequestPersister,
    private val requestCancellationPersister: PaymentRequestCancellationPersister,
    private val retryPersister: PaymentRequestRetryPersister,
) {
  data class SubmitRequestSpec(
      val ownerId: PaymentOwnerId,
      val requestId: UUID,
      val purchaseReference: UUID,
      val amountMinor: Long,
      val currency: String,
  )

  @Transactional
  suspend fun submitRequest(spec: SubmitRequestSpec): SubmitPaymentRequestResult =
      try {
        SubmitPaymentRequestResult.Accepted(
            requestPersister.accept(
                spec.requestId,
                spec.ownerId,
                PaymentSpec(spec.purchaseReference, spec.amountMinor, spec.currency),
            ),
        )
      } catch (_: PaymentRequestPersistenceConflict) {
        SubmitPaymentRequestResult.Conflict
      } catch (invalidSpec: IllegalArgumentException) {
        SubmitPaymentRequestResult.Invalid(requireNotNull(invalidSpec.message))
      }

  @Transactional
  suspend fun cancel(ownerId: PaymentOwnerId, paymentRequestId: UUID): CancelPaymentResult =
      when (requestCancellationPersister.requestCancellation(ownerId, paymentRequestId)) {
        PaymentRequestCancellationStatus.REQUESTED -> CancelPaymentResult.Requested
        PaymentRequestCancellationStatus.ALREADY_REQUESTED -> CancelPaymentResult.AlreadyRequested
        PaymentRequestCancellationStatus.TERMINAL -> CancelPaymentResult.Terminal
        PaymentRequestCancellationStatus.NOT_FOUND -> CancelPaymentResult.NotFound
      }

  @Transactional
  suspend fun retry(requestId: UUID, subject: String, reason: String): PaymentRequestRetryResult =
      retryPersister.requestRetry(requestId, subject, reason)
}

sealed interface SubmitPaymentRequestResult {
  data class Accepted(val paymentRequest: PaymentRequestAcceptance) : SubmitPaymentRequestResult

  data object Conflict : SubmitPaymentRequestResult

  data class Invalid(val message: String) : SubmitPaymentRequestResult
}

sealed interface CancelPaymentResult {
  data object Requested : CancelPaymentResult

  data object AlreadyRequested : CancelPaymentResult

  data object Terminal : CancelPaymentResult

  data object NotFound : CancelPaymentResult
}
