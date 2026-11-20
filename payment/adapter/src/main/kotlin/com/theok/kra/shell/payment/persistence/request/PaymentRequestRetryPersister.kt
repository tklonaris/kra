package com.theok.kra.shell.payment.persistence.request

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.payment.domain.PaymentRequestRetryResult
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.delivery.appendPaymentOutbox
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

/** Durable, operator-authorised recovery request. The saga still owns all business work. */
@Component
@Profile("payments")
class PaymentRequestRetryPersister(
    private val jooq: TransactionalJooqExecutor,
    private val objectMapper: ObjectMapper,
) {
  suspend fun requestRetry(
      requestId: UUID,
      subject: String,
      reason: String,
  ): PaymentRequestRetryResult = jooq.execute { dsl ->
    val status =
        Flux.from(
                dsl.select(PAYMENT_REQUEST.STATUS)
                    .from(PAYMENT_REQUEST)
                    .where(PAYMENT_REQUEST.ID.eq(requestId))
                    .forUpdate(),
            )
            .next()
            .awaitSingleOrNull()
            ?.get(PAYMENT_REQUEST.STATUS)
    when {
      status == null -> PaymentRequestRetryResult.NOT_FOUND
      !PaymentRequestStatus.valueOf(status).allowsOperationalRetry() ->
          PaymentRequestRetryResult.NOT_ELIGIBLE
      else -> {
        dsl.appendPaymentRequestLifecycleEvent(
            requestId,
            PaymentRequestLifecycleEventType.RETRY_REQUESTED,
            reason = reason,
            initiatedBySubject = subject,
        )
        dsl.appendPaymentOutbox(
            requestId,
            null,
            PaymentEventType.RETRY_REQUESTED,
            objectMapper,
        )
        PaymentRequestRetryResult.ACCEPTED
      }
    }
  }
}
