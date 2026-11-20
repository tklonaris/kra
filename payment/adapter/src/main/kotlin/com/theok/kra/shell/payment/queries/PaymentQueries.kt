package com.theok.kra.shell.payment.queries

import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.payment.dto.PaymentRequestSetup
import com.theok.kra.core.payment.dto.PaymentRequestSnapshot
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.impl.DSL
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
@Profile("payments")
class PaymentQueries(
    private val jooq: TransactionalJooqExecutor,
    private val cipher: ClientSecretCipher,
) {
  suspend fun requestDetails(
      ownerId: PaymentOwnerId,
      paymentRequestId: UUID,
  ): PaymentRequestSnapshot? = jooq.execute { dsl ->
    Flux.from(
            dsl.select(
                    PAYMENT_REQUEST.ID,
                    PAYMENT_REQUEST.PURCHASE_REFERENCE,
                    PAYMENT_REQUEST.AMOUNT_MINOR,
                    PAYMENT_REQUEST.CURRENCY,
                    PAYMENT_REQUEST.STATUS,
                    PAYMENT.ID,
                    PAYMENT.STATUS,
                    PAYMENT.PROVIDER_PAYMENT_ID,
                    PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT,
                    PAYMENT_REQUEST.PROCESSING_DEADLINE_AT,
                    PAYMENT.TERMINAL_REASON,
                    PAYMENT.RECONCILIATION_REQUIRED,
                )
                .from(PAYMENT_REQUEST)
                .leftJoin(PAYMENT)
                .on(PAYMENT.PAYMENT_REQUEST_ID.eq(PAYMENT_REQUEST.ID))
                .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value)),
        )
        .next()
        .awaitSingleOrNull()
        ?.toPaymentRequestSnapshot()
  }

  suspend fun requestSetup(ownerId: PaymentOwnerId, paymentRequestId: UUID): PaymentRequestSetup? =
      jooq.execute { dsl ->
        val databaseNow = DSL.currentOffsetDateTime().`as`("database_now")
        val record =
            Flux.from(
                    dsl.select(
                            PAYMENT.ID,
                            PAYMENT_REQUEST.STATUS,
                            PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT,
                            PAYMENT.STATUS,
                            PAYMENT.CLIENT_SECRET_NONCE,
                            PAYMENT.CLIENT_SECRET_CIPHERTEXT,
                            PAYMENT.CLIENT_SECRET_TAG,
                            databaseNow,
                        )
                        .from(PAYMENT_REQUEST)
                        .leftJoin(PAYMENT)
                        .on(PAYMENT.PAYMENT_REQUEST_ID.eq(PAYMENT_REQUEST.ID))
                        .where(PAYMENT_REQUEST.ID.eq(paymentRequestId))
                        .and(PAYMENT_REQUEST.OWNER_ID.eq(ownerId.value)),
                )
                .next()
                .awaitSingleOrNull()

        record?.toSetup(requireNotNull(record.get(databaseNow)).toInstant(), cipher)
      }
}
