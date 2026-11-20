package com.theok.kra.shell.payment.persistence.coordination

import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.impl.DSL
import reactor.core.publisher.Flux

/** Transaction-scoped workflow projection loading. */
internal class PaymentWorkflowLoader(private val dsl: DSLContext) {
  private val databaseNow = DSL.currentOffsetDateTime().`as`("database_now")

  suspend fun load(paymentId: UUID): PaymentWorkflowState? = load(PAYMENT.ID.eq(paymentId))

  suspend fun loadByRequestId(paymentRequestId: UUID): PaymentWorkflowState? =
      load(PAYMENT.PAYMENT_REQUEST_ID.eq(paymentRequestId))

  private suspend fun load(condition: Condition): PaymentWorkflowState? =
      Flux.from(
              dsl.select(PAYMENT.asterisk(), PAYMENT_REQUEST.asterisk(), databaseNow)
                  .from(PAYMENT)
                  .join(PAYMENT_REQUEST)
                  .on(PAYMENT_REQUEST.ID.eq(PAYMENT.PAYMENT_REQUEST_ID))
                  .where(condition),
          )
          .next()
          .awaitSingleOrNull()
          ?.toPaymentWorkflowState(databaseNow)
}
