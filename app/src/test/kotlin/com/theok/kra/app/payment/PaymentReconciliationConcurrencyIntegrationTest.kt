package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import java.sql.DriverManager
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import reactor.core.publisher.Flux

/**
 * Verifies the request-first fence around a reconciliation decision racing payment registration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentReconciliationConcurrencyIntegrationTest :
    PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `reconciliation observes a payment registered while it waits on the request lock`() =
      runBlocking {
        val request = acceptPaymentRequest()
        val paymentId = UUID.randomUUID()
        val lockHeld = CompletableDeferred<Unit>()
        val registerPayment = CompletableDeferred<Unit>()
        val registration =
            async(Dispatchers.IO) {
              DriverManager.getConnection(
                      PostgresFixture.jdbcUrl(fixture.postgres),
                      PostgresFixture.MIGRATOR_USER,
                      PostgresFixture.MIGRATOR_PASSWORD,
                  )
                  .use { connection ->
                    connection.autoCommit = false
                    try {
                      connection
                          .prepareStatement(
                              "SELECT 1 FROM kra.payment_request WHERE id = ? FOR UPDATE",
                          )
                          .use { statement ->
                            statement.setObject(1, request.paymentRequestId)
                            statement.executeQuery()
                          }
                      lockHeld.complete(Unit)
                      registerPayment.await()
                      connection
                          .prepareStatement(
                              "INSERT INTO kra.payment (id, payment_request_id, status, intent_creation_state) VALUES (?, ?, 'REQUESTED', 'NOT_STARTED')",
                          )
                          .use { statement ->
                            statement.setObject(1, paymentId)
                            statement.setObject(2, request.paymentRequestId)
                            statement.executeUpdate()
                          }
                      connection
                          .prepareStatement(
                              "UPDATE kra.payment_request SET status = 'CREATING_PROVIDER_INTENT' WHERE id = ?",
                          )
                          .use { statement ->
                            statement.setObject(1, request.paymentRequestId)
                            statement.executeUpdate()
                          }
                      connection.commit()
                    } catch (failure: Exception) {
                      connection.rollback()
                      throw failure
                    }
                  }
            }
        lockHeld.await()
        val reconciliation =
            async(Dispatchers.Default) {
              workflowOperations(PaymentWorkflowStripeFake().client)
                  .requireReconciliation(request.paymentRequestId, "temporary activity failure")
            }
        delay(200)
        registerPayment.complete(Unit)
        registration.await()
        reconciliation.await()

        assertEquals(
            true,
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(PAYMENT.RECONCILIATION_REQUIRED)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(paymentId)),
                  )
                  .single()
                  .awaitSingle()
                  .get(PAYMENT.RECONCILIATION_REQUIRED)
            },
        )
        assertEquals(
            "RECONCILING_PROVIDER_STATE",
            fixture.database.paymentRequestStatus(request.paymentRequestId),
        )
      }
}
