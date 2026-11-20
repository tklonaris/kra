package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import java.sql.DriverManager
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Verifies least-privilege permissions owned by the payment and shared CDC migrations. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentDatabasePermissionsIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `CDC state role can upsert shared Debezium offsets`() {
    val offsetId = UUID.randomUUID().toString()
    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(fixture.postgres),
            PostgresFixture.CDC_STATE_USER,
            PostgresFixture.CDC_STATE_PASSWORD,
        )
        .use { connection ->
          connection
              .prepareStatement(
                  """
                  INSERT INTO kra.debezium_offset_storage
                      (id, offset_key, offset_val, record_insert_ts, record_insert_seq)
                  VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
                  ON CONFLICT (id) DO UPDATE
                  SET offset_key = EXCLUDED.offset_key,
                      offset_val = EXCLUDED.offset_val,
                      record_insert_ts = EXCLUDED.record_insert_ts,
                      record_insert_seq = EXCLUDED.record_insert_seq
                  """
                      .trimIndent(),
              )
              .use { statement ->
                statement.setString(1, offsetId)
                statement.setString(2, "01")
                statement.setString(3, "02")
                statement.setInt(4, 1)
                assertEquals(1, statement.executeUpdate())

                statement.setString(1, offsetId)
                statement.setString(2, "03")
                statement.setString(3, "04")
                statement.setInt(4, 2)
                assertEquals(1, statement.executeUpdate())
              }
        }
  }

  @Test
  fun `application role cannot mutate immutable payment or request lifecycle events`(): Unit =
      runBlocking {
        val payment = registerPaymentForTest()

        DriverManager.getConnection(
                PostgresFixture.jdbcUrl(fixture.postgres),
                PostgresFixture.APP_USER,
                PostgresFixture.APP_PASSWORD,
            )
            .use { connection ->
              connection
                  .prepareStatement(
                      "UPDATE kra.payment_lifecycle_event SET event_type = ? WHERE payment_id = ?",
                  )
                  .use { statement ->
                    statement.setString(1, PaymentLifecycleEventType.STATUS_CHANGED.name)
                    statement.setObject(2, payment.paymentId)
                    assertFailsWith<Exception> { statement.executeUpdate() }
                  }
              connection
                  .prepareStatement("DELETE FROM kra.payment_lifecycle_event WHERE payment_id = ?")
                  .use { statement ->
                    statement.setObject(1, payment.paymentId)
                    assertFailsWith<Exception> { statement.executeUpdate() }
                  }
              connection
                  .prepareStatement(
                      "UPDATE kra.payment_request_lifecycle_event SET event_type = ? WHERE payment_request_id = ?",
                  )
                  .use { statement ->
                    statement.setString(1, "FAILED")
                    statement.setObject(2, payment.paymentRequestId)
                    assertFailsWith<Exception> { statement.executeUpdate() }
                  }
              connection
                  .prepareStatement(
                      "DELETE FROM kra.payment_request_lifecycle_event WHERE payment_request_id = ?",
                  )
                  .use { statement ->
                    statement.setObject(1, payment.paymentRequestId)
                    assertFailsWith<Exception> { statement.executeUpdate() }
                  }
            }
      }
}
