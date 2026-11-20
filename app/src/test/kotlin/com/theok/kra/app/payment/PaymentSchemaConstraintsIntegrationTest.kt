package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import java.sql.DriverManager
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import reactor.core.publisher.Flux

/** Verifies PostgreSQL constraints and least-privilege grants owned by payment migrations. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentSchemaConstraintsIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `database rejects contradictory payment intent states`() = runBlocking {
    val payment = registerPaymentForTest()

    val knownWithoutIntentRejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.INTENT_CREATION_STATE, "KNOWN")
                    .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                    .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "known-intent-key")
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure
    val intentWithoutKnownStateRejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.PROVIDER_PAYMENT_ID, "pi_test")
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure
    val awaitingWithoutSetupRejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.STATUS, "AWAITING_CUSTOMER")
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure
    val processingWithoutKnownIntentRejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.STATUS, "PROCESSING")
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure
    val succeededWithoutKnownIntentRejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.STATUS, "SUCCEEDED")
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure

    assertTrue(knownWithoutIntentRejected)
    assertTrue(intentWithoutKnownStateRejected)
    assertTrue(awaitingWithoutSetupRejected)
    assertTrue(processingWithoutKnownIntentRejected)
    assertTrue(succeededWithoutKnownIntentRejected)
  }

  @Test
  fun `database rejects a partial encrypted client secret`() = runBlocking {
    val payment = registerPaymentForTest()

    val rejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.CLIENT_SECRET_NONCE, byteArrayOf(1))
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure

    assertTrue(rejected)
  }

  @Test
  fun `database requires durable recovery facts for uncertain creation`() = runBlocking {
    val payment = registerPaymentForTest()

    val rejected = runCatching {
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.update(PAYMENT)
                    .set(PAYMENT.INTENT_CREATION_STATE, "UNCERTAIN")
                    .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                    .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "uncertain-key")
                    .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                    .set(PAYMENT.CREATION_ATTEMPTED_AT, OffsetDateTime.now(ZoneOffset.UTC))
                    .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                    .where(PAYMENT.ID.eq(payment.paymentId)),
            )
            .single()
            .awaitSingle()
        Unit
      }
    }
        .isFailure

    assertTrue(rejected)
  }

  @Test
  fun `database rejects terminal reconciliation states that conflict with their payment outcome`() =
      runBlocking {
        val payment = registerPaymentWithKnownIntent()

        val succeededWithReconciliationRejected = runCatching {
          fixture.jooq.execute { dsl ->
            Flux.from(
                    dsl.update(PAYMENT)
                        .set(PAYMENT.STATUS, "SUCCEEDED")
                        .set(PAYMENT.RECONCILIATION_REQUIRED, true)
                        .set(PAYMENT.RECONCILIATION_REASON, "must not survive success")
                        .where(PAYMENT.ID.eq(payment.paymentId)),
                )
                .single()
                .awaitSingle()
            Unit
          }
        }
            .isFailure
        val cancelledWithoutRequiredReconciliationRejected = runCatching {
          fixture.jooq.execute { dsl ->
            Flux.from(
                    dsl.update(PAYMENT)
                        .set(PAYMENT.STATUS, "CANCELLED")
                        .set(PAYMENT.TERMINAL_REASON, "CAUSE_UNDETERMINED")
                        .set(PAYMENT.RECONCILIATION_REQUIRED, false)
                        .set(PAYMENT.RECONCILIATION_REASON, null as String?)
                        .where(PAYMENT.ID.eq(payment.paymentId)),
                )
                .single()
                .awaitSingle()
            Unit
          }
        }
            .isFailure

        assertTrue(succeededWithReconciliationRejected)
        assertTrue(cancelledWithoutRequiredReconciliationRejected)
      }

  @Test
  fun `database rejects a provider event for a missing payment`() {
    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(fixture.postgres),
            PostgresFixture.APP_USER,
            PostgresFixture.APP_PASSWORD,
        )
        .use { connection ->
          connection
              .prepareStatement(
                  """
                  INSERT INTO kra.payment_provider_event
                      (provider_event_id, payment_id, provider_payment_id, event_type)
                  VALUES (?, ?, ?, ?)
                  """
                      .trimIndent(),
              )
              .use { statement ->
                statement.setString(1, "evt_test_${UUID.randomUUID()}")
                statement.setObject(2, UUID.randomUUID())
                statement.setString(3, "pi_test_${UUID.randomUUID()}")
                statement.setString(4, "payment_intent.succeeded")
                assertFailsWith<Exception> { statement.executeUpdate() }
              }
        }
  }

  @Test
  fun `database rejects a webhook delivery paired with another payment`() = runBlocking {
    val providerPayment = registerPaymentForTest()
    val consumerPayment = registerPaymentForTest()
    val stripeEventId = "evt_test_${UUID.randomUUID()}"

    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(fixture.postgres),
            PostgresFixture.APP_USER,
            PostgresFixture.APP_PASSWORD,
        )
        .use { connection ->
          connection
              .prepareStatement(
                  """
                  INSERT INTO kra.payment_provider_event
                      (provider_event_id, payment_id, provider_payment_id, event_type)
                  VALUES (?, ?, ?, ?)
                  """
                      .trimIndent(),
              )
              .use { statement ->
                statement.setString(1, stripeEventId)
                statement.setObject(2, providerPayment.paymentId)
                statement.setString(3, "pi_test_${UUID.randomUUID()}")
                statement.setString(4, "payment_intent.succeeded")
                assertEquals(1, statement.executeUpdate())
              }
          connection
              .prepareStatement(
                  """
                  INSERT INTO kra.payment_consumer_event
                      (event_id, delivery_status, attempts, payment_request_id, payment_id, event_type, provider_event_id)
                  VALUES (?, 'PENDING', 1, ?, ?, 'PaymentWebhookReceived', ?)
                  """
                      .trimIndent(),
              )
              .use { statement ->
                statement.setObject(1, UUID.randomUUID())
                statement.setObject(2, consumerPayment.paymentRequestId)
                statement.setObject(3, consumerPayment.paymentId)
                statement.setString(4, stripeEventId)
                assertFailsWith<Exception> { statement.executeUpdate() }
              }
        }
  }
}
