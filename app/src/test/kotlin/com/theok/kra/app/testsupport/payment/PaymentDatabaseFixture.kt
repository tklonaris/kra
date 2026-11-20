package com.theok.kra.app.testsupport.payment

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.platform.database.json.JooqJsonSupport
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_CONSUMER_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_LIFECYCLE_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_OUTBOX
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_PROVIDER_EVENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST_LIFECYCLE_EVENT
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEvent
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.PaymentEventType
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import java.sql.DriverManager
import java.time.OffsetDateTime
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.testcontainers.postgresql.PostgreSQLContainer
import reactor.core.publisher.Flux

/** Test-only access to persistence guarantees which have no application query contract. */
internal class PaymentDatabaseFixture(
    private val jooq: TransactionalJooqExecutor,
    private val postgres: PostgreSQLContainer,
) {
  suspend fun reset() {
    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(postgres),
            PostgresFixture.MIGRATOR_USER,
            PostgresFixture.MIGRATOR_PASSWORD,
        )
        .use { connection ->
          connection.createStatement().use { statement ->
            statement.executeUpdate("DELETE FROM kra.payment_consumer_event")
            statement.executeUpdate("DELETE FROM kra.payment_webhook_handoff")
            statement.executeUpdate("DELETE FROM kra.payment_outbox")
            statement.executeUpdate("DELETE FROM kra.payment_provider_event")
            statement.executeUpdate("DELETE FROM kra.payment_lifecycle_event")
            statement.executeUpdate("DELETE FROM kra.payment")
            statement.executeUpdate("DELETE FROM kra.payment_request_lifecycle_event")
            statement.executeUpdate("DELETE FROM kra.payment_request")
          }
        }
  }

  suspend fun consumerEventStatus(eventId: UUID): PaymentConsumerEventStatus? =
      jooq.execute { dsl ->
        Flux.from(
                dsl.select(PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS)
                    .from(PAYMENT_CONSUMER_EVENT)
                    .where(PAYMENT_CONSUMER_EVENT.EVENT_ID.eq(eventId)),
            )
            .next()
            .awaitSingleOrNull()
            ?.get(PAYMENT_CONSUMER_EVENT.DELIVERY_STATUS)
            ?.let(PaymentConsumerEventStatus::valueOf)
      }

  suspend fun outboxCount(paymentId: UUID, eventType: String): Int = jooq.execute { dsl ->
    Flux.from(
            dsl.selectCount()
                .from(PAYMENT_OUTBOX)
                .where(PAYMENT_OUTBOX.PAYMENT_ID.eq(paymentId))
                .and(PAYMENT_OUTBOX.EVENT_TYPE.eq(eventType)),
        )
        .map { it.value1() }
        .single()
        .awaitSingle()
  }

  suspend fun lifecycleEventCount(paymentId: UUID, eventType: PaymentLifecycleEventType): Int =
      jooq.execute { dsl ->
        Flux.from(
                dsl.selectCount()
                    .from(PAYMENT_LIFECYCLE_EVENT)
                    .where(PAYMENT_LIFECYCLE_EVENT.PAYMENT_ID.eq(paymentId))
                    .and(PAYMENT_LIFECYCLE_EVENT.EVENT_TYPE.eq(eventType.name)),
            )
            .map { it.value1() }
            .single()
            .awaitSingle()
      }

  suspend fun lifecycleEventCreatedAt(
      paymentId: UUID,
      eventType: PaymentLifecycleEventType,
  ): OffsetDateTime? = jooq.execute { dsl ->
    Flux.from(
            dsl.select(PAYMENT_LIFECYCLE_EVENT.CREATED_AT)
                .from(PAYMENT_LIFECYCLE_EVENT)
                .where(PAYMENT_LIFECYCLE_EVENT.PAYMENT_ID.eq(paymentId))
                .and(PAYMENT_LIFECYCLE_EVENT.EVENT_TYPE.eq(eventType.name)),
        )
        .next()
        .awaitSingleOrNull()
        ?.get(PAYMENT_LIFECYCLE_EVENT.CREATED_AT)
  }

  suspend fun providerEventCount(stripeEventId: String): Int = jooq.execute { dsl ->
    Flux.from(
            dsl.selectCount()
                .from(PAYMENT_PROVIDER_EVENT)
                .where(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID.eq(stripeEventId)),
        )
        .map { it.value1() }
        .single()
        .awaitSingle()
  }

  suspend fun webhookConsumerEvent(paymentId: UUID): PaymentConsumerEvent = jooq.execute { dsl ->
    val row =
        Flux.from(
                dsl.select(
                        PAYMENT_OUTBOX.EVENT_ID,
                        PAYMENT_OUTBOX.PAYLOAD,
                        PAYMENT_OUTBOX.PAYMENT_REQUEST_ID,
                        PAYMENT_OUTBOX.PAYMENT_ID,
                    )
                    .from(PAYMENT_OUTBOX)
                    .where(PAYMENT_OUTBOX.PAYMENT_ID.eq(paymentId))
                    .and(PAYMENT_OUTBOX.EVENT_TYPE.eq("PaymentWebhookReceived")),
            )
            .single()
            .awaitSingle()
    PaymentConsumerEvent(
        consumerEventId = requireNotNull(row.get(PAYMENT_OUTBOX.EVENT_ID)),
        paymentRequestId = requireNotNull(row.get(PAYMENT_OUTBOX.PAYMENT_REQUEST_ID)),
        paymentId = requireNotNull(row.get(PAYMENT_OUTBOX.PAYMENT_ID)),
        eventType = PaymentEventType.WEBHOOK_RECEIVED,
        stripeEventId =
            requireNotNull(row.get(PAYMENT_OUTBOX.PAYLOAD)).required("stripeEventId").asText(),
    )
  }

  suspend fun terminalOutboxCount(paymentId: UUID): Int = jooq.execute { dsl ->
    Flux.from(
            dsl.selectCount()
                .from(PAYMENT_OUTBOX)
                .where(PAYMENT_OUTBOX.PAYMENT_ID.eq(paymentId))
                .and(PAYMENT_OUTBOX.EVENT_TYPE.`in`("PaymentSucceeded", "PaymentCancelled")),
        )
        .map { it.value1() }
        .single()
        .awaitSingle()
  }

  /** Seeds an inconsistent terminal outbox row to verify transactional invariant enforcement. */
  suspend fun insertTerminalOutbox(paymentRequestId: UUID, paymentId: UUID, eventType: String) {
    jooq.execute { dsl ->
      Flux.from(
              dsl.insertInto(PAYMENT_OUTBOX)
                  .set(PAYMENT_OUTBOX.EVENT_ID, UUID.randomUUID())
                  .set(PAYMENT_OUTBOX.AGGREGATE_ID, paymentRequestId)
                  .set(PAYMENT_OUTBOX.AGGREGATE_TYPE, "payment")
                  .set(PAYMENT_OUTBOX.PAYMENT_REQUEST_ID, paymentRequestId)
                  .set(PAYMENT_OUTBOX.PAYMENT_ID, paymentId)
                  .set(PAYMENT_OUTBOX.EVENT_TYPE, eventType)
                  .set(PAYMENT_OUTBOX.PAYLOAD, JooqJsonSupport.objectMapper().createObjectNode()),
          )
          .single()
          .awaitSingle()
    }
  }

  suspend fun stripeEventApplied(stripeEventId: String, stripeStatus: String): Boolean =
      jooq.execute { dsl ->
        Flux.from(
                dsl.select(
                        PAYMENT_PROVIDER_EVENT.APPLIED_AT,
                        PAYMENT_PROVIDER_EVENT.PROVIDER_STATUS,
                    )
                    .from(PAYMENT_PROVIDER_EVENT)
                    .where(PAYMENT_PROVIDER_EVENT.PROVIDER_EVENT_ID.eq(stripeEventId)),
            )
            .next()
            .awaitSingleOrNull()
            ?.let { row ->
              row.get(PAYMENT_PROVIDER_EVENT.APPLIED_AT) != null &&
                  row.get(PAYMENT_PROVIDER_EVENT.PROVIDER_STATUS) == stripeStatus
            } ?: false
      }

  suspend fun paymentRequestOutboxCount(paymentRequestId: UUID, eventType: String): Int =
      jooq.execute { dsl ->
        Flux.from(
                dsl.selectCount()
                    .from(PAYMENT_OUTBOX)
                    .where(PAYMENT_OUTBOX.PAYMENT_REQUEST_ID.eq(paymentRequestId))
                    .and(PAYMENT_OUTBOX.EVENT_TYPE.eq(eventType)),
            )
            .map { it.value1() }
            .single()
            .awaitSingle()
      }

  suspend fun paymentRequestLifecycleEventCount(paymentRequestId: UUID, eventType: String): Int =
      jooq.execute { dsl ->
        Flux.from(
                dsl.selectCount()
                    .from(PAYMENT_REQUEST_LIFECYCLE_EVENT)
                    .where(PAYMENT_REQUEST_LIFECYCLE_EVENT.PAYMENT_REQUEST_ID.eq(paymentRequestId))
                    .and(PAYMENT_REQUEST_LIFECYCLE_EVENT.EVENT_TYPE.eq(eventType)),
            )
            .map { it.value1() }
            .single()
            .awaitSingle()
      }

  suspend fun paymentRequestStatus(paymentRequestId: UUID): String? = jooq.execute { dsl ->
    Flux.from(
            dsl.select(PAYMENT_REQUEST.STATUS)
                .from(PAYMENT_REQUEST)
                .where(PAYMENT_REQUEST.ID.eq(paymentRequestId)),
        )
        .next()
        .awaitSingleOrNull()
        ?.get(PAYMENT_REQUEST.STATUS)
  }

  suspend fun paymentCount(paymentRequestId: UUID): Int = jooq.execute { dsl ->
    Flux.from(
            dsl.selectCount().from(PAYMENT).where(PAYMENT.PAYMENT_REQUEST_ID.eq(paymentRequestId)),
        )
        .map { it.value1() }
        .single()
        .awaitSingle()
  }

  suspend fun elapseCheckoutDeadline(paymentRequestId: UUID) = jooq.execute { dsl ->
    Flux.from(
            dsl.update(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT, OffsetDateTime.now().minusMinutes(1))
                .where(PAYMENT_REQUEST.ID.eq(paymentRequestId)),
        )
        .single()
        .awaitSingle()
  }

  suspend fun elapseProcessingDeadline(paymentRequestId: UUID) = jooq.execute { dsl ->
    Flux.from(
            dsl.update(PAYMENT_REQUEST)
                .set(PAYMENT_REQUEST.PROCESSING_DEADLINE_AT, OffsetDateTime.now().minusMinutes(1))
                .where(PAYMENT_REQUEST.ID.eq(paymentRequestId)),
        )
        .single()
        .awaitSingle()
  }

  suspend fun claimProviderIntentCreation(paymentId: UUID) = jooq.execute { dsl ->
    Flux.from(
            dsl.update(PAYMENT)
                .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "claimed-intent-key")
                .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                .set(PAYMENT.CREATION_ATTEMPTED_AT, OffsetDateTime.now())
                .where(PAYMENT.ID.eq(paymentId)),
        )
        .single()
        .awaitSingle()
  }

  suspend fun paymentRequestLifecycleTransition(
      paymentRequestId: UUID,
      eventType: String,
  ): PaymentRequestLifecycleTransition? = jooq.execute { dsl ->
    Flux.from(
            dsl.select(
                    PAYMENT_REQUEST_LIFECYCLE_EVENT.FROM_STATUS,
                    PAYMENT_REQUEST_LIFECYCLE_EVENT.TO_STATUS,
                    PAYMENT_REQUEST_LIFECYCLE_EVENT.REASON,
                )
                .from(PAYMENT_REQUEST_LIFECYCLE_EVENT)
                .where(PAYMENT_REQUEST_LIFECYCLE_EVENT.PAYMENT_REQUEST_ID.eq(paymentRequestId))
                .and(PAYMENT_REQUEST_LIFECYCLE_EVENT.EVENT_TYPE.eq(eventType)),
        )
        .next()
        .awaitSingleOrNull()
        ?.let {
          PaymentRequestLifecycleTransition(
              it.get(PAYMENT_REQUEST_LIFECYCLE_EVENT.FROM_STATUS),
              it.get(PAYMENT_REQUEST_LIFECYCLE_EVENT.TO_STATUS),
              it.get(PAYMENT_REQUEST_LIFECYCLE_EVENT.REASON),
          )
        }
  }

  data class PaymentRequestLifecycleTransition(
      val fromStatus: String?,
      val toStatus: String?,
      val reason: String?,
  )
}
