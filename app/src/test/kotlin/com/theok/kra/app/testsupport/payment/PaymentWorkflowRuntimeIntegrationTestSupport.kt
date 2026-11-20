package com.theok.kra.app.testsupport.payment

import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.infrastructure.RabbitMqFixture
import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.dto.PaymentRequestAcceptance
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.commands.CancelPaymentResult
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.commands.PaymentWebhookCommands
import com.theok.kra.shell.payment.commands.SubmitPaymentRequestResult
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventStatus
import com.theok.kra.shell.payment.persistence.delivery.WebhookAcceptance
import com.theok.kra.shell.payment.queries.PaymentQueries
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.WorkflowClient
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.DockerClientFactory

/**
 * Shared application lifecycle and eventual-consistency assertions for broker integration tests.
 */
internal abstract class PaymentWorkflowRuntimeIntegrationTestSupport {
  private val paymentOwner = PaymentOwnerId("workflow-test-owner")
  protected val fixture = PaymentWorkflowFixture()
  private val paymentRequestIds = linkedSetOf<UUID>()
  private lateinit var context: ConfigurableApplicationContext
  private lateinit var commands: PaymentCommands
  private lateinit var webhookCommands: PaymentWebhookCommands
  private lateinit var queries: PaymentQueries
  private lateinit var database: PaymentDatabaseFixture
  protected lateinit var stripe: PaymentWorkflowStripeFake
  private lateinit var temporal: WorkflowClient

  @BeforeAll
  fun startApplication() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL, RabbitMQ, and Temporal Testcontainers",
    )
    fixture.start()
    context =
        SpringApplicationBuilder(
                KotlinReferenceArchitectureApplication::class.java,
                PaymentWorkflowStripeTestConfiguration::class.java,
            )
            .profiles("payments", "test")
            .web(WebApplicationType.NONE)
            .run(
                "--spring.r2dbc.url=${fixture.r2dbcUrl()}",
                "--spring.r2dbc.username=${PostgresFixture.APP_USER}",
                "--spring.r2dbc.password=${PostgresFixture.APP_PASSWORD}",
                "--spring.r2dbc.pool.initial-size=0",
                "--spring.liquibase.enabled=false",
                "--spring.rabbitmq.host=${fixture.rabbitHost()}",
                "--spring.rabbitmq.port=${fixture.rabbitPort()}",
                "--spring.rabbitmq.username=${RabbitMqFixture.USER}",
                "--spring.rabbitmq.password=${RabbitMqFixture.PASSWORD}",
                "--temporal.address=${fixture.temporalAddress()}",
                "--temporal.namespace=kra",
                "--payments.stripe.secret-key=sk_test_unused",
                "--payments.stripe.webhook-secret=whsec_test_unused",
                "--payments.stripe.client-secret-encryption-key=${Base64.getEncoder().encodeToString(ByteArray(32))}",
                "--payments.worker.enabled=true",
                "--payments.api-access.mode=trusted-demo",
            )
    commands = context.getBean(PaymentCommands::class.java)
    webhookCommands = context.getBean(PaymentWebhookCommands::class.java)
    queries = context.getBean(PaymentQueries::class.java)
    database =
        PaymentDatabaseFixture(
            context.getBean(TransactionalJooqExecutor::class.java),
            fixture.postgres,
        )
    stripe = context.getBean(PaymentWorkflowStripeFake::class.java)
    temporal = context.getBean(WorkflowClient::class.java)
  }

  @BeforeEach fun resetFixture() = runBlocking { reset() }

  @AfterEach fun cleanFixture() = runBlocking { reset() }

  @AfterAll
  fun stopApplication() {
    if (::context.isInitialized) context.close()
    fixture.close()
  }

  protected suspend fun acceptAndStartPaymentRequest(): StartedPaymentRequest {
    val acceptedRequest = acceptPaymentRequest()
    val eventId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        eventId,
        acceptedRequest.paymentRequestId,
        null,
        "PaymentRequestAccepted",
    )
    awaitPaymentConsumerEventStatus(eventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(acceptedRequest.paymentRequestId, PaymentStatus.AWAITING_CUSTOMER)
    val paymentId =
        requireNotNull(
                queries.requestDetails(paymentOwner, acceptedRequest.paymentRequestId)?.payment,
            )
            .paymentId
    return StartedPaymentRequest(acceptedRequest.paymentRequestId, paymentId)
  }

  protected suspend fun cancelAndAwaitTerminal(payment: StartedPaymentRequest) {
    requestCancellation(payment.paymentRequestId)
    val eventId = UUID.randomUUID()
    fixture.publishPaymentConsumerEvent(
        eventId,
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentCancellationRequested",
    )
    awaitPaymentConsumerEventStatus(eventId, PaymentConsumerEventStatus.ACCEPTED)
    awaitPaymentStatus(payment.paymentRequestId, PaymentStatus.CANCELLED)
    awaitWorkflowClosed(payment.paymentRequestId)
  }

  protected suspend fun requestCancellation(paymentRequestId: UUID) {
    assertEquals(CancelPaymentResult.Requested, commands.cancel(paymentOwner, paymentRequestId))
  }

  protected suspend fun acceptPaymentRequest(): PaymentRequestAcceptance {
    val result =
        commands.submitRequest(
            PaymentCommands.SubmitRequestSpec(
                paymentOwner,
                UUID.randomUUID(),
                UUID.randomUUID(),
                100,
                "AUD",
            ),
        )
    val acceptedRequest =
        (result as? SubmitPaymentRequestResult.Accepted)?.paymentRequest
            ?: error("Expected payment request to be accepted: $result")
    paymentRequestIds += acceptedRequest.paymentRequestId
    return acceptedRequest
  }

  protected suspend fun recordSucceededStripeEvent(
      stripeEventId: String,
      paymentId: UUID,
      intentId: String,
  ) {
    val webhook =
        PaymentWebhookTestSupport.paymentIntentSucceeded(stripeEventId, paymentId, intentId)
    assertEquals(
        WebhookAcceptance.Accepted,
        webhookCommands.accept(webhook.body, webhook.signature),
    )
  }

  protected suspend fun awaitPaymentConsumerEventStatus(
      eventId: UUID,
      status: PaymentConsumerEventStatus,
  ) {
    await("payment consumer event $eventId to become $status") {
      paymentConsumerEventStatus(eventId) == status
    }
  }

  protected suspend fun awaitPaymentStatus(paymentRequestId: UUID, status: PaymentStatus) {
    await("payment request $paymentRequestId to reach payment status $status") {
      paymentStatus(paymentRequestId) == status
    }
  }

  protected suspend fun awaitStripeEventApplied(stripeEventId: String, stripeStatus: String) {
    await("Stripe event $stripeEventId to be applied") {
      database.stripeEventApplied(stripeEventId, stripeStatus)
    }
  }

  protected suspend fun awaitWorkflowClosed(paymentRequestId: UUID) {
    await("Temporal workflow for payment request $paymentRequestId to close") {
      workflowStatus(paymentRequestId) ==
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
    }
  }

  protected fun workflowStatus(paymentRequestId: UUID): WorkflowExecutionStatus? = runCatching {
    temporal.newUntypedWorkflowStub(workflowId(paymentRequestId)).describe().status
  }
      .getOrNull()

  protected suspend fun paymentStatus(paymentRequestId: UUID): PaymentStatus? =
      queries.requestDetails(paymentOwner, paymentRequestId)?.payment?.status

  protected suspend fun paymentIntentId(paymentRequestId: UUID): String? =
      queries.requestDetails(paymentOwner, paymentRequestId)?.payment?.stripePaymentIntentId

  protected suspend fun hasAvailableSetup(paymentRequestId: UUID): Boolean =
      queries.requestSetup(paymentOwner, paymentRequestId)?.clientSecret != null

  protected suspend fun paymentConsumerEventStatus(eventId: UUID): PaymentConsumerEventStatus? =
      database.consumerEventStatus(eventId)

  protected suspend fun outboxCount(paymentId: UUID, eventType: String): Int =
      database.outboxCount(paymentId, eventType)

  protected suspend fun terminalOutboxCount(paymentId: UUID): Int =
      database.terminalOutboxCount(paymentId)

  protected fun stripeEventId() = "evt_test_${UUID.randomUUID()}"

  protected suspend fun await(description: String, condition: suspend () -> Boolean) {
    val deadline = Instant.now().plus(WAIT_TIMEOUT)
    while (Instant.now().isBefore(deadline)) {
      if (condition()) return
      delay(POLL_INTERVAL.toMillis())
    }
    assertTrue(condition(), "Timed out waiting for $description")
  }

  private suspend fun reset() {
    terminateUnfinishedWorkflows()
    database.reset()
    stripe.reset()
  }

  private suspend fun terminateUnfinishedWorkflows() {
    paymentRequestIds.toList().forEach { paymentRequestId ->
      val workflow = temporal.newUntypedWorkflowStub(workflowId(paymentRequestId))
      if (
          workflowStatus(paymentRequestId) ==
              WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
      ) {
        workflow.terminate("test fixture cleanup")
        await("Temporal workflow for payment request $paymentRequestId to terminate") {
          workflowStatus(paymentRequestId) !=
              WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        }
      }
    }
    paymentRequestIds.clear()
  }

  private fun workflowId(paymentRequestId: UUID) = "payment-request-$paymentRequestId"

  private companion object {
    val WAIT_TIMEOUT: Duration = Duration.ofSeconds(45)
    val POLL_INTERVAL: Duration = Duration.ofMillis(100)
  }

  protected data class StartedPaymentRequest(
      val paymentRequestId: UUID,
      val paymentId: UUID,
  )
}
