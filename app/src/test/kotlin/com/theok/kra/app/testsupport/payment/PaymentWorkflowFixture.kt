package com.theok.kra.app.testsupport.payment

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.infrastructure.RabbitMqFixture
import com.theok.kra.platform.database.json.JooqJsonSupport
import io.grpc.StatusRuntimeException
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

/** PostgreSQL, RabbitMQ, and Temporal services required by the payment worker end-to-end tests. */
internal class PaymentWorkflowFixture : AutoCloseable {
  private val network = Network.newNetwork()

  val postgres =
      PostgreSQLContainer("postgres:18.6").withNetwork(network).withNetworkAliases("postgres")

  private val rabbit = RabbitMqFixture()
  private val temporal = TemporalContainer(network)
  private val objectMapper = JooqJsonSupport.objectMapper()

  fun start() {
    postgres.start()
    PostgresFixture.bootstrap(postgres)
    PostgresFixture.migrate(postgres, "db/changelog/app-master.yaml")
    rabbit.start(network)
    rabbit.provisionFanoutExchange(PAYMENT_EVENTS_EXCHANGE)
    temporal.start()
    awaitTemporalNamespace()
  }

  fun r2dbcUrl(): String = PostgresFixture.r2dbcUrl(postgres)

  fun rabbitHost(): String = rabbit.container.host

  fun rabbitPort(): Int = rabbit.container.getMappedPort(5672)

  fun temporalAddress(): String = "${temporal.host}:${temporal.getMappedPort(7233)}"

  fun publishPaymentConsumerEvent(
      consumerEventId: UUID,
      paymentRequestId: UUID,
      paymentId: UUID?,
      eventType: String,
      stripeEventId: String? = null,
  ) {
    val payload =
        objectMapper
            .createObjectNode()
            .put("eventId", consumerEventId.toString())
            .put("paymentRequestId", paymentRequestId.toString())
            .put("eventType", eventType)
    paymentId?.let { payload.put("paymentId", it.toString()) }
    stripeEventId?.let { payload.put("stripeEventId", it) }
    rabbit.openConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.basicPublish(
            PAYMENT_EVENTS_EXCHANGE,
            "",
            null,
            objectMapper.writeValueAsBytes(payload),
        )
      }
    }
  }

  override fun close() {
    temporal.stop()
    rabbit.container.stop()
    postgres.stop()
    network.close()
  }

  private fun awaitTemporalNamespace() {
    val temporalService =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(temporalAddress()).build(),
        )
    try {
      val deadline = Instant.now().plus(TEMPORAL_NAMESPACE_WAIT_TIMEOUT)
      while (Instant.now().isBefore(deadline)) {
        try {
          temporalService
              .blockingStub()
              .withDeadlineAfter(1, TimeUnit.SECONDS)
              .describeNamespace(
                  DescribeNamespaceRequest.newBuilder().setNamespace(TEMPORAL_NAMESPACE).build(),
              )
          return
        } catch (_: StatusRuntimeException) {
          Thread.sleep(TEMPORAL_NAMESPACE_POLL_INTERVAL.toMillis())
        }
      }
      error("Temporal namespace $TEMPORAL_NAMESPACE was not ready")
    } finally {
      temporalService.shutdown()
    }
  }

  private class TemporalContainer(network: Network) :
      GenericContainer<TemporalContainer>(DockerImageName.parse("temporalio/auto-setup:1.29.7")) {
    init {
      withNetwork(network)
      withNetworkAliases("temporal")
      withExposedPorts(7233)
      withEnv(
          mapOf(
              "DB" to "postgres12",
              "DB_PORT" to "5432",
              "POSTGRES_SEEDS" to "postgres",
              "POSTGRES_USER" to "temporal",
              "POSTGRES_PWD" to PostgresFixture.TEMPORAL_PASSWORD,
              "DBNAME" to "temporal",
              "VISIBILITY_DBNAME" to "temporal_visibility",
              "SKIP_DB_CREATE" to "true",
              "DEFAULT_NAMESPACE" to "kra",
          ),
      )
      waitingFor(Wait.forListeningPort())
      withStartupTimeout(Duration.ofMinutes(2))
    }
  }

  companion object {
    const val PAYMENT_EVENTS_EXCHANGE = "kra.payment.events"
    private const val TEMPORAL_NAMESPACE = "kra"
    private val TEMPORAL_NAMESPACE_WAIT_TIMEOUT = Duration.ofSeconds(30)
    private val TEMPORAL_NAMESPACE_POLL_INTERVAL = Duration.ofMillis(100)
  }
}
