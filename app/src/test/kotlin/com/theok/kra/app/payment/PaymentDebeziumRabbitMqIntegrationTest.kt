package com.theok.kra.app.payment

import com.rabbitmq.client.Connection
import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.app.testsupport.infrastructure.DebeziumFixture
import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.infrastructure.RabbitMqFixture
import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.core.voucher.dto.VoucherDetails
import com.theok.kra.platform.database.json.JooqJsonSupport
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.commands.SubmitPaymentRequestResult
import com.theok.kra.shell.voucher.commands.VoucherCreationCommands
import com.theok.kra.shell.voucher.commands.VoucherRedemptionCommands
import java.time.Duration
import java.util.Base64
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.Network
import org.testcontainers.postgresql.PostgreSQLContainer

/**
 * Verifies the shared transactional-outbox connector routes payment events independently of the
 * worker and alongside voucher events.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PaymentDebeziumRabbitMqIntegrationTest {
  private val network = Network.newNetwork()
  private val postgres =
      PostgreSQLContainer("postgres:18.6")
          .withNetwork(network)
          .withNetworkAliases("postgres")
          .withCommand(
              "postgres",
              "-c",
              "wal_level=logical",
              "-c",
              "max_replication_slots=4",
              "-c",
              "max_wal_senders=4",
          )
  private val rabbit = RabbitMqFixture()
  private val objectMapper = JooqJsonSupport.objectMapper()
  private lateinit var context: ConfigurableApplicationContext
  private lateinit var connection: Connection
  private lateinit var commands: PaymentCommands
  private lateinit var voucherCreationCommands: VoucherCreationCommands
  private lateinit var voucherRedemptionCommands: VoucherRedemptionCommands

  @BeforeAll
  fun startInfrastructure() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for payment Debezium/RabbitMQ integration",
    )
    postgres.start()
    rabbit.start(network)
    rabbit.provisionFanoutExchange(PAYMENT_EXCHANGE)
    rabbit.provisionFanoutExchange(VOUCHER_EXCHANGE)
    PostgresFixture.bootstrap(postgres)
    PostgresFixture.migrate(postgres, "db/changelog/app-master.yaml")
    connection = rabbit.openConnection()
    context =
        SpringApplicationBuilder(KotlinReferenceArchitectureApplication::class.java)
            .profiles("payments", "test")
            .web(WebApplicationType.NONE)
            .run(
                "--spring.r2dbc.url=${PostgresFixture.r2dbcUrl(postgres)}",
                "--spring.r2dbc.username=${PostgresFixture.APP_USER}",
                "--spring.r2dbc.password=${PostgresFixture.APP_PASSWORD}",
                "--spring.r2dbc.pool.initial-size=0",
                "--spring.liquibase.enabled=false",
                "--payments.worker.enabled=false",
                "--payments.api-access.mode=trusted-demo",
                "--payments.stripe.secret-key=sk_test_unused",
                "--payments.stripe.webhook-secret=whsec_test_unused",
                "--payments.stripe.client-secret-encryption-key=${Base64.getEncoder().encodeToString(ByteArray(32))}",
            )
    commands = context.getBean(PaymentCommands::class.java)
    voucherCreationCommands = context.getBean(VoucherCreationCommands::class.java)
    voucherRedemptionCommands = context.getBean(VoucherRedemptionCommands::class.java)
  }

  @AfterAll
  fun stopInfrastructure() {
    if (::context.isInitialized) context.close()
    if (::connection.isInitialized) connection.close()
    rabbit.container.stop()
    postgres.stop()
    network.close()
  }

  @Test
  fun `one shared Debezium connector routes feature events and resumes from retained offsets`() =
      runBlocking {
        val paymentQueue = "payment-outbox-${UUID.randomUUID()}"
        val voucherQueue = "voucher-outbox-${UUID.randomUUID()}"
        rabbit.declareBoundQueue(connection, paymentQueue, PAYMENT_EXCHANGE)
        rabbit.declareBoundQueue(connection, voucherQueue, VOUCHER_EXCHANGE)
        var debezium = DebeziumFixture().withNetwork(network).withNetworkAliases("debezium")
        debezium.start()
        try {
          try {
            debezium.awaitReplicationStream(postgres, TIMEOUT)
          } catch (failure: IllegalStateException) {
            error("${failure.message}\n${debezium.logs}")
          }
          val first = acceptPaymentRequest()
          assertRequestAccepted(
              receive(paymentQueue, first.paymentRequestId),
              first.paymentRequestId,
          )
          val voucher = redeemVoucher()
          assertVoucherRedeemed(receive(voucherQueue, voucher.requestId), voucher)
          try {
            debezium.awaitOffset(postgres, TIMEOUT)
          } catch (failure: IllegalStateException) {
            error("${failure.message}\n${debezium.logs}")
          }

          debezium.stop()
          debezium.awaitReplicationSlotInactive(postgres, TIMEOUT)
          debezium = DebeziumFixture().withNetwork(network).withNetworkAliases("debezium")
          debezium.start()
          debezium.awaitReplicationStream(postgres, TIMEOUT)

          val second = acceptPaymentRequest()
          assertRequestAccepted(
              receive(paymentQueue, second.paymentRequestId),
              second.paymentRequestId,
          )
        } finally {
          debezium.stop()
        }
      }

  private suspend fun acceptPaymentRequest() =
      (commands.submitRequest(
              PaymentCommands.SubmitRequestSpec(
                  PAYMENT_OWNER,
                  UUID.randomUUID(),
                  UUID.randomUUID(),
                  100,
                  "AUD",
              ),
          ) as SubmitPaymentRequestResult.Accepted)
          .paymentRequest

  private suspend fun redeemVoucher(): RedeemedVoucher {
    val voucher =
        voucherCreationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))
    val requestId = UUID.randomUUID()
    voucherRedemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id))
    return RedeemedVoucher(voucher, requestId)
  }

  private fun receive(queue: String, expectedId: UUID): String =
      String(rabbit.receiveUntil(connection, queue, expectedId, TIMEOUT).body)

  private fun assertRequestAccepted(body: String, paymentRequestId: UUID) {
    val event = objectMapper.readTree(body)
    assertEquals(paymentRequestId.toString(), event.required("paymentRequestId").asText())
    assertEquals("PaymentRequestAccepted", event.required("eventType").asText())
    assertNotNull(UUID.fromString(event.required("eventId").asText()))
  }

  private fun assertVoucherRedeemed(body: String, voucher: RedeemedVoucher) {
    val event = objectMapper.readTree(body)
    assertEquals(voucher.details.id.toString(), event.required("voucherId").asText())
    assertEquals(voucher.requestId.toString(), event.required("requestId").asText())
    assertEquals("VoucherRedeemed", event.required("eventType").asText())
    assertNotNull(UUID.fromString(event.required("eventId").asText()))
  }

  private data class RedeemedVoucher(
      val details: VoucherDetails,
      val requestId: UUID,
  )

  private companion object {
    const val PAYMENT_EXCHANGE = "kra.payment.events"
    const val VOUCHER_EXCHANGE = "kra.voucher.events"
    val PAYMENT_OWNER = PaymentOwnerId("debezium-test-owner")
    val TIMEOUT: Duration = Duration.ofSeconds(45)
  }
}
