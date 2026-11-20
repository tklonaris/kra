package com.theok.kra.app.voucher

import com.rabbitmq.client.Connection
import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.app.testsupport.infrastructure.DebeziumFixture
import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.infrastructure.RabbitMqFixture
import com.theok.kra.app.testsupport.voucher.VoucherHttpClient
import com.theok.kra.platform.database.json.JooqJsonSupport
import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.Network
import org.testcontainers.postgresql.PostgreSQLContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VoucherDebeziumRabbitMqIntegrationTest {
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
  private lateinit var rabbitConnection: Connection
  private lateinit var voucherHttp: VoucherHttpClient

  @BeforeAll
  fun startInfrastructure() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for Debezium/RabbitMQ integration",
    )
    postgres.start()
    rabbit.start(network)
    rabbit.provisionFanoutExchange(EXCHANGE)
    PostgresFixture.bootstrap(postgres)
    PostgresFixture.migrate(postgres, "db/changelog/app-master.yaml")
    verifyStateRole()
    rabbitConnection = rabbit.openConnection()
    context =
        SpringApplicationBuilder(KotlinReferenceArchitectureApplication::class.java)
            .web(WebApplicationType.REACTIVE)
            .run(
                "--spring.r2dbc.url=${PostgresFixture.r2dbcUrl(postgres)}",
                "--spring.r2dbc.username=${PostgresFixture.APP_USER}",
                "--spring.r2dbc.password=${PostgresFixture.APP_PASSWORD}",
                "--spring.r2dbc.pool.initial-size=0",
                "--spring.liquibase.enabled=false",
                "--server.port=0",
            )
    voucherHttp =
        VoucherHttpClient(
            "http://localhost:${requireNotNull((context as WebServerApplicationContext).webServer).port}",
        )
  }

  @AfterAll
  fun stopInfrastructure() {
    if (::context.isInitialized) context.close()
    if (::rabbitConnection.isInitialized) rabbitConnection.close()
    rabbit.container.stop()
    postgres.stop()
    network.close()
  }

  @Test
  fun `Debezium fans out redemption JSON and resumes from PostgreSQL offsets`() = runBlocking {
    val firstQueue = "voucher-redemption-first-${UUID.randomUUID()}"
    val secondQueue = "voucher-redemption-second-${UUID.randomUUID()}"
    rabbit.declareBoundQueue(rabbitConnection, firstQueue, EXCHANGE)
    rabbit.declareBoundQueue(rabbitConnection, secondQueue, EXCHANGE)

    var debezium = DebeziumFixture().withNetwork(network).withNetworkAliases("debezium")
    debezium.start()
    try {
      debezium.awaitReplicationStream(postgres, RECEIVE_TIMEOUT)
      val firstVoucher = voucherHttp.create()
      val firstRequestId = UUID.randomUUID()
      voucherHttp.redeem(firstVoucher.id, firstRequestId)

      val firstFromFirstQueue = receive(firstQueue)
      val firstFromSecondQueue = receive(secondQueue)
      assertVoucherRedeemed(firstFromFirstQueue, firstVoucher.id, firstRequestId)
      assertVoucherRedeemed(firstFromSecondQueue, firstVoucher.id, firstRequestId)
      assertEquals(firstFromFirstQueue.body.eventId, firstFromSecondQueue.body.eventId)
      debezium.awaitOffset(postgres, RECEIVE_TIMEOUT)

      debezium.stop()
      debezium.awaitReplicationSlotInactive(postgres, RECEIVE_TIMEOUT)
      debezium = DebeziumFixture().withNetwork(network).withNetworkAliases("debezium")
      debezium.start()
      debezium.awaitReplicationStream(postgres, RECEIVE_TIMEOUT)

      val secondVoucher = voucherHttp.create()
      val secondRequestId = UUID.randomUUID()
      voucherHttp.redeem(secondVoucher.id, secondRequestId)

      assertVoucherRedeemed(
          receiveUntil(firstQueue, secondRequestId),
          secondVoucher.id,
          secondRequestId,
      )
      assertVoucherRedeemed(
          receiveUntil(secondQueue, secondRequestId),
          secondVoucher.id,
          secondRequestId,
      )
    } finally {
      debezium.stop()
    }
  }

  private fun receive(queue: String): ReceivedEvent = receiveUntil(queue, null)

  private fun receiveUntil(queue: String, expectedRequestId: UUID?): ReceivedEvent {
    val delivery = rabbit.receiveUntil(rabbitConnection, queue, expectedRequestId, RECEIVE_TIMEOUT)
    return receivedEvent(delivery.properties, delivery.body)
  }

  private fun receivedEvent(
      properties: com.rabbitmq.client.AMQP.BasicProperties,
      body: ByteArray,
  ): ReceivedEvent {
    val json = objectMapper.readTree(String(body, StandardCharsets.UTF_8))
    val fields = json.fieldNames().asSequence().toSet()
    assertEquals(setOf("eventId", "voucherId", "requestId", "eventType"), fields)
    val event =
        EventBody(
            UUID.fromString(json.required("eventId").asText()),
            UUID.fromString(json.required("voucherId").asText()),
            UUID.fromString(json.required("requestId").asText()),
            json.required("eventType").asText(),
        )
    return ReceivedEvent(event, properties.headers.orEmpty(), properties.deliveryMode)
  }

  private fun assertVoucherRedeemed(event: ReceivedEvent, voucherId: UUID, requestId: UUID) {
    // The configured JSON header formatter produces JSON scalar values in AMQP headers.
    assertEquals("\"${event.body.eventId}\"", headerText(event.headers, "id"))
    assertEquals("\"VoucherRedeemed\"", headerText(event.headers, "eventType"))
    assertTrue(assertNotNull(headerText(event.headers, "occurredAt")).startsWith("\""))
    assertEquals(2, event.deliveryMode)
    assertEquals(voucherId, event.body.voucherId)
    assertEquals(requestId, event.body.requestId)
    assertEquals("VoucherRedeemed", event.body.eventType)
  }

  private fun headerText(headers: Map<String, Any>, name: String): String? =
      when (val value = headers[name]) {
        null -> null
        is ByteArray -> String(value, StandardCharsets.UTF_8)
        else -> value.toString()
      }

  private fun verifyStateRole() {
    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(postgres),
            PostgresFixture.CDC_STATE_USER,
            PostgresFixture.CDC_STATE_PASSWORD,
        )
        .use { connection ->
          connection.createStatement().use { statement ->
            statement.executeQuery("SELECT count(*) FROM kra.debezium_offset_storage").use { rows ->
              check(rows.next())
            }
            assertFailsWith<SQLException> {
              statement.executeQuery("SELECT count(*) FROM kra.voucher_outbox")
            }
          }
        }
  }

  private data class EventBody(
      val eventId: UUID,
      val voucherId: UUID,
      val requestId: UUID,
      val eventType: String,
  )

  private data class ReceivedEvent(
      val body: EventBody,
      val headers: Map<String, Any>,
      val deliveryMode: Int?,
  )

  private companion object {
    const val EXCHANGE = "kra.voucher.events"
    val RECEIVE_TIMEOUT: Duration = Duration.ofSeconds(45)
  }
}
