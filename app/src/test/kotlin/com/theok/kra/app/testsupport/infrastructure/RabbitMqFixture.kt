package com.theok.kra.app.testsupport.infrastructure

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.theok.kra.platform.database.json.JooqJsonSupport
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/** RabbitMQ container and transport operations used by end-to-end tests. */
internal class RabbitMqFixture {
  val container: GenericContainer<*> = RabbitContainer()

  fun start(network: Network) {
    container.withNetwork(network).withNetworkAliases("rabbitmq").start()
  }

  fun provisionFanoutExchange(exchange: String) {
    val credentials =
        Base64.getEncoder().encodeToString("$USER:$PASSWORD".toByteArray(StandardCharsets.UTF_8))
    val exchangeDefinition =
        JooqJsonSupport.objectMapper()
            .createObjectNode()
            .apply {
              put("type", "fanout")
              put("durable", true)
              put("auto_delete", false)
              put("internal", false)
              putObject("arguments")
            }
            .toString()
    val request =
        HttpRequest.newBuilder(
                URI.create(
                    "http://${container.host}:${container.getMappedPort(15672)}/api/exchanges/%2F/$exchange",
                ),
            )
            .header("Authorization", "Basic $credentials")
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(exchangeDefinition))
            .build()
    val response =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build()
            .send(request, HttpResponse.BodyHandlers.discarding())
    check(response.statusCode() in setOf(201, 204)) {
      "Exchange declaration failed with HTTP ${response.statusCode()}"
    }
  }

  fun openConnection(): Connection =
      ConnectionFactory()
          .also {
            it.host = container.host
            it.port = container.getMappedPort(5672)
            it.username = USER
            it.password = PASSWORD
            it.isAutomaticRecoveryEnabled = false
          }
          .newConnection()

  fun declareBoundQueue(connection: Connection, queue: String, exchange: String) {
    connection.createChannel().use { channel ->
      channel.queueDeclare(queue, true, false, false, null)
      channel.queueBind(queue, exchange, "")
    }
  }

  fun receiveUntil(
      connection: Connection,
      queue: String,
      expectedRequestId: UUID?,
      timeout: Duration,
  ): Delivery {
    val deadline = Instant.now().plus(timeout)
    connection.createChannel().use { channel ->
      while (Instant.now().isBefore(deadline)) {
        val delivery = channel.basicGet(queue, true)
        if (
            delivery != null &&
                (expectedRequestId == null ||
                    expectedRequestId.toString() in String(delivery.body, StandardCharsets.UTF_8))
        ) {
          return Delivery(delivery.props, delivery.body)
        }
        Thread.sleep(POLL_INTERVAL.toMillis())
      }
    }
    error("Timed out waiting for ${expectedRequestId ?: "a RabbitMQ event"} on $queue")
  }

  data class Delivery(
      val properties: AMQP.BasicProperties,
      val body: ByteArray,
  )

  private class RabbitContainer :
      GenericContainer<RabbitContainer>(DockerImageName.parse("rabbitmq:4.4.0-management")) {
    init {
      withExposedPorts(5672, 15672)
      waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
      withEnv("RABBITMQ_DEFAULT_USER", USER)
      withEnv("RABBITMQ_DEFAULT_PASS", PASSWORD)
    }
  }

  companion object {
    const val USER = "kra"
    const val PASSWORD = "rabbit-password"
    private val POLL_INTERVAL: Duration = Duration.ofMillis(100)
  }
}
