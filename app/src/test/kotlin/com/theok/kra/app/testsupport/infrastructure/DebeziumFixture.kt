package com.theok.kra.app.testsupport.infrastructure

import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import org.testcontainers.containers.GenericContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

/** Debezium Server container and PostgreSQL readiness checks for the outbox smoke test. */
internal class DebeziumFixture(
    private val configuration: java.nio.file.Path = InfrastructureFiles.debeziumConfiguration(),
    private val slotName: String = SLOT_NAME,
    private val publicationName: String = "kra_outbox_publication",
    private val offsetStorageTable: String = "kra.debezium_offset_storage",
) :
    GenericContainer<DebeziumFixture>(
        DockerImageName.parse("quay.io/debezium/server:3.7.1.Final"),
    ) {
  fun awaitReplicationStream(postgres: PostgreSQLContainer, timeout: Duration) {
    await("Debezium did not activate the PostgreSQL replication slot", timeout) {
      DriverManager.getConnection(
              PostgresFixture.jdbcUrl(postgres),
              postgres.username,
              postgres.password,
          )
          .use { connection ->
            connection
                .prepareStatement("SELECT active FROM pg_replication_slots WHERE slot_name = ?")
                .use { statement ->
                  statement.setString(1, slotName)
                  statement.executeQuery().use { rows ->
                    rows.next() && rows.getBoolean(1)
                  }
                }
          }
    }
  }

  fun awaitReplicationSlotInactive(postgres: PostgreSQLContainer, timeout: Duration) {
    await("Debezium did not retain an inactive PostgreSQL replication slot", timeout) {
      DriverManager.getConnection(
              PostgresFixture.jdbcUrl(postgres),
              postgres.username,
              postgres.password,
          )
          .use { connection ->
            connection
                .prepareStatement("SELECT active FROM pg_replication_slots WHERE slot_name = ?")
                .use { statement ->
                  statement.setString(1, slotName)
                  statement.executeQuery().use { rows -> rows.next() && !rows.getBoolean(1) }
                }
          }
    }
  }

  fun awaitOffset(postgres: PostgreSQLContainer, timeout: Duration) {
    await("Debezium did not persist a PostgreSQL offset", timeout) {
      DriverManager.getConnection(
              PostgresFixture.jdbcUrl(postgres),
              PostgresFixture.CDC_STATE_USER,
              PostgresFixture.CDC_STATE_PASSWORD,
          )
          .use { connection ->
            connection.createStatement().use { statement ->
              statement.executeQuery("SELECT count(*) FROM $offsetStorageTable").use { rows ->
                check(rows.next())
                rows.getInt(1) > 0
              }
            }
          }
    }
  }

  private fun await(message: String, timeout: Duration, condition: () -> Boolean) {
    val deadline = Instant.now().plus(timeout)
    while (Instant.now().isBefore(deadline)) {
      if (condition()) return
      Thread.sleep(POLL_INTERVAL.toMillis())
    }
    error(message)
  }

  init {
    withCopyFileToContainer(
        MountableFile.forHostPath(configuration),
        "/debezium/config/application.properties",
    )
    withEnv(
        mapOf(
            "KRA_DB_NAME" to PostgresFixture.DATABASE,
            "KRA_CDC_USER" to PostgresFixture.CDC_USER,
            "KRA_CDC_PASSWORD" to PostgresFixture.CDC_PASSWORD,
            "KRA_CDC_STATE_USER" to PostgresFixture.CDC_STATE_USER,
            "KRA_CDC_STATE_PASSWORD" to PostgresFixture.CDC_STATE_PASSWORD,
            "KRA_CDC_PUBLICATION" to publicationName,
            "KRA_CDC_SLOT_NAME" to slotName,
            "KRA_RABBITMQ_USER" to RabbitMqFixture.USER,
            "KRA_RABBITMQ_PASSWORD" to RabbitMqFixture.PASSWORD,
        ),
    )
  }

  companion object {
    const val SLOT_NAME = "kra_outbox_slot"
    private val POLL_INTERVAL: Duration = Duration.ofMillis(100)
  }
}
