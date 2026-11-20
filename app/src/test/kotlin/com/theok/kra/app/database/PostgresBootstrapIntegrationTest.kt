package com.theok.kra.app.database

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.testcontainers.DockerClientFactory
import org.testcontainers.postgresql.PostgreSQLContainer

class PostgresBootstrapIntegrationTest {
  @Test
  fun `bootstrap rerun preserves append-only and infrastructure privilege boundaries`() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    val postgres = PostgreSQLContainer("postgres:18.6")
    postgres.start()

    try {
      PostgresFixture.bootstrap(postgres)
      PostgresFixture.migrate(postgres, "db/changelog/app-master.yaml")
      PostgresFixture.bootstrap(postgres)

      connect(postgres, PostgresFixture.APP_USER, PostgresFixture.APP_PASSWORD, "kra").use {
          connection ->
        connection.createStatement().use { statement ->
          assertFailsWith<SQLException> {
            statement.executeUpdate("UPDATE kra.payment_lifecycle_event SET reason = 'tampered'")
          }
          assertFailsWith<SQLException> {
            statement.executeUpdate("DELETE FROM kra.payment_lifecycle_event")
          }
          listOf(
                  "kra.databasechangelog",
                  "kra.databasechangeloglock",
                  "kra.debezium_offset_storage",
              )
              .forEach { table ->
                assertFailsWith<SQLException> {
                  statement.executeUpdate("DELETE FROM $table WHERE FALSE")
                }
              }
        }
      }
    } finally {
      postgres.stop()
    }
  }

  @Test
  fun `psql bootstrap creates least-privileged roles and is password-safe on rerun`() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    val postgres = PostgreSQLContainer("postgres:18.6")
    postgres.start()

    try {
      PostgresFixture.bootstrap(
          postgres,
          PostgresFixture.BootstrapPasswords(
              migrator = "migrator-first",
              app = "app-first",
              cdc = "cdc-first",
              cdcState = "cdc-state-first",
              temporal = "temporal-first",
          ),
      )
      PostgresFixture.bootstrap(
          postgres,
          PostgresFixture.BootstrapPasswords(
              migrator = "migrator-second",
              app = "app-second",
              cdc = "cdc-second",
              cdcState = "cdc-state-second",
              temporal = "temporal-second",
          ),
      )

      connect(postgres, postgres.username, postgres.password, "kra").use { connection ->
        connection.createStatement().use { statement ->
          statement.execute("SET ROLE kra_migrator")
          statement.execute("CREATE TABLE kra.bootstrap_privilege_probe (id integer primary key)")
        }
      }

      connect(postgres, "kra_app", "app-first", "kra").use { connection ->
        connection.createStatement().use { statement ->
          assertEquals(
              1,
              statement.executeUpdate("INSERT INTO kra.bootstrap_privilege_probe VALUES (1)"),
          )
        }
      }
      connect(postgres, "kra_cdc", "cdc-first", "kra").use { connection ->
        connection.createStatement().use { statement ->
          statement.executeQuery("SELECT id FROM kra.bootstrap_privilege_probe").use { rows ->
            check(rows.next())
            assertEquals(1, rows.getInt(1))
          }
          assertFailsWith<SQLException> {
            statement.executeUpdate("INSERT INTO kra.bootstrap_privilege_probe VALUES (2)")
          }
          assertFailsWith<SQLException> {
            statement.execute("CREATE TABLE kra.cdc_write_probe (id integer primary key)")
          }
        }
      }
      connect(postgres, "kra_cdc_state", "cdc-state-first", "kra").use { connection ->
        connection.createStatement().use { statement ->
          assertFailsWith<SQLException> {
            statement.executeQuery("SELECT id FROM kra.bootstrap_privilege_probe")
          }
          assertFailsWith<SQLException> {
            statement.executeUpdate("INSERT INTO kra.bootstrap_privilege_probe VALUES (3)")
          }
          assertFailsWith<SQLException> {
            statement.execute("CREATE TABLE kra.cdc_state_write_probe (id integer primary key)")
          }
        }
      }
      assertFailsWith<SQLException> { connect(postgres, "kra_app", "app-second", "kra").close() }
      assertFailsWith<SQLException> {
        connect(postgres, "kra_cdc_state", "cdc-state-second", "kra").close()
      }

      connect(postgres, postgres.username, postgres.password, "postgres").use { connection ->
        connection.createStatement().use { statement ->
          statement
              .executeQuery(
                  "SELECT rolreplication FROM pg_roles WHERE rolname = 'kra_cdc'",
              )
              .use { rows ->
                check(rows.next())
                assertEquals(true, rows.getBoolean(1))
              }
          statement
              .executeQuery(
                  "SELECT rolreplication FROM pg_roles WHERE rolname = 'kra_cdc_state'",
              )
              .use { rows ->
                check(rows.next())
                assertEquals(false, rows.getBoolean(1))
              }
        }
      }
    } finally {
      postgres.stop()
    }
  }

  private fun connect(
      postgres: PostgreSQLContainer,
      username: String,
      password: String,
      database: String,
  ) = DriverManager.getConnection(PostgresFixture.jdbcUrl(postgres, database), username, password)
}
