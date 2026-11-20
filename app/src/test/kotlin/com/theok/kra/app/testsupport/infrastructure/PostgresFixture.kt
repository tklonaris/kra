package com.theok.kra.app.testsupport.infrastructure

import java.sql.DriverManager
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.MountableFile

/** PostgreSQL setup shared by application integration tests. */
internal object PostgresFixture {
  const val DATABASE = "kra"
  const val MIGRATOR_USER = "kra_migrator"
  const val MIGRATOR_PASSWORD = "migrator-password"
  const val APP_USER = "kra_app"
  const val APP_PASSWORD = "app-password"
  const val CDC_USER = "kra_cdc"
  const val CDC_PASSWORD = "cdc-password"
  const val CDC_STATE_USER = "kra_cdc_state"
  const val CDC_STATE_PASSWORD = "cdc-state-password"
  const val TEMPORAL_PASSWORD = "temporal-password"

  fun bootstrap(
      postgres: PostgreSQLContainer,
      passwords: BootstrapPasswords = defaultPasswords,
  ) {
    postgres.copyFileToContainer(
        MountableFile.forHostPath(InfrastructureFiles.postgresBootstrapSql()),
        "/tmp/init.sql",
    )
    val result =
        postgres.execInContainer(
            "psql",
            "--username=${postgres.username}",
            "--dbname=postgres",
            "--set=kra_migrator_password=${passwords.migrator}",
            "--set=kra_app_password=${passwords.app}",
            "--set=kra_cdc_password=${passwords.cdc}",
            "--set=kra_cdc_state_password=${passwords.cdcState}",
            "--set=kra_temporal_password=${passwords.temporal}",
            "--file=/tmp/init.sql",
        )
    check(result.exitCode == 0) { result.stderr }
  }

  fun migrate(
      postgres: PostgreSQLContainer,
      changelog: String = "db/changelog/db.changelog-master.yaml",
  ) {
    DriverManager.getConnection(jdbcUrl(postgres), MIGRATOR_USER, MIGRATOR_PASSWORD).use {
        connection ->
      val database =
          DatabaseFactory.getInstance()
              .findCorrectDatabaseImplementation(JdbcConnection(connection))
      database.defaultSchemaName = "kra"
      Liquibase(
              changelog,
              ClassLoaderResourceAccessor(),
              database,
          )
          .update(Contexts(), LabelExpression())
    }
  }

  fun jdbcUrl(postgres: PostgreSQLContainer, database: String = DATABASE): String =
      "${postgres.jdbcUrl.substringBeforeLast("/")}/$database"

  fun r2dbcUrl(postgres: PostgreSQLContainer): String =
      "r2dbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/$DATABASE?schema=kra"

  data class BootstrapPasswords(
      val migrator: String,
      val app: String,
      val cdc: String,
      val cdcState: String,
      val temporal: String,
  )

  private val defaultPasswords =
      BootstrapPasswords(
          MIGRATOR_PASSWORD,
          APP_PASSWORD,
          CDC_PASSWORD,
          CDC_STATE_PASSWORD,
          TEMPORAL_PASSWORD,
      )
}
