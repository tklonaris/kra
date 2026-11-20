package com.theok.kra.app.testsupport.infrastructure

import java.nio.file.Path
import kotlin.io.path.isRegularFile

/** Repository-owned operational files used by integration tests. */
internal object InfrastructureFiles {
  private const val POSTGRES_BOOTSTRAP_SQL = "infrastructure/postgres/bootstrap.sql"
  private const val DEBEZIUM_CONFIGURATION = "infrastructure/debezium/application.properties"

  fun postgresBootstrapSql(): Path = repositoryFile(POSTGRES_BOOTSTRAP_SQL)

  fun debeziumConfiguration(): Path = repositoryFile(DEBEZIUM_CONFIGURATION)

  private fun repositoryFile(relativePath: String): Path =
      generateSequence(Path.of("").toAbsolutePath()) { it.parent }
          .map { it.resolve(relativePath) }
          .first { it.isRegularFile() }
}
