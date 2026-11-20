package com.theok.kra.app.database

import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.platform.database.TransactionalJooqExecutor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.transaction.annotation.Transactional
import org.testcontainers.DockerClientFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import reactor.core.publisher.Flux

class DeclarativeTransactionIntegrationTest {
  @Test
  fun `declarative suspend transaction shares R2DBC state with jooq`() = runBlocking {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    val postgres = PostgreSQLContainer("postgres:18.6")
    postgres.start()

    try {
      SpringApplicationBuilder(
              KotlinReferenceArchitectureApplication::class.java,
              TransactionProbeConfiguration::class.java,
          )
          .web(WebApplicationType.NONE)
          .run(
              "--spring.r2dbc.url=${postgres.jdbcUrl.replaceFirst("jdbc:", "r2dbc:")}",
              "--spring.r2dbc.username=${postgres.username}",
              "--spring.r2dbc.password=${postgres.password}",
              "--spring.r2dbc.pool.initial-size=0",
              "--spring.liquibase.enabled=false",
          )
          .use { context ->
            val probe = context.getBean(TransactionProbe::class.java)
            val jooq = context.getBean(TransactionalJooqExecutor::class.java)
            assertEquals(1, probe.writeThenRead())
            probe.detectsStaleOptimisticUpdate()

            jooq.execute { dsl ->
              Flux.from(
                      dsl.query("CREATE TABLE transaction_rollback_probe (id integer primary key)"),
                  )
                  .then()
                  .awaitSingleOrNull()
              Unit
            }
            try {
              assertFailsWith<TransactionProbeFailure> { probe.writeThenRollback() }
              assertEquals(
                  0,
                  jooq.execute { dsl ->
                    Flux.from(dsl.selectCount().from(DSL.table("transaction_rollback_probe")))
                        .map { it.value1() }
                        .single()
                        .awaitSingle()
                  },
              )
            } finally {
              jooq.execute { dsl ->
                Flux.from(dsl.query("DROP TABLE IF EXISTS transaction_rollback_probe"))
                    .then()
                    .awaitSingleOrNull()
                Unit
              }
            }
          }
    } finally {
      postgres.stop()
    }
  }
}

@Configuration(proxyBeanMethods = false)
private class TransactionProbeConfiguration {
  @Bean
  fun transactionProbe(
      template: R2dbcEntityTemplate,
      jooq: TransactionalJooqExecutor,
  ) = TransactionProbe(template, jooq)
}

private open class TransactionProbe(
    private val template: R2dbcEntityTemplate,
    private val jooq: TransactionalJooqExecutor,
) {
  @Transactional
  open suspend fun writeThenRead(): Int {
    jooq.execute { dsl ->
      Flux.from(
              dsl.query(
                  "CREATE TEMPORARY TABLE transaction_probe (id integer primary key) ON COMMIT DROP",
              ),
          )
          .then()
          .awaitSingleOrNull()
      Unit
    }
    template.insert(TransactionProbeRow(1)).awaitSingle()
    return jooq.execute { dsl ->
      Flux.from(dsl.selectCount().from(DSL.table("transaction_probe")))
          .map { it.value1() }
          .single()
          .awaitSingle()
    }
  }

  @Transactional
  open suspend fun detectsStaleOptimisticUpdate() {
    jooq.execute { dsl ->
      Flux.from(
              dsl.query(
                  "CREATE TEMPORARY TABLE versioned_transaction_probe (id integer primary key, value integer not null, version bigint not null) ON COMMIT DROP",
              ),
          )
          .then()
          .awaitSingleOrNull()
      Unit
    }
    val inserted = template.insert(VersionedTransactionProbeRow(id = 1, value = 1)).awaitSingle()
    template.update(inserted.copy(value = 2)).awaitSingle()

    assertFailsWith<OptimisticLockingFailureException> {
      template.update(inserted.copy(value = 3)).awaitSingle()
    }
  }

  @Transactional
  open suspend fun writeThenRollback() {
    template.insert(TransactionRollbackProbeRow(1)).awaitSingle()
    throw TransactionProbeFailure()
  }
}

private class TransactionProbeFailure : RuntimeException()

@Table("transaction_probe")
private data class TransactionProbeRow(
    @Id val id: Int,
)

@Table("versioned_transaction_probe")
private data class VersionedTransactionProbeRow(
    @Id val id: Int,
    val value: Int,
    @Version val version: Long? = null,
)

@Table("transaction_rollback_probe")
private data class TransactionRollbackProbeRow(
    @Id val id: Int,
)
