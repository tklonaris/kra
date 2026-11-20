package com.theok.kra.app.voucher

import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.app.testsupport.voucher.VoucherRedemptionDatabase
import com.theok.kra.core.voucher.domain.RedemptionOutcome
import com.theok.kra.core.voucher.domain.RedemptionStatus
import com.theok.kra.core.voucher.domain.RejectionReason
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.voucher.commands.VoucherCreationCommands
import com.theok.kra.shell.voucher.commands.VoucherRedemptionCommands
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_CLAIM
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_OUTBOX
import com.theok.kra.shell.voucher.queries.VoucherQueries
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.DockerClientFactory
import org.testcontainers.postgresql.PostgreSQLContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VoucherRedemptionIntegrationTest {
  private val postgres = PostgreSQLContainer("postgres:18.6")
  private lateinit var context: ConfigurableApplicationContext
  private lateinit var redemptionCommands: VoucherRedemptionCommands
  private lateinit var creationCommands: VoucherCreationCommands
  private lateinit var queries: VoucherQueries
  private lateinit var jooq: TransactionalJooqExecutor
  private lateinit var database: VoucherRedemptionDatabase

  @BeforeAll
  fun startApplication() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    postgres.start()
    PostgresFixture.bootstrap(postgres)
    PostgresFixture.migrate(postgres)
    context =
        SpringApplicationBuilder(KotlinReferenceArchitectureApplication::class.java)
            .web(WebApplicationType.NONE)
            .run(
                "--spring.r2dbc.url=${PostgresFixture.r2dbcUrl(postgres)}",
                "--spring.r2dbc.username=${PostgresFixture.APP_USER}",
                "--spring.r2dbc.password=${PostgresFixture.APP_PASSWORD}",
                "--spring.r2dbc.pool.initial-size=0",
                "--spring.liquibase.enabled=false",
            )
    redemptionCommands = context.getBean(VoucherRedemptionCommands::class.java)
    creationCommands = context.getBean(VoucherCreationCommands::class.java)
    queries = context.getBean(VoucherQueries::class.java)
    jooq = context.getBean(TransactionalJooqExecutor::class.java)
    database = VoucherRedemptionDatabase(jooq, postgres)
  }

  @AfterAll
  fun stopApplication() {
    if (::context.isInitialized) context.close()
    postgres.stop()
  }

  @BeforeEach fun resetDatabase() = runBlocking { database.reset() }

  @Test
  fun `concurrent final redemptions allow one success`() = runBlocking {
    val voucher = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))

    val outcomes = coroutineScope {
      (1..24)
          .map {
            async(Dispatchers.Default) {
              redemptionCommands.redeem(
                  VoucherRedemptionCommands.RedeemSpec(UUID.randomUUID(), voucher.id),
              )
            }
          }
          .awaitAll()
    }

    assertEquals(1, outcomes.count { it.status == RedemptionStatus.SUCCEEDED })
    assertEquals(0, requireNotNull(queries.details(voucher.id)).remaining)
    assertEquals(1, database.count(VOUCHER_CLAIM, VOUCHER_CLAIM.STATUS.eq("SUCCEEDED")))
    assertEquals(1, database.count(VOUCHER_OUTBOX))
    assertTrue(requireNotNull(queries.details(voucher.id)).remaining >= 0)
  }

  @Test
  fun `concurrent duplicate requests perform one redemption`() = runBlocking {
    val voucher = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 2))
    val requestId = UUID.randomUUID()

    val outcomes = coroutineScope {
      (1..24)
          .map {
            async(Dispatchers.Default) {
              redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id))
            }
          }
          .awaitAll()
    }

    assertTrue(
        outcomes.all { it == RedemptionOutcome(requestId, voucher.id, RedemptionStatus.SUCCEEDED) },
    )
    assertEquals(1, requireNotNull(queries.details(voucher.id)).remaining)
    assertEquals(1, database.count(VOUCHER_CLAIM))
    assertEquals(1, database.count(VOUCHER_OUTBOX))
  }

  @Test
  fun `conflicting idempotency key does not consume another voucher`() = runBlocking {
    val first = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))
    val second = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))
    val requestId = UUID.randomUUID()
    assertEquals(
        RedemptionOutcome(requestId, first.id, RedemptionStatus.SUCCEEDED),
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, first.id)),
    )

    val conflict =
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, second.id))

    assertEquals(
        RedemptionOutcome(
            requestId,
            second.id,
            RedemptionStatus.REJECTED,
            RejectionReason.IDEMPOTENCY_CONFLICT,
        ),
        conflict,
    )
    assertEquals(1, requireNotNull(queries.details(second.id)).remaining)
  }

  @Test
  fun `rejected outcome remains stable for retries`() = runBlocking {
    val voucher = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))
    redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(UUID.randomUUID(), voucher.id))
    val requestId = UUID.randomUUID()

    val first =
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id))
    val retry =
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id))

    assertEquals(
        RedemptionOutcome(
            requestId,
            voucher.id,
            RedemptionStatus.REJECTED,
            RejectionReason.CAPACITY_EXHAUSTED,
        ),
        first,
    )
    assertEquals(first, retry)
    assertEquals(1, database.count(VOUCHER_CLAIM, VOUCHER_CLAIM.STATUS.eq("REJECTED")))
  }

  @Test
  fun `unknown voucher rejection is persisted for retries`() = runBlocking {
    val requestId = UUID.randomUUID()
    val voucherId = UUID.randomUUID()

    val first =
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucherId))
    val retry =
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucherId))

    assertEquals(
        RedemptionOutcome(
            requestId,
            voucherId,
            RedemptionStatus.REJECTED,
            RejectionReason.VOUCHER_NOT_FOUND,
        ),
        first,
    )
    assertEquals(first, retry)
    assertEquals(1, database.count(VOUCHER_CLAIM))
    assertEquals(0, database.count(VOUCHER_OUTBOX))
  }

  @Test
  fun `transaction rollback removes pending claim and restores capacity`() = runBlocking {
    val voucher = creationCommands.create(VoucherCreationCommands.CreateSpec(initialCapacity = 1))
    database.installClaimFinalisationFailure()
    val requestId = UUID.randomUUID()

    try {
      assertFails {
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id))
      }
      assertEquals(1, requireNotNull(queries.details(voucher.id)).remaining)
      assertEquals(0, database.count(VOUCHER_CLAIM))
      assertEquals(0, database.count(VOUCHER_OUTBOX))
    } finally {
      database.removeClaimFinalisationFailure()
    }

    assertEquals(
        RedemptionOutcome(requestId, voucher.id, RedemptionStatus.SUCCEEDED),
        redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, voucher.id)),
    )
  }

  @Test
  fun `database rejects invalid voucher capacity`() = runBlocking {
    assertFails {
      database.insertInvalidVoucher()
    }
  }
}
