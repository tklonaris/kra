package com.theok.kra.app.testsupport.voucher

import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_CLAIM
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_OUTBOX
import java.sql.DriverManager
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.Condition
import org.jooq.Table
import org.testcontainers.postgresql.PostgreSQLContainer
import reactor.core.publisher.Flux

/** Database-only test operations for Voucher redemption scenarios. */
internal class VoucherRedemptionDatabase(
    private val jooq: TransactionalJooqExecutor,
    private val postgres: PostgreSQLContainer,
) {
  suspend fun reset() {
    jooq.execute { dsl ->
      Flux.from(dsl.deleteFrom(VOUCHER_OUTBOX)).single().awaitSingle()
      Flux.from(dsl.deleteFrom(VOUCHER_CLAIM)).single().awaitSingle()
      Flux.from(dsl.deleteFrom(VOUCHER)).single().awaitSingle()
      Unit
    }
  }

  suspend fun count(table: Table<*>, condition: Condition? = null): Int = jooq.execute { dsl ->
    val query =
        condition?.let { dsl.selectCount().from(table).where(it) } ?: dsl.selectCount().from(table)
    Flux.from(query).map { it.value1() }.single().awaitSingle()
  }

  suspend fun insertInvalidVoucher() {
    jooq.execute { dsl ->
      Flux.from(
              dsl.insertInto(VOUCHER)
                  .set(VOUCHER.ID, UUID.randomUUID())
                  .set(VOUCHER.INITIAL_CAPACITY, 0)
                  .set(VOUCHER.REMAINING, 0),
          )
          .single()
          .awaitSingle()
    }
  }

  fun installClaimFinalisationFailure() =
      executeAsMigrator(
          """
          CREATE FUNCTION kra.fail_voucher_claim_finalisation() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
          BEGIN
              RAISE EXCEPTION 'controlled redemption failure';
          END;
          ${'$'}${'$'};
          CREATE TRIGGER voucher_claim_finalisation_failure
              BEFORE UPDATE OF status ON kra.voucher_claim
              FOR EACH ROW EXECUTE FUNCTION kra.fail_voucher_claim_finalisation();
          """
              .trimIndent(),
      )

  fun removeClaimFinalisationFailure() =
      executeAsMigrator(
          """
          DROP TRIGGER IF EXISTS voucher_claim_finalisation_failure ON kra.voucher_claim;
          DROP FUNCTION IF EXISTS kra.fail_voucher_claim_finalisation();
          """
              .trimIndent(),
      )

  private fun executeAsMigrator(sql: String) {
    DriverManager.getConnection(
            PostgresFixture.jdbcUrl(postgres),
            PostgresFixture.MIGRATOR_USER,
            PostgresFixture.MIGRATOR_PASSWORD,
        )
        .use { connection ->
          connection.createStatement().use { it.execute(sql) }
        }
  }
}
