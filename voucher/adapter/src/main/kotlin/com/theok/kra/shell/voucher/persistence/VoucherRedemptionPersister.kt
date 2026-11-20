package com.theok.kra.shell.voucher.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.core.voucher.domain.RedemptionOutcome
import com.theok.kra.core.voucher.domain.RedemptionStatus
import com.theok.kra.core.voucher.domain.RejectionReason
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.platform.database.json.JooqJsonSupport
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_CLAIM
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER_OUTBOX
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.jooq.DSLContext
import org.springframework.beans.factory.ObjectProvider
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

/**
 * PostgreSQL implementation of the one atomic redemption operation.
 *
 * Its caller owns the declarative transaction. Every statement runs through the transaction-bound
 * connection supplied by [TransactionalJooqExecutor], so a failure rolls back the claim, capacity
 * update, final outcome, and outbox event together.
 */
@Component
class VoucherRedemptionPersister(
    private val jooq: TransactionalJooqExecutor,
    objectMapperProvider: ObjectProvider<ObjectMapper>,
) {
  private val objectMapper = objectMapperProvider.getIfAvailable() ?: JooqJsonSupport.objectMapper()

  suspend fun redeemAtomically(requestId: UUID, voucherId: UUID): RedemptionOutcome =
      jooq.execute { dsl ->
        val command = RedemptionAttempt(requestId, voucherId)
        val inserted =
            Flux.from(
                    dsl.insertInto(VOUCHER_CLAIM)
                        .set(VOUCHER_CLAIM.REQUEST_ID, command.requestId)
                        .set(VOUCHER_CLAIM.VOUCHER_ID, command.voucherId)
                        .set(VOUCHER_CLAIM.STATUS, PENDING)
                        .onConflict(VOUCHER_CLAIM.REQUEST_ID)
                        .doNothing(),
                )
                .single()
                .awaitSingle()

        if (inserted == 0) loadRecordedOutcome(dsl, command)
        else redeemRegisteredClaim(dsl, command)
      }

  private suspend fun redeemRegisteredClaim(
      dsl: DSLContext,
      command: RedemptionAttempt,
  ): RedemptionOutcome {
    val updated =
        Flux.from(
                dsl.update(VOUCHER)
                    .set(VOUCHER.REMAINING, VOUCHER.REMAINING.sub(1))
                    .where(VOUCHER.ID.eq(command.voucherId))
                    .and(VOUCHER.REMAINING.gt(0)),
            )
            .single()
            .awaitSingle()

    val outcome =
        if (updated == 1) {
          RedemptionOutcome(command.requestId, command.voucherId, RedemptionStatus.SUCCEEDED)
        } else {
          RedemptionOutcome(
              command.requestId,
              command.voucherId,
              RedemptionStatus.REJECTED,
              if (voucherExists(dsl, command.voucherId)) {
                RejectionReason.CAPACITY_EXHAUSTED
              } else {
                RejectionReason.VOUCHER_NOT_FOUND
              },
          )
        }

    persistOutcome(dsl, outcome)
    if (outcome.status == RedemptionStatus.SUCCEEDED) {
      insertOutboxEvent(dsl, outcome)
    }
    return outcome
  }

  private suspend fun loadRecordedOutcome(
      dsl: DSLContext,
      command: RedemptionAttempt,
  ): RedemptionOutcome {
    val existing =
        Flux.from(
                dsl.select(
                        VOUCHER_CLAIM.VOUCHER_ID,
                        VOUCHER_CLAIM.STATUS,
                        VOUCHER_CLAIM.REJECTION_REASON,
                    )
                    .from(VOUCHER_CLAIM)
                    .where(VOUCHER_CLAIM.REQUEST_ID.eq(command.requestId)),
            )
            .single()
            .awaitSingle()
    if (requireNotNull(existing.get(VOUCHER_CLAIM.VOUCHER_ID)) != command.voucherId) {
      return RedemptionOutcome(
          command.requestId,
          command.voucherId,
          RedemptionStatus.REJECTED,
          RejectionReason.IDEMPOTENCY_CONFLICT,
      )
    }
    return when (requireNotNull(existing.get(VOUCHER_CLAIM.STATUS))) {
      SUCCEEDED ->
          RedemptionOutcome(command.requestId, command.voucherId, RedemptionStatus.SUCCEEDED)
      REJECTED ->
          RedemptionOutcome(
              command.requestId,
              command.voucherId,
              RedemptionStatus.REJECTED,
              RejectionReason.valueOf(requireNotNull(existing.get(VOUCHER_CLAIM.REJECTION_REASON))),
          )
      PENDING -> error("A committed voucher claim cannot remain pending")
      else -> error("Unknown voucher claim status")
    }
  }

  private suspend fun voucherExists(dsl: DSLContext, voucherId: UUID): Boolean =
      Flux.from(
              dsl.select(VOUCHER.ID).from(VOUCHER).where(VOUCHER.ID.eq(voucherId)),
          )
          .next()
          .awaitSingleOrNull() != null

  private suspend fun persistOutcome(dsl: DSLContext, outcome: RedemptionOutcome) {
    val update =
        when (outcome.status) {
          RedemptionStatus.SUCCEEDED ->
              dsl.update(VOUCHER_CLAIM)
                  .set(VOUCHER_CLAIM.STATUS, SUCCEEDED)
                  .where(VOUCHER_CLAIM.REQUEST_ID.eq(outcome.requestId))
          RedemptionStatus.REJECTED ->
              dsl.update(VOUCHER_CLAIM)
                  .set(VOUCHER_CLAIM.STATUS, REJECTED)
                  .set(VOUCHER_CLAIM.REJECTION_REASON, requireNotNull(outcome.rejectionReason).name)
                  .where(VOUCHER_CLAIM.REQUEST_ID.eq(outcome.requestId))
        }
    check(Flux.from(update).single().awaitSingle() == 1) {
      "Voucher claim finalisation must update one row"
    }
  }

  private suspend fun insertOutboxEvent(dsl: DSLContext, outcome: RedemptionOutcome) {
    val eventId = UUID.randomUUID()
    val payload =
        objectMapper
            .createObjectNode()
            .put("eventId", eventId.toString())
            .put("voucherId", outcome.voucherId.toString())
            .put("requestId", outcome.requestId.toString())
            .put("eventType", VOUCHER_REDEEMED)
    Flux.from(
            dsl.insertInto(VOUCHER_OUTBOX)
                .set(VOUCHER_OUTBOX.EVENT_ID, eventId)
                .set(VOUCHER_OUTBOX.AGGREGATE_ID, outcome.voucherId)
                .set(VOUCHER_OUTBOX.AGGREGATE_TYPE, "voucher")
                .set(VOUCHER_OUTBOX.EVENT_TYPE, VOUCHER_REDEEMED)
                .set(VOUCHER_OUTBOX.PAYLOAD, payload),
        )
        .single()
        .awaitSingle()
  }

  private data class RedemptionAttempt(
      val requestId: UUID,
      val voucherId: UUID,
  )

  private companion object {
    const val PENDING = "PENDING"
    const val SUCCEEDED = "SUCCEEDED"
    const val REJECTED = "REJECTED"
    const val VOUCHER_REDEEMED = "VoucherRedeemed"
  }
}
