package com.theok.kra.shell.voucher.controller

import com.theok.kra.core.voucher.domain.RedemptionOutcome
import com.theok.kra.core.voucher.domain.RedemptionStatus
import com.theok.kra.core.voucher.domain.RejectionReason
import com.theok.kra.core.voucher.dto.VoucherDetails
import com.theok.kra.shell.voucher.commands.VoucherCreationCommands
import com.theok.kra.shell.voucher.commands.VoucherRedemptionCommands
import com.theok.kra.shell.voucher.queries.VoucherQueries
import jakarta.validation.Valid
import jakarta.validation.constraints.Positive
import java.util.UUID
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/vouchers")
class VoucherController(
    private val creationCommands: VoucherCreationCommands,
    private val redemptionCommands: VoucherRedemptionCommands,
    private val queries: VoucherQueries,
) {
  @PostMapping
  suspend fun create(
      @Valid @RequestBody request: CreateVoucherRequest,
  ): ResponseEntity<VoucherDetails> {
    val voucher =
        creationCommands.create(VoucherCreationCommands.CreateSpec(request.initialCapacity))
    return ResponseEntity.status(HttpStatus.CREATED).body(voucher)
  }

  @GetMapping("/{id}")
  suspend fun details(@PathVariable id: UUID): VoucherDetails {
    return queries.details(id)
        ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Voucher not found")
  }

  @PostMapping("/{id}/redemptions")
  suspend fun redeem(
      @PathVariable id: UUID,
      @RequestHeader("Idempotency-Key") requestId: UUID,
  ): ResponseEntity<RedemptionOutcome> {
    val outcome = redemptionCommands.redeem(VoucherRedemptionCommands.RedeemSpec(requestId, id))
    return when (outcome.status) {
      RedemptionStatus.SUCCEEDED -> ResponseEntity.ok(outcome)
      RedemptionStatus.REJECTED -> ResponseEntity.status(outcome.httpStatus()).body(outcome)
    }
  }
}

data class CreateVoucherRequest(
    @field:Positive val initialCapacity: Int,
)

private fun RedemptionOutcome.httpStatus(): HttpStatus =
    when (requireNotNull(rejectionReason)) {
      RejectionReason.CAPACITY_EXHAUSTED,
      RejectionReason.IDEMPOTENCY_CONFLICT,
      -> HttpStatus.CONFLICT
      RejectionReason.VOUCHER_NOT_FOUND -> HttpStatus.NOT_FOUND
    }
