package com.theok.kra.core.foundation.domain

data class AggregateIdentifier<ID : Any>(
    val id: ID,
    val version: Long = 0,
) {
  init {
    require(version >= 0) { "Aggregate version must not be negative" }
  }
}
