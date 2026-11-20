package com.theok.kra.core.foundation.domain

/** A root owns its identity without prescribing a backing persistence entity. */
interface AggregateRoot<ID : Any> : Entity<ID> {
  fun identifier(): AggregateIdentifier<ID> {
    return AggregateIdentifier(requireId(), versionOrNull() ?: 0)
  }
}
