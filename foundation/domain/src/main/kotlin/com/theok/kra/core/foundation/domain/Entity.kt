package com.theok.kra.core.foundation.domain

/**
 * Minimal identity and optimistic-version contract for a domain entity.
 *
 * It deliberately says nothing about persistence, mapping, or lifecycle.
 */
interface Entity<ID : Any> {
  fun idOrNull(): ID?

  fun versionOrNull(): Long?

  fun requireId(): ID = requireNotNull(idOrNull()) { "Entity ID has not been assigned" }
}
