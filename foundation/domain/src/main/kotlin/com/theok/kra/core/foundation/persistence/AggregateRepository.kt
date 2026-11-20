package com.theok.kra.core.foundation.persistence

/** Command-side aggregate loading contract; it is not a query projection API. */
fun interface AggregateLoader<ID : Any, A> {
  suspend fun load(id: ID): A?
}

/** Command-side persistence contract. Implementations return assigned IDs and versions. */
fun interface AggregatePersister<A> {
  suspend fun save(aggregate: A): A
}

interface AggregateRepository<ID : Any, A> : AggregateLoader<ID, A>, AggregatePersister<A>

/** Thin composition only. It intentionally does not add CRUD, retries, transactions, or mapping. */
class DefaultAggregateRepository<ID : Any, A>(
    private val loader: AggregateLoader<ID, A>,
    private val persister: AggregatePersister<A>,
) : AggregateRepository<ID, A> {
  override suspend fun load(id: ID): A? = loader.load(id)

  override suspend fun save(aggregate: A): A = persister.save(aggregate)
}
