# Project conventions

## Module and package ownership

- Keep framework-free rules, contracts, domain models, and DTOs under `com.theok.kra.core`. Domain packages must not depend on their feature DTO packages.
- Keep HTTP controllers, commands, queries, persistence adapters, mapped R2DBC entities, feature migrations, and generated jOOQ types in the feature adapter under `com.theok.kra.shell.<feature>`.
- A feature adapter is organised as `shell.<feature>.controller`, `.commands`, `.queries`, and `.persistence` (including `.persistence.entity`). Controllers depend only on commands and queries; commands delegate writes to persistence; queries use read projections; persistence never depends on controllers.
- Dependency direction is `core <- shell.<feature> <- app`; `shell.<feature>` may also depend on shared `platform` infrastructure. `platform` must not depend on a feature, and one feature adapter must not depend on another.
- `platform-database` and `platform-web` contain reusable technical infrastructure only; they must not own feature HTTP endpoints, schema, aggregate persistence, or business rules.
- `app` is the Spring Boot deployment and composition unit. It scans `com.theok.kra.shell` and explicitly imports platform infrastructure; do not move feature code into `app`.
- The functional core must not depend on Spring, R2DBC, jOOQ, Jackson, RabbitMQ, Temporal, `platform`, `shell`, or `app`.

## Commands, queries, and persistence

- Controllers map HTTP only. They do not access R2DBC, jOOQ, repositories, or transaction APIs directly.
- Commands define transactional write units and may mutate state or return a command outcome. Queries use projection-only reads, never invoke commands or write-capable persistence adapters, and never cause side effects.
- Use non-blocking WebFlux, Kotlin coroutines, and R2DBC on production request paths. Keep blocking JDBC, HTTP clients, and container control in tests, migrations, or operational tooling.
- Public `suspend` command methods normally own declarative `@Transactional` boundaries; controllers never own them. A workflow activity or message consumer that sequences database work around remote I/O may instead use an operation-specific persistence adapter's narrow `@Transactional` method or transaction block. Such blocks contain database work only; Stripe, Temporal, RabbitMQ, and other remote calls occur before or after them, never within them.
- Keep production business, command, workflow, messaging, and controller code free of jOOQ DSL and generated-table references. Put feature writes and jOOQ record mapping in a dedicated, operation-specific persistence adapter; put projection reads in a dedicated feature query class. Their callers communicate through domain models, DTOs, named persistence outcomes, and read projections.
- Use Spring Data R2DBC and mapped entities for ordinary aggregate writes. Keep persistence mappings in the shell and preserve optimistic `@Version` handling where applicable.
- Use generated jOOQ projections for reads; return DTOs and do not load aggregates merely to serve a query.
- Use `TransactionalJooqExecutor` for PostgreSQL-specific or contention-sensitive SQL. Execute and consume all jOOQ publishers inside its block so they use the Spring transaction-bound R2DBC connection.
- Prefer expression-based transaction blocks; do not use labeled `return@…` control flow inside `TransactionalJooqExecutor.execute` blocks.
- Do not introduce JPA/Hibernate, generic CRUD repositories, generic persistence services, or query-style `fetch` methods on command-side aggregate repositories. Command-side repository loading is named `load`.
- Keep persistence adapters operation-sized. A thin transaction facade may compose narrowly scoped collaborators, but each collaborator owns one coherent concern such as loading, intent creation, state transitions, cancellation, or workflow-event persistence.
- Name components and variables for their role: use `Loader` for command-side reads and `Persister` for durable writes, transaction entry points, and integration-event state. Do not retain a broader or historical name when the responsibility has narrowed.
- Keep workflow coordinators small by delegating distinct decisions and remote-I/O sequences to focused collaborators. `when (val outcome = ...)` is valid value-based branching when the subject is only useful inside the branches; bind it before the `when` when that is clearer. Use braced branches for multi-statement or side-effecting work.

## Payment workflows and provider reconciliation

- Treat the durable `payment_request`, its optional durable payment row, and verified provider events as authoritative. Broker messages and Temporal signals are at-least-once wake-ups: they must not, by themselves, authorize a Stripe create, cancellation, or terminal transition.
- Derive cancellation from durable state in one short persistence operation: a recorded customer request or elapsed checkout deadline, the current terminal status, and whether a provider intent exists. The resulting decision is either no action, local finalisation without an intent, or provider-intent reconciliation; do not duplicate that decision tree in dispatchers or workflows.
- Persist a local cancellation-origin claim before calling Stripe, use an origin-specific stable idempotency key, and reconcile Stripe after a cancellation-call failure before deciding the durable outcome. Keep the Stripe call outside the database transaction.
- Use one stable Temporal Workflow ID per payment request with Signal-With-Start. A later cancellation, verified provider event, or authorised retry re-enters from authoritative PostgreSQL state; do not add direct dispatcher-to-business-operation recovery paths, keep-alive windows, recovery workflows, or database reconciliation workers.
- Never broadly search Stripe or repeat an intent create after its idempotency window. Recovery may retrieve only the intent named by a verified, payment-associated webhook event, and must validate its ID, amount, currency, and `kra_payment_id` metadata before persisting it.

## Concurrency-sensitive writes and integration events

- Put an operation-specific PostgreSQL persistence adapter behind its command service when correctness depends on database semantics; do not implement it as aggregate load-mutate-save.
- Model idempotency with durable database state and constraints. A duplicate request returns its recorded outcome; reuse of the same idempotency key for incompatible input returns a domain conflict.
- Use generated jOOQ for atomic conditional writes whose affected-row result determines the outcome. Persist a stable terminal result for retries.
- Rely on PostgreSQL constraints, row locking, and conditional updates for cross-instance correctness; do not substitute application locks, distributed locks, or in-memory coordination.
- Keep every state transition required by the outcome in one declarative transaction.
- When a successful state transition emits an integration event, append its outbox record in that same transaction. Publish asynchronously from the outbox; never perform a direct dual write.

## Database, generation, and operations

- Feature adapters own their Liquibase migrations and generated jOOQ snapshot. Numbered `one-off` migrations are immutable; repeatable definitions belong in `repeatable` with `runOnChange`.
- Never manually edit `src/generated/jooq`. Regenerate and verify it with `:platform-database:dbGenerateJooq`.
- Normal compilation and tests use the tracked generated snapshot and must not require a local database.
- Use shared platform JSON/jOOQ support rather than adding feature-local jOOQ JSON converters or ad-hoc mapper configuration.
- Liquibase owns application schema, database publications, and CDC offset storage. Do not make Compose or CDC connectors create application tables.
- Preserve least-privilege PostgreSQL roles: the application writes business tables, the CDC role reads and replicates, and CDC state accesses only its offset table.
- Keep the R2DBC pool bounded, remotely validated, and environment-configurable; update its binding test when pool settings change.
- The application does not publish directly to RabbitMQ. Debezium reads PostgreSQL logical replication from the transactional outbox and publishes to the durable fanout exchange.
- Consumers own their durable queues and bindings. Development observer queues remain manual-test-only; event delivery is at-least-once, so event IDs are consumer deduplication keys.

## Tests

- Keep pure domain tests with their domain module. Keep architecture, Spring, PostgreSQL, CDC, messaging, and HTTP integration tests in `app/src/test/kotlin/com/theok/kra/app`.
- Put reusable application integration fixtures in `com.theok.kra.app.testsupport`.
- Keep integration test classes scenario-focused; extract shared lifecycle, assertions, and database access into test support before a class becomes a mixed-purpose integration suite. Name shared infrastructure fixtures after the resource they provide, not after the first feature that used them.
- Payment workflow integration tests cover a single recovery boundary each: durable cancellation before provider I/O, local cancellation without an intent, lost provider responses, delayed events after workflow closure without workflow revival, and narrowly eligible late-webhook recovery.
- Build dynamic JSON test payloads with the shared `ObjectMapper` and `ObjectNode` helpers rather than interpolated JSON strings. Keep repeated test SQL behind named database-fixture operations; leave literal SQL only where the SQL itself is the behaviour under test.
- Docker-dependent tests use Testcontainers and must skip cleanly when Docker is unavailable.
- A container listening on its port is not necessarily ready for the test. Await semantic readiness for service state required by the application, such as a Temporal namespace, before starting dependent workers.
- Keep manual feature smoke assets together under `app/src/test/manual/<feature>` and run them through their Compose-based HTTP client.
- Run `./gradlew spotlessApply` to apply Kotlin and Gradle Kotlin formatting. `spotlessCheck` runs before every `test` and `check` task, so do not bypass it. Run `./gradlew test` for the normal verification suite. Run `:platform-database:dbGenerateJooq` when migrations or generated jOOQ ownership change.
