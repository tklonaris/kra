# Structurizr architecture views

`workspace.dsl` is the editable C4 source of truth. Structurizr Local generates `.structurizr/` and `workspace.json`; both are local artifacts and are ignored by Git. Its views explain stable ownership and deployment boundaries; use the Mermaid diagrams in [the payment workflow design](../../design/payment-workflow.md) for request-level, deadline, and failure-recovery behaviour.

| View key | Level | Purpose |
| --- | --- | --- |
| `SystemContext` | System | API client, the application, Stripe, Temporal, Debezium, and RabbitMQ. |
| `Containers` | Container | The single Spring Boot deployment, PostgreSQL, and its external workflow, provider, and event-delivery dependencies. |
| `ApplicationModules` | Component | Voucher and payment feature ownership, plus the shared core and platform modules, inside the one deployment. |

## View locally

Docker Desktop is the only prerequisite. From the repository root:

```shell
./gradlew structurizr
```

The Gradle task validates `workspace.dsl`, replaces only the `kra-structurizr` container, and starts Structurizr Local at <http://localhost:8081>. It does not start or modify the application stack.

```shell
./gradlew structurizr -PstructurizrPort=8090
./gradlew structurizrStop
```

Keep this workspace focused on C4-level ownership and deployment views. Do not model each request, retry, or payment decision here; update the linked Mermaid design diagrams with those behavioural changes.
