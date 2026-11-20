workspace "Kotlin Reference Architecture" "C4 views for the deployed system and its logical Kotlin modules." {
    model {
        client = person "API client" "Creates, queries, redeems, and pays for purchases through the HTTP API."

        kra = softwareSystem "Kotlin Reference Architecture" "A Spring Boot reference application demonstrating a functional core, imperative shell, CQS, PostgreSQL correctness, and event-driven processing." {
            application = container "Spring Boot application" "One deployable WebFlux application. It composes the feature shells and platform infrastructure; its logical modules are not separately deployed services." "Kotlin / Spring Boot / WebFlux" {
                voucher = component "Voucher feature shell" "HTTP controllers, commands, query projections, and operation-specific persistence for voucher creation and redemption." "com.theok.kra.shell.voucher"
                paymentApi = component "Payment HTTP API" "Request, cancellation, checkout, operational-retry, and Stripe webhook ingress. Controllers map HTTP only and delegate to commands or queries." "com.theok.kra.shell.payment.controller"
                paymentCommands = component "Payment commands" "Request acceptance, request cancellation, webhook verification, and authorised retry command boundaries." "com.theok.kra.shell.payment.commands"
                paymentQueries = component "Payment query projections" "PostgreSQL-backed request and checkout projections; never an alternative Temporal state source." "com.theok.kra.shell.payment.queries"
                paymentPersistence = component "Payment persistence" "Operation-sized request, payment, lifecycle-ledger, provider-event, consumer-receipt, and transactional-outbox persistence." "com.theok.kra.shell.payment.persistence"
                paymentWorkflow = component "Payment request saga" "Re-entrant Temporal workflow activities, provider reconciliation, immutable-deadline timers, and Stripe boundary." "com.theok.kra.shell.payment.workflow"
                paymentDelivery = component "Payment event delivery" "RabbitMQ receipt handling and Signal-With-Start dispatch. Broker messages are durable wake-ups only." "com.theok.kra.shell.payment.messaging"
                core = component "Functional core" "Framework-free voucher/payment rules, state machines, contracts, domain models, and DTOs." "com.theok.kra.core"
                databasePlatform = component "Database platform" "Reusable R2DBC transaction participation and jOOQ JSON support; owns no feature business rules." "com.theok.kra.platform.database"
                webPlatform = component "Web platform" "Reusable WebFlux problem-response infrastructure; owns no feature endpoints." "com.theok.kra.platform.web"
            }

            postgres = container "PostgreSQL" "Feature-owned business tables, idempotency records, provider events, and transactional outboxes." "PostgreSQL 18" {
                tags "Database"
            }
        }

        stripe = softwareSystem "Stripe" "Test-mode PaymentIntent creation, cancellation, retrieval, and signed webhook delivery."
        temporal = softwareSystem "Temporal" "Runs re-entrant payment request sagas and routes activity execution."
        debezium = softwareSystem "Debezium Server" "Reads committed outbox rows through PostgreSQL logical replication."
        rabbitmq = softwareSystem "RabbitMQ" "Durable fanout exchanges and consumer-owned durable queues; delivery is at least once."

        client -> application "Uses voucher and payment HTTP endpoints" "HTTPS/JSON"
        stripe -> application "Delivers signed PaymentIntent webhook events" "HTTPS/JSON"
        application -> postgres "Reads and writes feature state, provider events, and outbox rows" "R2DBC / jOOQ"
        application -> stripe "Creates, retrieves, and cancels named PaymentIntents" "HTTPS"
        application -> temporal "Starts payment workflows and sends signals" "gRPC"
        debezium -> postgres "Reads committed outbox changes through logical replication" "JDBC / pgoutput"
        debezium -> rabbitmq "Publishes durable event messages" "AMQP"
        rabbitmq -> application "Delivers payment workflow wake-ups" "AMQP"

        voucher -> core "Uses voucher rules and DTOs"
        paymentApi -> paymentCommands "Maps HTTP requests"
        paymentApi -> paymentQueries "Reads request projections"
        paymentCommands -> paymentPersistence "Persists command outcomes"
        paymentQueries -> databasePlatform "Uses transaction-bound jOOQ projections"
        paymentQueries -> core "Returns payment DTOs"
        paymentPersistence -> core "Uses payment rules and status policy"
        paymentPersistence -> databasePlatform "Uses transaction-bound jOOQ"
        paymentWorkflow -> paymentPersistence "Loads and changes authoritative state"
        paymentWorkflow -> core "Uses payment lifecycle rules"
        paymentWorkflow -> stripe "Creates, retrieves, and cancels named PaymentIntents"
        paymentDelivery -> paymentPersistence "Persists consumer receipts"
        paymentDelivery -> paymentWorkflow "Signals one request-scoped saga"
        paymentDelivery -> temporal "Uses Signal-With-Start"
        voucher -> databasePlatform "Uses shared transaction and jOOQ support"
        voucher -> webPlatform "Uses shared HTTP problem handling"
        paymentApi -> webPlatform "Uses shared HTTP problem handling"
    }

    views {
        systemContext kra "SystemContext" "External systems surrounding the Kotlin Reference Architecture" {
            include *
            autoLayout lr
        }

        container kra "Containers" "Deployed application, durable data, workflow, provider, and event-delivery boundaries" {
            include *
            autoLayout lr
        }

        component application "ApplicationModules" "Logical modules inside one Spring Boot deployment; arrows show the permitted dependency direction" {
            include *
            autoLayout lr
        }

        styles {
            element "Person" {
                shape person
                background #08427b
                color #ffffff
            }
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Component" {
                background #85bbf0
                color #000000
            }
            element "Database" {
                shape cylinder
            }
        }
    }
}
