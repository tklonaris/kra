# Payment API access

## Status

Implemented. `oidc` is the default and requires the standard Spring resource-server issuer
configuration (`SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_ISSUER_URI`). `trusted-demo` is an
explicit single-owner local/test mode selected with `PAYMENTS_API_ACCESS_MODE=trusted-demo`; it is
not authentication and must not be used outside a trusted environment. It requires an explicit
`local` or `test` Spring profile and the application fails at startup otherwise.

## Problem

Payment request IDs must not authorize access. In OIDC mode, `GET /payment-requests/{id}`,
`POST /payment-requests/{id}/cancellations`, and `GET /payment-requests/{id}/setup` are scoped to the authenticated
owner. The setup endpoint can return a Stripe client secret, so an identifier is never treated as
an access credential.

## Implemented design

1. Configure Spring Security WebFlux as an OIDC resource server when `payments.api-access.mode` is
   `oidc`. The issuer and subject claims form the durable owner identifier.
2. In `trusted-demo` mode, resolve the configured single owner without a bearer token. Startup
   permits this mode only when the explicit `local` or `test` Spring profile is active.
3. Do not accept an owner or
   customer identifier in request JSON or headers controlled by the caller.
4. Make the subject a durable payment ownership attribute with the framework-free `PaymentOwnerId`
   value type in `core.payment` because ownership affects payment access and idempotency semantics.
5. Pass that owner value from the controller to payment commands and queries. Controllers remain
   responsible only for HTTP/principal mapping; they do not make persistence calls.
6. Scope payment-request lookups, cancellation requests, details, and setup-secret reads by both request ID
   and owner ID. Return `404 Not Found` for a payment outside the caller's scope to avoid revealing
   whether another owner's payment request exists.
7. Derive ownership during creation from the resolved owner, then persist it with the payment request.
   Do not rely on the client-supplied `purchaseReference` for authorization.
8. Keep `POST /payment-requests/stripe/webhook` unauthenticated at the application layer, but continue to
   require Stripe signature verification. Webhooks are provider ingress, not customer access.
9. Reserve `POST /internal/payment-requests/{paymentRequestId}/retries` for operators. It always requires an
   OIDC JWT authority of `SCOPE_payments:retry`; trusted-demo access never bypasses this scope.

## Schema and idempotency

- The payment-request schema has a non-null `owner_id`; each new payment request receives its owner from the
  authenticated subject or the explicitly configured trusted-demo owner.
- Idempotency and purchase-reference constraints are owner-scoped:
  `(owner_id, idempotency_key)` and `(owner_id, purchase_reference)`.
- Stripe provider intent IDs and provider event IDs remain globally unique.
- The payment adapter owns this schema as its Liquibase baseline and generated jOOQ snapshot.

## Verified behaviour

- OIDC HTTP integration tests use no access-mode override, confirming the production default, and
  cover authenticated create, read, cancellation, setup retrieval, cross-owner `404` results,
  owner-scoped idempotency, and unauthenticated `401` results.
- OIDC startup integration tests run without the test decoder, proving that the default mode fails
  without issuer configuration and starts from issuer discovery when it is supplied.
- A trusted-demo HTTP integration test verifies that a request without a bearer token uses the
  configured single owner for create, read, setup retrieval, and cancellation.
- Stripe webhook HTTP tests confirm the endpoint remains public but rejects an invalid signature.
- The test JWT decoder is a test-only deterministic decoder; production still uses the configured
  OIDC issuer and its normal JWT validation.
