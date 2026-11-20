package com.theok.kra.shell.payment.workflow.operations

import com.stripe.StripeClient
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentCancellationReconciler
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentIntentStatusRefresher
import com.theok.kra.shell.payment.workflow.operations.lifecycle.PaymentIntentStatusUpdateApplier
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentCancellation
import com.theok.kra.shell.payment.workflow.stripe.StripePaymentGateway
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowControlState
import java.util.UUID
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/** Reconciles provider status, cancellation, checkout expiry, and processing-deadline outcomes. */
@Component
@Profile("payments")
class PaymentLifecycleReconciliationOperations
@Autowired
constructor(
    private val workflowPersister: PaymentWorkflowPersister,
    stripePaymentGateway: StripePaymentGateway,
) {
  constructor(
      workflowPersister: PaymentWorkflowPersister,
      stripeClient: StripeClient,
  ) : this(
      workflowPersister,
      StripePaymentGateway(stripeClient),
  )

  private val paymentIntentStatusUpdateApplier = PaymentIntentStatusUpdateApplier(workflowPersister)
  private val paymentIntentStatusRefresher =
      PaymentIntentStatusRefresher(
          workflowPersister,
          stripePaymentGateway,
          paymentIntentStatusUpdateApplier,
      )
  private val cancellationReconciler =
      PaymentCancellationReconciler(
          workflowPersister,
          StripePaymentCancellation(
              workflowPersister,
              stripePaymentGateway,
              paymentIntentStatusRefresher,
          ),
          paymentIntentStatusRefresher,
      )

  suspend fun reconcileCancellation(
      paymentId: UUID,
      webhookConsumerEventId: UUID? = null,
  ): PaymentWorkflowControlState =
      cancellationReconciler.reconcile(paymentId, webhookConsumerEventId)

  suspend fun refreshIntentStatus(
      paymentId: UUID,
      webhookConsumerEventId: UUID? = null,
      webhookStripeEventId: String? = null,
  ): PaymentWorkflowControlState =
      paymentIntentStatusRefresher.refresh(
          paymentId,
          webhookConsumerEventId,
          webhookStripeEventId,
      )

  suspend fun expire(paymentId: UUID): PaymentWorkflowControlState =
      cancellationReconciler.reconcile(paymentId, null)

  suspend fun expireProcessingDeadline(paymentId: UUID): PaymentWorkflowControlState =
      paymentIntentStatusRefresher.expireProcessingDeadline(paymentId, null)
}
