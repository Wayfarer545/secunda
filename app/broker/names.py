"""Имена AMQP-объектов. Единственный источник для всех модулей."""

EXCHANGE_PAYMENTS = "payments"
EXCHANGE_RETRY = "payments.retry"
EXCHANGE_DLX = "payments.dlx"

QUEUE_PAYMENTS_NEW = "payments.new"
QUEUE_RETRY_2S = "payments.retry.2s"
QUEUE_RETRY_4S = "payments.retry.4s"
QUEUE_DLQ = "payments.dlq"

RK_PAYMENTS_NEW = "payments.new"
RK_RETRY_2S = "retry.2s"
RK_RETRY_4S = "retry.4s"
RK_DLQ = "payments.dlq"
