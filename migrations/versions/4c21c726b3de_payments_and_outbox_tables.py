"""Таблицы платежей и transactional outbox.

Revision ID: 4c21c726b3de
Revises:
Create Date: 2026-08-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "4c21c726b3de"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_VARIANT = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def upgrade() -> None:
    """Создаёт таблицы payments и outbox."""
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("routing_key", sa.String(length=255), nullable=False),
        sa.Column("payload", JSON_VARIANT, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column(
            "currency",
            sa.Enum("RUB", "USD", "EUR", name="currency", native_enum=False, length=3),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("metadata", JSON_VARIANT, nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "succeeded",
                "failed",
                name="paymentstatus",
                native_enum=False,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
    )
    op.create_index(op.f("ix_payments_status"), "payments", ["status"], unique=False)


def downgrade() -> None:
    """Удаляет таблицы payments и outbox."""
    op.drop_index(op.f("ix_payments_status"), table_name="payments")
    op.drop_table("payments")
    op.drop_index(
        "ix_outbox_unpublished",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
