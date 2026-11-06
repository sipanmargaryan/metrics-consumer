"""Create durable order receipts and their duplicate guard."""

import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "received_orders",
        sa.Column("consumer_name", sa.String(200), primary_key=True),
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("product_id", sa.BigInteger(), nullable=False),
        sa.Column("quantity", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.BigInteger(), nullable=False),
        sa.Column("order_type", sa.String(32), nullable=False),
        sa.Column("order_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("topic", sa.String(249), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_received_orders_order_id", "received_orders", ["order_id"])


def downgrade() -> None:
    op.drop_index("ix_received_orders_order_id", table_name="received_orders")
    op.drop_table("received_orders")
