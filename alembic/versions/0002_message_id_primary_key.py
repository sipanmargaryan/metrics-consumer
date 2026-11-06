"""Deduplicate order receipts by message ID across all consumer groups."""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # PostgreSQL transactional DDL rolls back if existing IDs are duplicated.
    # Do not delete any existing receipts to make the constraint succeed.
    op.drop_constraint("received_orders_pkey", "received_orders", type_="primary")
    op.create_primary_key("received_orders_pkey", "received_orders", ["message_id"])
    op.drop_column("received_orders", "consumer_name")


def downgrade() -> None:
    op.add_column(
        "received_orders",
        sa.Column(
            "consumer_name", sa.String(200), nullable=False, server_default="legacy"
        ),
    )
    op.drop_constraint("received_orders_pkey", "received_orders", type_="primary")
    op.create_primary_key(
        "received_orders_pkey", "received_orders", ["consumer_name", "message_id"]
    )
    op.alter_column("received_orders", "consumer_name", server_default=None)
