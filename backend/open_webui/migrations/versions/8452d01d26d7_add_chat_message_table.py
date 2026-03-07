"""Add chat_message table

Revision ID: 8452d01d26d7
Revises: 374d2f66af06
Create Date: 2026-02-01 04:00:00.000000

"""

import time
import json
import logging
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect

log = logging.getLogger(__name__)

revision: str = "8452d01d26d7"
down_revision: Union[str, None] = "374d2f66af06"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # Step 1: Create table (skip if interrupted re-run already created it)
    if not inspector.has_table("chat_message"):
        op.create_table(
            "chat_message",
            sa.Column("id", sa.Text(), primary_key=True),
            sa.Column("chat_id", sa.Text(), nullable=False, index=True),
            sa.Column("user_id", sa.Text(), index=True),
            sa.Column("role", sa.Text(), nullable=False),
            sa.Column("parent_id", sa.Text(), nullable=True),
            sa.Column("content", sa.JSON(), nullable=True),
            sa.Column("output", sa.JSON(), nullable=True),
            sa.Column("model_id", sa.Text(), nullable=True, index=True),
            sa.Column("files", sa.JSON(), nullable=True),
            sa.Column("sources", sa.JSON(), nullable=True),
            sa.Column("embeds", sa.JSON(), nullable=True),
            sa.Column("done", sa.Boolean(), default=True),
            sa.Column("status_history", sa.JSON(), nullable=True),
            sa.Column("error", sa.JSON(), nullable=True),
            sa.Column("usage", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.BigInteger(), index=True),
            sa.Column("updated_at", sa.BigInteger()),
            sa.ForeignKeyConstraint(["chat_id"], ["chat.id"], ondelete="CASCADE"),
        )

        # Create composite indexes
        op.create_index(
            "chat_message_chat_parent_idx", "chat_message", ["chat_id", "parent_id"]
        )
        op.create_index(
            "chat_message_model_created_idx", "chat_message", ["model_id", "created_at"]
        )
        op.create_index(
            "chat_message_user_created_idx", "chat_message", ["user_id", "created_at"]
        )

    # Step 2: Backfill from existing chats

    chat_table = sa.table(
        "chat",
        sa.column("id", sa.Text()),
        sa.column("user_id", sa.Text()),
        sa.column("chat", sa.JSON()),
    )

    chat_message_table = sa.table(
        "chat_message",
        sa.column("id", sa.Text()),
        sa.column("chat_id", sa.Text()),
        sa.column("user_id", sa.Text()),
        sa.column("role", sa.Text()),
        sa.column("parent_id", sa.Text()),
        sa.column("content", sa.JSON()),
        sa.column("output", sa.JSON()),
        sa.column("model_id", sa.Text()),
        sa.column("files", sa.JSON()),
        sa.column("sources", sa.JSON()),
        sa.column("embeds", sa.JSON()),
        sa.column("done", sa.Boolean()),
        sa.column("status_history", sa.JSON()),
        sa.column("error", sa.JSON()),
        sa.column("usage", sa.JSON()),
        sa.column("created_at", sa.BigInteger()),
        sa.column("updated_at", sa.BigInteger()),
    )

    now = int(time.time())
    messages_inserted = 0
    messages_failed = 0
    chats_processed = 0

    BATCH_SIZE = 500
    last_id = ""  # empty string sorts before all UUIDs

    # Build a dialect-aware idempotent insert so re-runs skip already-inserted rows
    dialect = conn.dialect.name
    if dialect == "mysql":
        insert_stmt = sa.insert(chat_message_table).prefix_with("IGNORE")
    elif dialect == "sqlite":
        insert_stmt = sqlite_dialect.insert(chat_message_table).on_conflict_do_nothing()
    else:
        # PostgreSQL (including Aurora) supports ON CONFLICT DO NOTHING via dialect-specific insert
        insert_stmt = pg_dialect.insert(chat_message_table).on_conflict_do_nothing()

    while True:
        batch = conn.execute(
            sa.select(chat_table.c.id, chat_table.c.user_id, chat_table.c.chat)
            .where(
                ~chat_table.c.user_id.like("shared-%"),
                chat_table.c.id > last_id,
            )
            .order_by(chat_table.c.id)
            .limit(BATCH_SIZE)
        ).fetchall()

        if not batch:
            break

        rows_to_insert = []

        for chat_row in batch:
            chat_id = chat_row[0]
            user_id = chat_row[1]
            chat_data = chat_row[2]

            if not chat_data:
                continue

            # Handle both string and dict chat data
            if isinstance(chat_data, str):
                try:
                    chat_data = json.loads(chat_data)
                except Exception:
                    continue

            history = chat_data.get("history", {})
            messages = history.get("messages", {})

            for message_id, message in messages.items():
                if not isinstance(message, dict):
                    continue

                role = message.get("role")
                if not role:
                    continue

                timestamp = message.get("timestamp", now)

                # Normalize timestamp: convert ms to seconds, validate range
                if timestamp > 10_000_000_000:
                    timestamp = timestamp // 1000
                # Must be after 2020 and not too far in the future
                if timestamp < 1577836800 or timestamp > now + 86400:
                    timestamp = now

                rows_to_insert.append(
                    dict(
                        id=f"{chat_id}-{message_id}",
                        chat_id=chat_id,
                        user_id=user_id,
                        role=role,
                        parent_id=message.get("parentId"),
                        content=message.get("content"),
                        output=message.get("output"),
                        model_id=message.get("model"),
                        files=message.get("files"),
                        sources=message.get("sources"),
                        embeds=message.get("embeds"),
                        done=message.get("done", True),
                        status_history=message.get("statusHistory"),
                        error=message.get("error"),
                        created_at=timestamp,
                        updated_at=timestamp,
                    )
                )

        if rows_to_insert:
            sp = conn.begin_nested()
            try:
                conn.execute(insert_stmt, rows_to_insert)
                sp.commit()
                messages_inserted += len(rows_to_insert)
            except Exception:
                sp.rollback()
                for row in rows_to_insert:
                    sp2 = conn.begin_nested()
                    try:
                        conn.execute(insert_stmt.values(**row))
                        sp2.commit()
                        messages_inserted += 1
                    except Exception as e:
                        sp2.rollback()
                        messages_failed += 1
                        log.warning(f"Failed to insert message {row['id']}: {e}")

        chats_processed += len(batch)
        last_id = batch[-1][0]

        if (chats_processed // BATCH_SIZE) % 10 == 0:
            log.info(
                f"Backfill progress: {chats_processed} chats processed, "
                f"{messages_inserted} messages inserted, {messages_failed} failed"
            )

    log.info(
        f"Backfilled {messages_inserted} messages into chat_message table "
        f"({chats_processed} chats processed, {messages_failed} failed)"
    )


def downgrade() -> None:
    op.drop_index("chat_message_user_created_idx", table_name="chat_message")
    op.drop_index("chat_message_model_created_idx", table_name="chat_message")
    op.drop_index("chat_message_chat_parent_idx", table_name="chat_message")
    op.drop_table("chat_message")
