"""align canonical index values with formula framework

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-21 10:08:03.515417

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0005'
down_revision: Union[str, Sequence[str], None] = '0004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM ms_markets__index_values__t_1d
                    WHERE definition_uid IS NOT NULL
                ) THEN
                    RAISE EXCEPTION
                        'valmer_connectors.0005 cannot translate legacy calculation definitions';
                END IF;
            END
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE ms_markets__index_values__t_1d
            SET metadata_json = (
                COALESCE(metadata_json::jsonb, '{}'::jsonb)
                || jsonb_build_object('quote_unit', unit)
            )::json
            WHERE index_identifier LIKE 'VALMER_CURVE_QUOTE.%'
              AND COALESCE(metadata_json::jsonb, '{}'::jsonb)
                  ->> 'quote_unit' IS NULL
            """
        )
    )
    op.drop_constraint(
        "fk__ms_markets__index_values__t_1d__definition_uid_5b419a5d21",
        "ms_markets__index_values__t_1d",
        type_="foreignkey",
    )
    op.drop_column("ms_markets__index_values__t_1d", "unit")
    op.create_foreign_key(
        "fk__ms_markets__index_values__t_1d__definition_uid_72895f3c43",
        "ms_markets__index_values__t_1d",
        "ms_markets__indexformuladefinition",
        ["definition_uid"],
        ["uid"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Downgrade schema."""
    raise RuntimeError(
        "valmer_connectors.0005 is intentionally one-way: canonical Index values "
        "cannot be restored to the removed calculation-definition/unit contract."
    )
