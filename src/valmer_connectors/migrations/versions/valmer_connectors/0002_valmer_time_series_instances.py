"""valmer_time_series_instances

Valmer's instances of the Markets discount-curve and Index-fixing storages
(ms-markets ADR 0047): ``valmer_connectors__discountcurvests__valmer``
(``valmer_connectors.DiscountCurvesTS.valmer``) and
``valmer_connectors__indexfixingsts__valmer``
(``valmer_connectors.IndexFixingsTS.valmer``), in the ``valmer_connectors``
namespace. They copy the core storages' columns, unique time index and foreign
keys (``msm.storage_instances.configured_time_index_instance``). Valmer's curve
and fixing producers write them from this release on; their history is copied
in by docs/time-series-instances.md.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-26 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0002'
down_revision: Union[str, Sequence[str], None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('valmer_connectors__discountcurvests__valmer',
    sa.Column('time_index', sa.DateTime(timezone=True), nullable=False),
    sa.Column('curve_identifier', sa.String(length=255), nullable=False),
    sa.Column('curve', sa.String(), nullable=False),
    sa.Column('key_nodes', sa.Text(), nullable=True),
    sa.Column('metadata_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.ForeignKeyConstraint(['curve_identifier'], ['ms_markets__curve.unique_identifier'], name=op.f('fk__valmer_connectors__discountcurvests__valmer__cur_8f8701e4e5'), ondelete='RESTRICT'),
    info={'namespace': 'valmer_connectors', 'identifier': 'valmer_connectors.DiscountCurvesTS.valmer', 'markets_storage_app': 'valmer_connectors'}
    )
    op.create_index('uix__valmer_connectors__discountcurvests__valmer__ti_55f708bbd2', 'valmer_connectors__discountcurvests__valmer', ['time_index', 'curve_identifier'], unique=True)
    op.create_table('valmer_connectors__indexfixingsts__valmer',
    sa.Column('time_index', sa.DateTime(timezone=True), nullable=False),
    sa.Column('index_identifier', sa.String(length=255), nullable=False),
    sa.Column('rate', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['index_identifier'], ['ms_markets__index.unique_identifier'], name=op.f('fk__valmer_connectors__indexfixingsts__valmer__index_a783380fd6'), ondelete='RESTRICT'),
    info={'namespace': 'valmer_connectors', 'identifier': 'valmer_connectors.IndexFixingsTS.valmer', 'markets_storage_app': 'valmer_connectors'}
    )
    op.create_index('uix__valmer_connectors__indexfixingsts__valmer__time_d7d65b52f7', 'valmer_connectors__indexfixingsts__valmer', ['time_index', 'index_identifier'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('uix__valmer_connectors__indexfixingsts__valmer__time_d7d65b52f7', table_name='valmer_connectors__indexfixingsts__valmer')
    op.drop_table('valmer_connectors__indexfixingsts__valmer')
    op.drop_index('uix__valmer_connectors__discountcurvests__valmer__ti_55f708bbd2', table_name='valmer_connectors__discountcurvests__valmer')
    op.drop_table('valmer_connectors__discountcurvests__valmer')
