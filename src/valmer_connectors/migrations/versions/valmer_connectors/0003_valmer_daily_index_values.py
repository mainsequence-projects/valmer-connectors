"""valmer_daily_index_values

Valmer's own canonical daily Index-value table (ms-markets ADR 0047, decision
4): ``valmer_connectors__index_values__t_1d``
(``valmer_connectors.IndexValuesTS.1d``) in the ``valmer_connectors``
namespace, built with ``configured_index_values_storage(cadence="1d",
namespace=, storage_app=, base=)``: the columns, foreign keys, grain and indexes
of the Markets table ``ms_markets__index_values__t_1d``, which revision 0001
created and this provider keeps registering until every Environment has moved.
Expand only: nothing writes it until ``VALMER_TIME_SERIES_STORAGE=valmer`` is
set, after the copy in docs/time-series-instances.md.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-26 18:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '0003'
down_revision: Union[str, Sequence[str], None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('valmer_connectors__index_values__t_1d',
    sa.Column('time_index', sa.DateTime(timezone=True), nullable=False),
    sa.Column('index_identifier', sa.String(length=255), nullable=False),
    sa.Column('value', sa.Float(), nullable=False),
    sa.Column('definition_uid', sa.Uuid(), nullable=True),
    sa.Column('observation_status', sa.String(length=32), nullable=True),
    sa.Column('source_as_of', sa.DateTime(timezone=True), nullable=True),
    sa.Column('metadata_json', sa.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['definition_uid'], ['ms_markets__indexformuladefinition.uid'], name=op.f('fk__valmer_connectors__index_values__t_1d__definitio_cd5e2dca89'), ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['index_identifier'], ['ms_markets__index.unique_identifier'], name=op.f('fk__valmer_connectors__index_values__t_1d__index_ide_cf289cff5b'), ondelete='RESTRICT'),
    info={'namespace': 'valmer_connectors', 'identifier': 'valmer_connectors.IndexValuesTS.1d', 'markets_storage_app': 'valmer_connectors'}
    )
    op.create_index('ix__valmer_connectors__index_values__t_1d__definition_uid', 'valmer_connectors__index_values__t_1d', ['definition_uid'], unique=False)
    op.create_index('ix__valmer_connectors__index_values__t_1d__index_ide_1aea0ce1f9', 'valmer_connectors__index_values__t_1d', ['index_identifier', 'time_index'], unique=False)
    op.create_index('ix__valmer_connectors__index_values__t_1d__observation_status', 'valmer_connectors__index_values__t_1d', ['observation_status'], unique=False)
    op.create_index('uix__valmer_connectors__index_values__t_1d__time_ind_df6033b0fc', 'valmer_connectors__index_values__t_1d', ['time_index', 'index_identifier'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('uix__valmer_connectors__index_values__t_1d__time_ind_df6033b0fc', table_name='valmer_connectors__index_values__t_1d')
    op.drop_index('ix__valmer_connectors__index_values__t_1d__observation_status', table_name='valmer_connectors__index_values__t_1d')
    op.drop_index('ix__valmer_connectors__index_values__t_1d__index_ide_1aea0ce1f9', table_name='valmer_connectors__index_values__t_1d')
    op.drop_index('ix__valmer_connectors__index_values__t_1d__definition_uid', table_name='valmer_connectors__index_values__t_1d')
    op.drop_table('valmer_connectors__index_values__t_1d')
