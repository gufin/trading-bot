"""01_initial-db

Revision ID: 91332ed08934
Revises: 
Create Date: 2023-11-04 22:36:05.258886

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '91332ed08934'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('strategies',
    sa.Column('strategy_id', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('strategy_id')
    )
    op.drop_table('strategy')
    op.alter_column('candles', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('candles', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=False)
    op.alter_column('candles', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=False)
    op.alter_column('candles', 'open',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.alter_column('candles', 'high',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.alter_column('candles', 'low',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.alter_column('candles', 'close',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.drop_constraint('unique_candle_combination', 'candles', type_='unique')
    op.create_unique_constraint('unique_candle', 'candles', ['ticker_id', 'interval', 'timestamp_column'])
    op.alter_column('ema', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('ema', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=False)
    op.alter_column('ema', 'span',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('ema', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=False)
    op.alter_column('ema', 'ema',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.alter_column('ema', 'atr',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=False)
    op.drop_constraint('ema_ticker_id_interval_span_timestamp_column_key', 'ema', type_='unique')
    op.create_unique_constraint('unique_ema', 'ema', ['ticker_id', 'interval', 'timestamp_column'])
    op.alter_column('ema_cross', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('ema_cross', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=False)
    op.alter_column('ema_cross', 'span',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('ema_cross', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=False)
    op.drop_constraint('ema_cross_ticker_id_interval_span_timestamp_column_key', 'ema_cross', type_='unique')
    op.create_unique_constraint('unique_ema_cross_combination', 'ema_cross', ['ticker_id', 'interval', 'span', 'timestamp_column'])
    op.alter_column('ema_to_calc', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=False)
    op.alter_column('ema_to_calc', 'span',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('tickers', 'disable',
               existing_type=sa.BOOLEAN(),
               nullable=True,
               existing_server_default=sa.text('false'))
    op.add_column('user_strategies', sa.Column('user_strategy_id', sa.BIGINT(), autoincrement=True, nullable=False))
    op.alter_column('user_strategies', 'user_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('user_strategies', 'strategy_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('user_strategies', 'timeframe_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.drop_constraint('user_strategies_strategy_id_fkey', 'user_strategies', type_='foreignkey')
    op.create_foreign_key(None, 'user_strategies', 'strategies', ['strategy_id'], ['strategy_id'])
    op.drop_column('user_strategies', 'user_strategies_id')
    op.add_column('user_tickers', sa.Column('user_ticker_id', sa.BIGINT(), autoincrement=True, nullable=False))
    op.alter_column('user_tickers', 'user_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('user_tickers', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.drop_column('user_tickers', 'user_tickers_id')
    op.alter_column('users', 'name',
               existing_type=sa.VARCHAR(length=32),
               nullable=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('users', 'name',
               existing_type=sa.VARCHAR(length=32),
               nullable=True)
    op.add_column('user_tickers', sa.Column('user_tickers_id', sa.BIGINT(), autoincrement=True, nullable=False))
    op.alter_column('user_tickers', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.alter_column('user_tickers', 'user_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.drop_column('user_tickers', 'user_ticker_id')
    op.add_column('user_strategies', sa.Column('user_strategies_id', sa.BIGINT(), autoincrement=True, nullable=False))
    op.drop_constraint(None, 'user_strategies', type_='foreignkey')
    op.create_foreign_key('user_strategies_strategy_id_fkey', 'user_strategies', 'strategy', ['strategy_id'], ['strategy_id'])
    op.alter_column('user_strategies', 'timeframe_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.alter_column('user_strategies', 'strategy_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.alter_column('user_strategies', 'user_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.drop_column('user_strategies', 'user_strategy_id')
    op.alter_column('tickers', 'disable',
               existing_type=sa.BOOLEAN(),
               nullable=False,
               existing_server_default=sa.text('false'))
    op.alter_column('ema_to_calc', 'span',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('ema_to_calc', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=True)
    op.drop_constraint('unique_ema_cross_combination', 'ema_cross', type_='unique')
    op.create_unique_constraint('ema_cross_ticker_id_interval_span_timestamp_column_key', 'ema_cross', ['ticker_id', 'interval', 'span', 'timestamp_column'])
    op.alter_column('ema_cross', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=True)
    op.alter_column('ema_cross', 'span',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('ema_cross', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=True)
    op.alter_column('ema_cross', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.drop_constraint('unique_ema', 'ema', type_='unique')
    op.create_unique_constraint('ema_ticker_id_interval_span_timestamp_column_key', 'ema', ['ticker_id', 'interval', 'span', 'timestamp_column'])
    op.alter_column('ema', 'atr',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('ema', 'ema',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('ema', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=True)
    op.alter_column('ema', 'span',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('ema', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=True)
    op.alter_column('ema', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.drop_constraint('unique_candle', 'candles', type_='unique')
    op.create_unique_constraint('unique_candle_combination', 'candles', ['ticker_id', 'interval', 'timestamp_column'])
    op.alter_column('candles', 'close',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('candles', 'low',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('candles', 'high',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('candles', 'open',
               existing_type=sa.NUMERIC(precision=10, scale=3),
               nullable=True)
    op.alter_column('candles', 'timestamp_column',
               existing_type=postgresql.TIMESTAMP(),
               nullable=True)
    op.alter_column('candles', 'interval',
               existing_type=sa.VARCHAR(length=64),
               nullable=True)
    op.alter_column('candles', 'ticker_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.create_table('strategy',
    sa.Column('strategy_id', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('name', sa.VARCHAR(length=64), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('strategy_id', name='strategy_pkey')
    )
    op.drop_table('strategies')
    # ### end Alembic commands ###
