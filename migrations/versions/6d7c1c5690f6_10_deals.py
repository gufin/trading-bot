"""10_deals

Revision ID: 6d7c1c5690f6
Revises: 144a19ec473e
Create Date: 2023-11-19 18:38:01.773987

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6d7c1c5690f6'
down_revision: Union[str, None] = '144a19ec473e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('deals',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('ticker_id', sa.BIGINT(), nullable=False),
    sa.Column('buy_order', sa.UUID(), nullable=False),
    sa.Column('sell_order', sa.UUID(), nullable=True),
    sa.ForeignKeyConstraint(['buy_order'], ['orders.id'], ),
    sa.ForeignKeyConstraint(['sell_order'], ['orders.id'], ),
    sa.ForeignKeyConstraint(['ticker_id'], ['tickers.ticker_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('deals')
    # ### end Alembic commands ###
