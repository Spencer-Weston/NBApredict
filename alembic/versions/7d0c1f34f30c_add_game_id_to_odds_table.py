"""Add game_id to odds_table

Revision ID: 7d0c1f34f30c
Revises: 
Create Date: 2019-03-15 11:04:27.164080

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7d0c1f34f30c'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('odds_2019', sa.Column("game_id", sa.Integer, sa.ForeignKey("sched_2019.id")))


def downgrade():
    op.drop_column("account", "game_id")
