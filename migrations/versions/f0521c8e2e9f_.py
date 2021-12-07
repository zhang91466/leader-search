"""empty message

Revision ID: f0521c8e2e9f
Revises: 
Create Date: 2021-12-06 17:08:32.251314

"""
from alembic import op
import sqlalchemy as sa
from l_search.models import FullTextIndex
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'f0521c8e2e9f'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('full_text_index',
    sa.Column('id', sa.String(length=300), nullable=False),
    sa.Column('domain', sa.String(length=150), nullable=False),
    sa.Column('db_object_type', sa.String(length=150), nullable=False),
    sa.Column('block_name', sa.String(length=500), nullable=False),
    sa.Column('block_key', sa.String(length=500), nullable=False),
    sa.Column('db_name', sa.String(length=500), nullable=False),
    sa.Column('table_name', sa.String(length=500), nullable=False),
    sa.Column('table_primary_id', sa.String(length=150), nullable=False),
    sa.Column('table_extract_col', sa.String(length=150), nullable=False),
    sa.Column('row_content', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )

    op.execute("ALTER TABLE full_text_index ADD FULLTEXT (row_content) WITH PARSER ngram")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('full_text_index')
    # ### end Alembic commands ###
