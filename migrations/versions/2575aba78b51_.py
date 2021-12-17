"""empty message

Revision ID: 2575aba78b51
Revises: 1a6a8cf9b4a8
Create Date: 2021-12-17 15:25:20.814973

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '2575aba78b51'
down_revision = '1a6a8cf9b4a8'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('extract_data_info', sa.Column('table_primary_id_is_int', sa.Boolean(), nullable=False))
    op.alter_column('extract_data_info', 'is_entity',
               existing_type=mysql.TINYINT(display_width=1),
               nullable=False)
    op.alter_column('extract_data_info', 'is_full_text_index',
               existing_type=mysql.TINYINT(display_width=1),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('extract_data_info', 'is_full_text_index',
               existing_type=mysql.TINYINT(display_width=1),
               nullable=True)
    op.alter_column('extract_data_info', 'is_entity',
               existing_type=mysql.TINYINT(display_width=1),
               nullable=True)
    op.drop_column('extract_data_info', 'table_primary_id_is_int')
    # ### end Alembic commands ###
