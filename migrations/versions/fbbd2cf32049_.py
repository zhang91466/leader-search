"""empty message

Revision ID: fbbd2cf32049
Revises: 
Create Date: 2022-03-03 14:47:05.021576

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fbbd2cf32049'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('db_connect_info',
                    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('domain', sa.String(length=255), nullable=False),
                    sa.Column('db_type', sa.String(length=255), nullable=False),
                    sa.Column('default_db', sa.String(length=255), nullable=False),
                    sa.Column('db_schema', sa.String(length=255), nullable=True),
                    sa.Column('host', sa.String(length=255), nullable=False),
                    sa.Column('port', sa.Integer(), nullable=False),
                    sa.Column('account', sa.String(length=255), nullable=False),
                    sa.Column('pwd', sa.String(length=255), nullable=False),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('full_text_index',
                    sa.Column('id', sa.String(length=300), nullable=False),
                    sa.Column('extract_data_info_id', sa.Integer(), nullable=False),
                    sa.Column('block_name', sa.String(length=500), nullable=False),
                    sa.Column('block_key', sa.String(length=500), nullable=False),
                    sa.Column('row_content', sa.Text(), nullable=False),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('table_info',
                    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('id', sa.String(length=50), nullable=False),
                    sa.Column('connection_id', sa.Integer(), nullable=False),
                    sa.Column('table_name', sa.String(length=500), nullable=False),
                    sa.Column('table_name_alias', sa.String(length=500), nullable=False),
                    sa.Column('need_extract', sa.Boolean(), nullable=True),
                    sa.Column('is_entity', sa.Boolean(), nullable=False),
                    sa.Column('table_extract_col', sa.String(length=150), nullable=True),
                    sa.Column('latest_extract_date', sa.DateTime(), nullable=True),
                    sa.ForeignKeyConstraint(['connection_id'], ['db_connect_info.id'], ),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_index('table_info_connection_id_table_name_index', 'table_info', ['connection_id', 'table_name'],
                    unique=True)
    op.create_table('table_detail',
                    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('id', sa.String(length=50), nullable=False),
                    sa.Column('table_info_id', sa.String(length=50), nullable=False),
                    sa.Column('column_name', sa.String(length=255), nullable=False),
                    sa.Column('column_type', sa.String(length=255), nullable=False),
                    sa.Column('column_type_length', sa.String(length=255), nullable=False),
                    sa.Column('column_comment', sa.String(length=255), nullable=True),
                    sa.Column('column_position', sa.Integer(), nullable=False),
                    sa.Column('is_extract', sa.Boolean(), nullable=False),
                    sa.Column('is_primary', sa.Boolean(), nullable=False),
                    sa.Column('is_system_col', sa.Boolean(), nullable=False),
                    sa.ForeignKeyConstraint(['table_info_id'], ['table_info.id'], ),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_index('table_detail_table_info_id_column_name_index', 'table_detail', ['table_info_id', 'column_name'],
                    unique=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('table_detail_table_info_id_column_name_index', table_name='table_detail')
    op.drop_table('table_detail')
    op.drop_index('table_info_connection_id_table_name_index', table_name='table_info')
    op.drop_table('table_info')
    op.drop_table('full_text_index')
    op.drop_table('db_connect_info')
    # ### end Alembic commands ###
