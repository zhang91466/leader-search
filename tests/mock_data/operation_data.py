# -*- coding: UTF-8 -*-
"""
@time:2022/4/19
@author:simonzhang
@file:operation_data
"""
from l_search.models.extract_table_models import DBSession
from l_search.utils.logger import Logger
from l_search import models
from l_search.app import create_app
from l_search.models import db
from tests import BaseTestCase
from tests.factories import Factory
import random
import string
import time

logger = Logger()

class TestAddMockData(BaseTestCase):

    def setUp(self):
        self.app = create_app()
        self.db = db
        self.app.config["TESTING"] = True
        self.app_ctx = self.app.app_context()
        self.app_ctx.push()
        db.session.close()
        self.client = self.app.test_client()
        self.factory = Factory()

    def test_add_data(self):
        def execute(sql):
            logger.info("Execute SQL: %s" % sql)
            connection_session.execute(sql)
            connection_session.commit()

        # connection_info = self.factory.create_db_connect(return_dict=False)
        connection_info = models.DBConnect.get_by_domain(connection_id=1)
        connection = DBSession(connection_info=connection_info[0])
        connection_session = connection.session

        table_name = "L_SEARCH_TEST"

        drop_table_stmt = """
        DROP TABLE dbo.%(table_name)s """ % {"table_name": table_name}

        create_table_stmt = """
        create table dbo.%(table_name)s (
        ID INT IDENTITY(1,1),
        K_NAME varchar(50),
        START_TIME datetime default CURRENT_TIMESTAMP,
        V_VALUE INT,
        UPDATE_TS datetime default CURRENT_TIMESTAMP,
        )""" % {"table_name": table_name}

        insert_stmt = """insert into dbo.%(table_name)s(K_NAME,V_VALUE) select '%(k_name)s', %(v_value)d"""

        update_stmt = """
        update dbo.%(table_name)s 
        set V_VALUE=%(v_value)d, UPDATE_TS=CURRENT_TIMESTAMP 
        where id in (%(id_list_str)s)"""

        delete_stmt = """delete from dbo.%(table_name)s where id in (%(id_list_str)s)"""

        try:
            execute(drop_table_stmt)
        except:
            pass
        execute(create_table_stmt)

        i = 1

        while True:
            k_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
            v_value = random.randint(0, 100)

            execute(insert_stmt % {"table_name": table_name,
                                   "k_name": k_name,
                                   "v_value": v_value
                                   })

            if i >= 100:

                if random.randint(0, 1000) > 900:
                    execute(update_stmt % {"table_name": table_name,
                                           "id_list_str": ",".join(
                                               [str(random.randint(1, i)) for x in range(random.randint(1, 10))]),
                                           "v_value": v_value
                                           })
                elif random.randint(0, 1000) > 960:
                    execute(delete_stmt % {"table_name": table_name,
                                           "id_list_str": ",".join(
                                               [str(random.randint(1, i)) for x in range(random.randint(1, 10))])
                                           })

            time.sleep(random.randint(1, 3))

            i += 1
