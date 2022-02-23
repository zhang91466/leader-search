# -*- coding: utf-8 -*-
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from l_search import models


class DBSession:
    def __init__(self, connection_info):
        engine_connect_string = None
        self.connect_info = connection_info

        if type in ["greenplum", "postgresql"]:
            # postgresql: // scott: tiger @ localhost:5432 / mydatabase
            connect_prefix = "postgresql+psycopg2"
            remark = ""

        elif type in ["mysql"]:
            # mysql: // scott: tiger @ localhost:5432 / mydatabase?charset=utf8
            connect_prefix = "mysql"
            remark = "?character_set_server=utf8mb4"

        elif type in ["mariadb"]:
            # mysql: // scott: tiger @ localhost:5432 / mydatabase?charset=utf8
            connect_prefix = "mysql"
            remark = "?charset=utf8"

        engine_connect_string = '%s://%s:%s@%s:%s/%s%s' % (connect_prefix,
                                                           self.connect_info.account,
                                                           self.connect_info.pwd,
                                                           self.connect_info.host,
                                                           self.connect_info.port,
                                                           self.connect_info.default_db,
                                                           remark)

        self.engine = create_engine(engine_connect_string)
        self.sessions = None

        self.new_session = Session(self.engine, future=True)

    # 1.0的老办法进行sql查询

    def connect_init(self):
        DBSession = sessionmaker(bind=self.engine)
        self.sessions = DBSession()

    @contextmanager
    def session_maker(self):
        session = self.sessions
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    # 2.0的新版本进行sql查询（1.4开始支持）
    def get_session(self):
        return self.new_session
