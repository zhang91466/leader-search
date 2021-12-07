# -*- coding: UTF-8 -*-
"""
@time:2021/11/29
@author:zhangwei
@file:__init__.py
"""
from flask_restx import Api
from l_search import __title__, __version__
from .search import api_search, QueryIndex
from .mirror_data import (api_mirror,
                          ExtractToFullTextIndexTable,
                          ConnectionInfo,
                          SyncMeta)


api = Api(title=__title__,
          version=__version__,)

api.add_namespace(api_search, path="/search")
api.add_namespace(api_mirror, path="/mirror_data")

api_search.add_resource(QueryIndex, "/index", endpoint="query_index")

api_mirror.add_resource(ExtractToFullTextIndexTable, "/extract/data_to_full_index", endpoint="mirror_data")

api_mirror.add_resource(ConnectionInfo, "/meta/connection", endpoint="connection_info")

api_mirror.add_resource(SyncMeta, "/meta/sync", endpoint="SyncMeta")



# Todo
# 要设计一个通用的表结构
# 要用一个强规则来应对业务库的更新
#
# 要有一个采集策略的逻辑
# 要有一个定时采集的摸快
# 要开发喂数据进来的接口

