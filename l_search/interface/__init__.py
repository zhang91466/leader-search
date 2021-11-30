# -*- coding: UTF-8 -*-
"""
@time:2021/11/29
@author:zhangwei
@file:__init__.py
"""
from flask_restx import Api
from l_search import __title__, __version__
from .search import api_search, QueryIndex
from .mirror_data import api_mirror, ImportData, MirrorData


api = Api(title=__title__,
          version=__version__,)

api.add_namespace(api_search, path="/search")
api.add_namespace(api_mirror, path="/data")

api_search.add_resource(QueryIndex, "/index", endpoint="query_index")

api_mirror.add_resource(ImportData, "/import_one", endpoint="import_data")

api_mirror.add_resource(MirrorData, "/import_all", endpoint="mirror_data")



# Todo
# 要设计一个通用的表结构
# 要用一个强规则来应对业务库的更新
#
# 要有一个采集策略的逻辑
# 要有一个定时采集的摸快
# 要开发喂数据进来的接口

