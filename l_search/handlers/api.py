# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:api
"""
from flask_restx import Api
from l_search import __name__, __version__
from .search import api_search, QueryIndex
from .mirror_data import api_mirror, ImportData


api = Api(title=__name__,
          version=__version__,)

api.add_namespace(api_search, path="/search")
api.add_namespace(api_mirror, path="/data")

api_search.add_resource(QueryIndex, "/index", endpoint="query_index")

api_mirror.add_resource(ImportData, "/import_one", endpoint="import_data")



