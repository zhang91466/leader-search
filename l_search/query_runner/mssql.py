# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:mssql
"""
from l_search.query_runner import BasicQueryRunner
from l_search import settings
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads
from l_search.utils.logger import Logger

logger = Logger()


class Mssql(BasicQueryRunner):

    def extract(self, table_info):
        logger.debug("%s start extract" % table_info.table_name)

        extract_stmt, geo_col = self.select_stmt(table_info=table_info)

        if geo_col:
            extract_stmt = extract_stmt % {
                "geo_col": "%s.STGeometryN(1).ToString() as %s" % (geo_col, settings.GEO_COLUMN_NAME_STAG)}

        for count, partial_df in enumerate(pd.read_sql(extract_stmt, self.db_engine, chunksize=self.chunk_size)):

            to_db_para = {"con": self.db_engine,
                          "if_exists": "append",
                          "schema": settings.ODS_STAG_SCHEMA_NAME,
                          "name": table_info.table_name}

            if geo_col:
                geometry = [loads(x) for x in partial_df[settings.GEO_COLUMN_NAME_STAG]]

                del partial_df[settings.GEO_COLUMN_NAME_STAG]

                partial_df = gpd.GeoDataFrame(partial_df, geometry=geometry)

                partial_df.to_postgis(**to_db_para)
            else:
                partial_df.to_sql(**to_db_para)

        logger.debug("%s extract end" % table_info.table_name)

        return geo_col
