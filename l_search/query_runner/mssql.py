# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:mssql
"""
from l_search.query_runner import BasicQueryRunner
from l_search import settings
from l_search.query_runner import register
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads
from werkzeug.exceptions import BadRequest
from l_search.utils.logger import Logger

logger = Logger()


class Mssql(BasicQueryRunner):

    def increment_where_stmt(self):
        if self.table_info.latest_extract_date is not None:
            where_stmt = " where %(update_ts_col)s > '%(latest_update_ts)s'" % {
                "update_ts_col": self.table_info.table_extract_col,
                "latest_update_ts": self.table_info.latest_extract_date}
        else:
            where_stmt = ""

        return where_stmt

    @staticmethod
    def geo_col_to_str(geo_col):
        return "%s.STGeometryN(1).ToString() as %s" % (geo_col, settings.GEO_COLUMN_NAME_STAG)

    @classmethod
    def get_check_geo_z_stat(cls, geo_col, table_name):
        geo_str = cls.geo_col_to_str(geo_col=geo_col)
        return "select %(col)s from %(tab)s where %(col_no_as)s is not null" % {"col": geo_str,
                                                                                "tab": table_name,
                                                                                "col_no_as": geo_str.split("as")[0]}

    def extract(self, increment=True):
        table_name = self.table_info.entity_table_name()
        logger.info("%s start extract" % table_name)

        extract_stmt, geo_col = self.select_stmt()

        if geo_col:
            extract_stmt = extract_stmt % {
                "geo_col": self.geo_col_to_str(geo_col=geo_col)}

        if increment is True:
            extract_stmt = extract_stmt + self.increment_where_stmt()

        logger.info("%s extract stmt %s" % (table_name, extract_stmt))

        try:
            for count, partial_df in enumerate(
                    pd.read_sql(extract_stmt, self.source_db_engine, chunksize=self.chunk_size)):

                if len(partial_df) == 0:
                    # 没有数据直接退出
                    break

                to_db_para = {"con": self.db_engine,
                              "if_exists": "append",
                              "schema": settings.ODS_STAG_SCHEMA_NAME,
                              "name": table_name,
                              "index": False}

                partial_df = self.df_structure_arrangement(insert_data_df=partial_df)

                logger.info("%s extracting loop %d" % (table_name, count))
                if geo_col:
                    # geometry = [loads(x) for x in partial_df[settings.GEO_COLUMN_NAME_STAG]]
                    geometry = []
                    for x in partial_df[settings.GEO_COLUMN_NAME_STAG]:
                        if x is None:
                            geometry.append(x)
                        else:
                            geometry.append(loads(x))

                    del partial_df[settings.GEO_COLUMN_NAME_STAG]

                    partial_df = gpd.GeoDataFrame(partial_df, geometry=geometry)
                    partial_df = partial_df.set_crs(crs=settings.GEO_CRS_CODE, allow_override=True)

                    where_geo_is_none_df = partial_df.loc[partial_df["geometry"].isna()]
                    contain_geo_df = partial_df.loc[~partial_df["geometry"].isna()]
                    logger.debug("%s geo load over" % (table_name))
                    if len(where_geo_is_none_df) > 0:
                        where_geo_is_none_df.to_sql(**to_db_para)

                    if len(contain_geo_df) > 0:
                        contain_geo_df.to_postgis(**to_db_para)
                else:
                    partial_df.to_sql(**to_db_para)

            logger.debug("%s extract end" % table_name)
        except Exception as e:
            error_message = "%s extract failed. Error Info: %s" % (table_name, e)
            logger.error(error_message)
            return error_message


register(Mssql)
