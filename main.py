# -*- coding: UTF-8 -*-
"""
@time:2021/11/23
@author:zhangwei
@file:main.py
"""

from DWMM import set_connect
from DWMM.source_meta_operate.handle.meta_handle import MetaDetector
from DWMM.operate.metadata_info import MetadataOperate



set_connect(db="metadata_l_search")
meta = MetadataOperate(subject_domain="datacenter", object_type="mysql")
detector = MetaDetector(subject_domain="datacenter", object_type="mysql")
detector.detector_schema(is_all=True)

meta.get_table_info()
# 提供查询sql的能力 --> dwmm 提供
