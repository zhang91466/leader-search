# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .app import create_app
from flask_migrate import Migrate

__name__ = "Leadmap full text search"
__version__ = "0.0.1"

migrate = Migrate()