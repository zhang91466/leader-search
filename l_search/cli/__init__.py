# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
import click
from flask.cli import FlaskGroup, run_command
from flask import current_app
from l_search import __version__, create_app


def create(group):
    app = current_app or create_app()
    group.app = app

    @app.shell_context_processor
    def shell_context():
        from l_search import models, settings

        return {"models": models, "settings": settings}

    return app


@click.group(cls=FlaskGroup, create_app=create)
def manager():
    """Management script for l_search"""


manager.add_command(run_command, "runserver")


@manager.command()
def version():
    """Displays l_search version."""
    print(__version__)


@manager.command()
def check_settings():
    """Show the settings as l_search sees them (useful for debugging)."""
    for name, item in current_app.config.items():
        print("{} = {}".format(name, item))
