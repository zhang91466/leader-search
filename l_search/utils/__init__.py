# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:simonzhang
@file:__init__.py
"""
import datetime
import simplejson
import decimal
import uuid
import binascii
from sqlalchemy.orm.query import Query
from psycopg2.extras import Range


class JSONEncoder(simplejson.JSONEncoder):
    """Adapter for `simplejson.dumps`."""

    def default(self, o):
        # Some SQLAlchemy collections are lazy.
        if isinstance(o, Query):
            result = list(o)
        elif isinstance(o, decimal.Decimal):
            result = float(o)
        elif isinstance(o, (datetime.timedelta, uuid.UUID)):
            result = str(o)
        # See "Date Time String Format" in the ECMA-262 specification.
        elif isinstance(o, datetime.datetime):
            result = o.isoformat()
            if o.microsecond:
                result = result[:23] + result[26:]
            if result.endswith("+00:00"):
                result = result[:-6] + "Z"
        elif isinstance(o, datetime.date):
            result = o.isoformat()
        elif isinstance(o, datetime.time):
            if o.utcoffset() is not None:
                raise ValueError("JSON can't represent timezone-aware times.")
            result = o.isoformat()
            if o.microsecond:
                result = result[:12]
        elif isinstance(o, memoryview):
            result = binascii.hexlify(o).decode()
        elif isinstance(o, bytes):
            result = binascii.hexlify(o).decode()
        elif isinstance(o, Range):
            # From: https://github.com/psycopg/psycopg2/pull/779
            if o._bounds is None:
                return ''

            items = [
                o._bounds[0],
                str(o._lower),
                ', ',
                str(o._upper),
                o._bounds[1]
            ]

            result = ''.join(items)
        else:
            result = super(JSONEncoder, self).default(o)
        return result


def json_loads(data, *args, **kwargs):
    """A custom JSON loading function which passes all parameters to the
    simplejson.loads function."""
    return simplejson.loads(data, *args, **kwargs)


def json_dumps(data, *args, **kwargs):
    """A custom JSON dumping function which passes all parameters to the
    simplejson.dumps function."""
    kwargs.setdefault("cls", JSONEncoder)
    kwargs.setdefault("encoding", None)
    # Float value nan or inf in Python should be render to None or null in json.
    # Using ignore_nan = False will make Python render nan as NaN, leading to parse error in front-end
    kwargs.setdefault('ignore_nan', True)
    return simplejson.dumps(data, *args, **kwargs)
