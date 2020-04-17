from inspect import isclass
import re
from urllib.parse import urlparse, parse_qs

def extend_attrs(settings, setting_items):
    for key, val in list(setting_items.items()):
        if hasattr(settings, key):
            setattr(settings, key, val)
    return settings

def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False

def boolstr2bool(s):
    v = s.lower()
    if v in ('true', 'false'):
        v = True if v == 'true' else False
    else:
        v = s
    return v

def db_url_parser(db_url):
    r = urlparse(db_url)
    props = parse_qs(r.query)
    props = {k: boolstr2bool(v[0]) for k,v in props.items()}
    db = r.path.replace('/','')

    return {
        'dbtype': r.scheme,
        'host': r.hostname,
        'port': r.port,
        'usr': r.username,
        'pwd': r.password,
        'db': db.strip(),
        'props': props
    }