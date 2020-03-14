# -*- coding: utf-8 -*-
import os
import abc
from collections import OrderedDict
from contextlib import contextmanager

from .utils import isexception

class Record(object):
    """A row, from a query, from a database."""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys, values):
        self._keys = keys
        self._values = values

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self):
        """Returns the list of column names from the query."""
        return self._keys

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def __repr__(self):
        return '<Record {}>'.format(self.as_dict())

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Record contains multiple '{}' fields.".format(key))
            return self.values()[i]

        raise KeyError("Record contains no '{}' field.".format(key))

    def __getattr__(self, key):
         # Support for attr-based lookup.
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __dir__(self):
        standard = dir(super(Record, self))
        # Merge standard attrs with generated ones (from column names).
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self, ordered=False):
        """Returns the row as a dictionary, as ordered."""
        items = zip(self.keys(), self.values())
        return dict(items)

class RecordCollection(object):
    """A set of Records from a query."""
    def __init__(self, keys, rows):
        self._keys = keys
        self._rows = rows
        self._all_rows = []
        self.pending = True

    def all(self, as_dict=False):
        """Returns a list of all rows for the RecordCollection. If they haven't
        been fetched yet, consume the iterator and cache the results."""

        # By calling list it calls the __iter__ method
        rows = list(self)        
        if as_dict:
            return [r.as_dict() for r in rows]
        return rows

    def as_dict(self):
        return self.all(as_dict=True)

    def __iter__(self):
        """Iterate over all rows, consuming the underlying generator only when necessary."""
        i = 0
        while True:
            # Other code may have iterated between yields,
            # so always check the cache.
            if i < len(self):
                yield self[i]
            else:
                # Throws StopIteration when done.
                # Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def __next__(self):
        try:
            nextrow = next(self._rows)
            nextrec = Record(self._keys, nextrow)
            self._all_rows.append(nextrec)
            return nextrec
        except StopIteration:
            self.pending = False
            raise StopIteration('RecordCollection contains no more rows.')

    def __getitem__(self, key):
        is_int = isinstance(key, int)

        # Convert RecordCollection[1] into slice.
        if is_int:
            key = slice(key, key + 1)

        while len(self) < key.stop or key.stop is None:
            try:
                next(self)
            except StopIteration:
                break

        rows = self._all_rows[key]
        if is_int:
            return rows[0]
        else:
            return RecordCollection(self._keys, iter(rows))

    def __len__(self):
        return len(self._all_rows)

    def __repr__(self):
        return '<RecordCollection size={} pending={}>'.format(len(self), self.pending)

class Connection(metaclass=abc.ABCMeta):
    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()
    
    @abc.abstractmethod
    def __next__(self):
        raise StopIteration('No more rows.')

    @abc.abstractmethod
    def close(self):
        return

    @abc.abstractmethod
    def execute(self, stmt, fetchall=True, commit=False, **params):
        return

    @abc.abstractmethod
    def find(self, dst, fetchall=False, **params):
        return

    @abc.abstractmethod
    def insert(self, dst, recs):
        return
    
    @abc.abstractmethod
    def update(self, dst, recs, keyflds):
        return
    
    @abc.abstractmethod
    def upsert(self, dst, recs, keyflds):
        return
    
    @abc.abstractmethod
    def delete(self, dst, recs, keyflds):
        return
    
class Database(object):
    """A Database"""
    def __init__(self, db_url=None, **kwargs):
        # If no db_url was provided, fallback to $DATABASE_URL.
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError('You must provide a db_url.')

        # Create an driver.
        self._drv = self.create_driver(self.db_url, **kwargs)
        self.open = True
    
    def create_driver(self, db_url, **kwargs):
        dbtype = db_url.split('://')[0].lower()
        from .zwmysql import ZWMysql
        from .zwmongo import ZWMongo
        supported = {
            'mysql': ZWMysql,
            'mongo': ZWMongo,
            # 'sqlite':,
            # 'redis':
        }
        if dbtype not in supported:
            raise ValueError('DB ERR, not support: %s'%dbtype)
        drvcls = supported[dbtype]
        return drvcls(db_url, **kwargs)

    def close(self):
        """Closes the Database."""
        self._drv.dispose()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database open={}>'.format(self.open)

    def get_table_names(self, internal=False):
        """Returns a list of table names for the connected database."""
        r = self._drv.get_table_names()
        return r

    def get_connection(self):
        """Get a connection to this Database. Connections are retrieved from a
        pool.
        """
        if not self.open:
            raise IOError('Database closed.')
        return self._drv.connect()

    def execute(self, stmt, **params):
        """Execute statement
        """
        conn = self.get_connection()
        return conn.execute(stmt, **params)

    def findone(self, stmt, **params):
        """Return one or None, or raise Exception is mulit records found
        """
        recs = self.findmany(stmt, fetchall=True, **params)
        if len(recs) > 1:
            raise ZwdbError('Multi records found in findone!')
        return recs[0]
    def findmany(self, dst, fetchall=False, **params):
        """Return iter, multi records or []
        """
        conn = self.get_connection()
        recs = conn.find(dst, fetchall, **params)
        if fetchall:
            recs.all()
            conn.close()
        return recs

    def insertone(self, dst, o):
        return self.insertmany(dst, [o])
    def insertmany(self, dst, recs):
        if recs is None:
            raise ZwdbError('Try insert None in insertmany!')
        with self.get_connection() as conn:
            result = conn.insert(dst, recs)
        return result
    
    def updateone(self, dst, o, keyflds):
        return self.updatemany(dst, [o], keyflds)
    def updatemany(self, dst, recs, keyflds):
        if recs is None:
            raise ZwdbError('Try update None in updatemany!')
        with self.get_connection() as conn:
            result = conn.update(dst, recs, keyflds)
        return result
    
    def upsertone(self, dst, o, keyflds):
        return self.upsertmany(dst, [o], keyflds)
    def upsertmany(self, dst, recs, keyflds):
        if recs is None or keyflds is None:
            raise ZwdbError('Recs or keyflds is None in upsertmany!')
        with self.get_connection() as conn:
            result = conn.upsert(dst, recs, keyflds)
        return result
    
    def deleteone(self, dst, o, keyflds):
        pass
    def deletemany(self, dst, recs, keyflds):
        if recs is None:
            raise ZwdbError('Try delete None in deletemany!')
        with self.get_connection() as conn:
            result = conn.delete(dst, recs, keyflds)
        return result

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""

        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            conn.close()

class ZwdbError(Exception):
    def __init__(self, arg):
        self.args = arg