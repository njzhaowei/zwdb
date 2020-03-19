class Record(object):
    """A row, from a query, from a database."""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys=None, values=None, o=None):
        if isinstance(o, dict):
            self._keys = list(o.keys())
            self._values = list(o.values())
        else:
            self._keys = keys
            self._values = values

        # Ensure that lengths match properly.
        if not isinstance(o, dict):
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

class DocumentCollection(object):
    """A set of Records from a query."""
    def __init__(self, docs):
        self._docs = docs
        self._all_docs = []
        self.pending = True

    def all(self):
        """Returns a list of all rows for the DocumentCollection. If they haven't
        been fetched yet, consume the iterator and cache the results."""

        # By calling list it calls the __iter__ method
        docs = list(self)        
        return docs

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
            nextdoc = next(self._docs)
            nextrec = Record(o=nextdoc)
            self._all_docs.append(nextrec)
            return nextrec
        except StopIteration:
            self.pending = False
            raise StopIteration('DocumentCollection contains no more docs.')

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

        rows = self._all_docs[key]
        if is_int:
            return rows[0]
        else:
            return DocumentCollection(iter(rows))

    def __len__(self):
        return len(self._all_docs)

    def __repr__(self):
        return '<DocumentCollection size={} pending={}>'.format(len(self), self.pending)

class ZwdbError(Exception):
    def __init__(self, arg):
        self.args = arg