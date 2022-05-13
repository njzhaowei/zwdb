import abc

class ZWDbase(abc.ABC):
    @abc.abstractproperty
    def version(self):
        pass

    @abc.abstractproperty
    def info(self):
        pass

    @abc.abstractmethod
    def insert(self):
        pass

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def upsert(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    @abc.abstractmethod
    def find(self):
        pass

    @abc.abstractmethod
    def findone(self):
        pass

    @abc.abstractmethod
    def exists(self):
        pass

    @abc.abstractmethod
    def count(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()
