import pytest 
import time

@pytest.fixture
def patch_time(monkeypatch):
    class mytime:
        counter = 0
        @classmethod
        def time(cls):
            cls.counter += 60
            return cls.counter

        @classmethod
        def sleep(cls, time=0):
            return 0

    monkeypatch.setattr(time, 'time', mytime.time)
    monkeypatch.setattr(time, 'sleep', mytime.sleep)


