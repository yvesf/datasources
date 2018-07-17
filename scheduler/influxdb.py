import logging
import reprlib
import typing
from urllib.request import urlopen

import collections
from pyinflux.client import Line

from . import Job


def _get_measurement_name(job: Job):
    if 'measurement' in job.properties:
        return job.properties['measurement']
    else:
        return job.name


class Dumper:
    def __init__(self):
        self._repr = reprlib.Repr()

    def __call__(self, *args, **kwargs) -> None:
        if len(args) == 2 and isinstance(args[0], Job):
            # assuming second is list of objects that str() to influx protocol lines
            data = self._convert(args[0], args[1])
            self._insert(data)
        else:
            raise Exception("Wrong arguments for InfluxDB inserter.")

    def _insert(self, lines: typing.Iterable):
        data = "\n".join(map(str, lines))
        print("===== Would insert:\n" + data)

    def _convert(self, job: Job, data) -> typing.Iterable[Line]:
        def c(name, value):
            if isinstance(value, Line):
                return value
            elif isinstance(value, int) or isinstance(value, str) or isinstance(value, float):
                return Line(name, {}, {'value': value})
            else:
                raise Exception("Cannot simply insert value of type: {} for job {}".format(type(value), job))

        measurement = _get_measurement_name(job)
        if isinstance(data, collections.Iterable):
            return map(lambda v: c(measurement, v), data)
        else:
            return [c(measurement, data)]


class Inserter(Dumper):
    def __init__(self, url: str) -> None:
        super().__init__()
        self._url: str = url

    def _insert(self, lines: typing.Iterable):
        try:
            data = "\n".join(map(str, lines)).encode('utf-8')
            try:
                with urlopen(self._url, data) as fh:
                    logging.debug("InfluxDB successful answer: %s", self._repr.repr(fh.read().decode('utf-8')))
            except Exception:
                logging.exception("Failed insert of:\n%s", self._repr.repr(lines))
        except Exception:
            logging.exception("Failed formatting of:\n%s", self._repr.repr(lines))

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} url={repr(self._url)}>"
