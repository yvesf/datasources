import codecs
import logging
import typing
from enum import Enum
from html.parser import HTMLParser
from urllib import request


rupture = 'Rupture de stock'

class Station:
    def __init__(self):
        self.station_name = ""
        self.prices = {}
        self.id = None

    def clean(self):
        self.prices = filter(lambda kv: kv[1] != '', self.prices.items())
        self.prices = filter(lambda kv: kv[1] != rupture, self.prices)
        self.prices = dict(map(lambda kv: (kv[0], float(kv[1])), self.prices))

    def __repr__(self):
        return "Prix: {} {}".format(self.station_name, self.prices)


State = Enum('State', 'pricelist fuel_name fuel_price station_name idle')


class Parser(HTMLParser):
    def error(self, message):
        logging.error("Parser error: %s", message)

    def __init__(self):
        super().__init__()
        self._prix = Station()
        self._current_fuel_name = ""
        self._state = State.idle

    def get_prix(self):
        self._prix.clean()
        return self._prix

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self._state == State.idle and tag == "div" and "id" in attrs and attrs['id'] == 'prix':
            self._state = State.pricelist
        elif self._state in [State.pricelist, State.fuel_price] and tag == 'strong':
            self._state = State.fuel_name
            self._current_fuel_name = ''
        elif self._state == State.idle and tag == 'div' and 'id' in attrs and attrs['id'] == 'colg':
            self._state = State.station_name

    def handle_endtag(self, tag):
        if self._state == State.pricelist and tag == 'div':
            self._state = State.idle
        elif self._state == State.fuel_name and tag == 'strong':
            self._state = State.fuel_price
        elif self._state == State.fuel_price and tag == 'div':
            self._state = State.idle
        elif self._state == State.station_name and tag == 'p':
            self._state = State.idle

    def handle_data(self, data: str):
        if self._state == State.fuel_name:
            self._current_fuel_name += data.strip().replace(':', '')
            self._prix.prices[self._current_fuel_name] = ""
        elif self._state == State.fuel_price:
            if data.strip() != "0.000":
                self._prix.prices[self._current_fuel_name] += data.strip()
        elif self._state == State.station_name:
            if len(data.strip()) > 0:
                self._prix.station_name += data.strip() + ". "


URL = "https://www.prix-carburants.gouv.fr/map/recupererInfosPdv/"


def _execute(station_id: str):
    parser = Parser()
    r = request.Request(URL + station_id, data=b"")
    r.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0')
    r.add_header('X-Requested-With', 'XMLHttpRequest')
    r.add_header('X-Prototype-Version', '1.7')
    r.add_header('Connection', 'keep-alive')
    r.add_header('Content-type', 'application/x-www-form-urlencoded; charset=UTF-8')
    with request.urlopen(r) as f:
        # with open("info.html", 'rb') as f:
        f2 = codecs.getreader('utf-8')(f)
        f2.errors = 'ignore'
        for line in f2.readlines():
            parser.feed(line)

        try:
            prix = parser.get_prix()
            prix.id = station_id
            return prix
        except Exception as e:
            raise Exception("Failed for station: {}".format(station_id), e)


def execute(*ids) -> typing.Iterable[Station]:
    for station_id in ids:
        try:
            yield _execute(station_id)
        except Exception as e:
            raise Exception("Failed for station {}".format(station_id), e)


if __name__ == "__main__":
    from pprint import pprint

    pprint(list(execute('1630001', '67760001', '1210003', '1630003', '1210002', '1710001')))
