import codecs
import logging
from enum import Enum
from html.parser import HTMLParser
from urllib import request

State = Enum('State', 'fuel_name fuel_price station_name idle')


class Tankstelle:
    def __init__(self):
        self.name = ""
        self.preise = {}
        self.id = None

    def __repr__(self):
        return "{}: {} {}".format(type(self).__name__, self.name, self.preise)


class Parser(HTMLParser):
    def error(self, message):
        logging.error("Parser error: %s", message)

    def __init__(self):
        super().__init__()
        self.tankstelle = Tankstelle()
        self._current_fuel_name = None
        self._state = State.idle

    def get_prix(self):
        for key, value in self.tankstelle.preise.items():
            self.tankstelle.preise[key] = float(value)
        return self.tankstelle

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self._state == State.idle:
            if tag == "div" and attrs.get('class') == 'fuel-price-type':
                self._state = State.fuel_name
                self._current_fuel_name = ""
            if tag == "span" and (attrs.get('id') == "main-content-fuel-station-header-name"
                                  or attrs.get('itemprop') == "http://schema.org/addressCountry"):
                self._state = State.station_name
            elif self._current_fuel_name is not None and tag == "span" and attrs.get('ng-bind') == "display_preis":
                self._state = State.fuel_price

    def handle_endtag(self, tag):
        if self._state == State.fuel_name and tag in ('span', 'div'):
            self._state = State.idle
        elif self._state == State.station_name and tag in ('span'):
            self._state = State.idle
        elif self._state == State.fuel_price and tag == 'span':
            self._state = State.idle
            preis = self.tankstelle.preise[self._current_fuel_name].strip()
            if preis == "":
                del self.tankstelle.preise[self._current_fuel_name]
            else:
                self.tankstelle.preise[self._current_fuel_name] = float(preis)
            self._current_fuel_name = None

    def handle_data(self, data: str):
        if self._state == State.fuel_name:
            self._current_fuel_name += data.strip().replace(':', '')
            self.tankstelle.preise[self._current_fuel_name] = ""
        elif self._state == State.fuel_price:
            self.tankstelle.preise[self._current_fuel_name] += data
        elif self._state == State.station_name:
            if len(data.strip()) > 0:
                if len(self.tankstelle.name) > 0:
                    self.tankstelle.name += " "
                self.tankstelle.name += data.strip()


URL = "http://www.clever-tanken.de/tankstelle_details/"


def execute(station_id: str):
    parser = Parser()
    r = request.Request(URL + station_id)
    r.add_header('Host', 'www.clever-tanken.de')
    r.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0')
    try:
        with request.urlopen(r) as f:
            f2 = codecs.getreader('utf-8')(f)
            f2.errors = 'ignore'
            for line in f2.readlines():
                parser.feed(line)

            tankstelle = parser.tankstelle
            tankstelle.id = station_id
            return tankstelle
    except Exception as e:
        logging.error("Failed for station: %s", station_id)
        raise e



if __name__ == "__main__":
    from pprint import pprint

    pprint(list(map(execute, [
        '20219', '11985', '17004',
        '19715',  # Kaiserst. Mineralölvertrieb Schwärzle
        '54296',  # ESSO Endingen
        '10355',  # ARAL Tiengen
        '20144',  # bft Rankackerweg
        '27534',  # EXTROL Freiburg
        '55690',  # Rheinmünster
        '15220',  # Esso Achern
        '5853',  # JET Rastatt
        '24048',  # Bodersweier
        '27534',
        '3819'])  # JET Freiburg
    ))
