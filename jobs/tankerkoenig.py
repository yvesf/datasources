import codecs
import json
import logging
import typing
from collections import namedtuple
from urllib import request

Data = namedtuple('Data', ['name', 'id', 'type', 'price'])


URL = "https://creativecommons.tankerkoenig.de/json/list.php?lat={lat}&lng={lng}&rad={rad}&sort=dist&type=all&apikey={api_key}"


def execute(api_key : str, lat: float, lng : float, rad: float) -> typing.Iterable[Data]:
    url = URL.format(api_key=api_key, rad=rad, lat=lat, lng=lng)
    r = request.Request(url)
    try:
        with request.urlopen(r) as f:
            f2 = codecs.getreader('utf-8')(f)
            data = json.load(f2)
            if not data['status'] == 'ok':
                raise Exception("Error %s", data['message'])
        for station in data['stations']:
            name = "{} - {} - {}".format(station['place'], station['brand'], station['name'])
            if not station['isOpen'] == True:
                continue

            if "diesel" in station:
                yield Data(name, station['id'], 'Diesel', station['diesel'])
            if "e5" in station:
                yield Data(name, station['id'], 'SP95-E5', station['e5'])
            if "e10" in station:
                yield Data(name, station['id'], 'SP95-E10', station['e10'])
    except Exception as e:
        logging.error("Failed for: %f %f %f", lat, lng, rad)
        raise e
