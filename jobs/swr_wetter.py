import codecs
import json
import re
import urllib.parse
import urllib.request

URL = "http://www.swr.de/-/id=5491998/cf=42/did=13968954/format=json/nid=5491998/17ag7cb/index.json"


def job(cc):
    """
    cc: id of the region. See webpage: http://www.swr.de/wetter
    """
    params = urllib.parse.urlencode({'cc': cc})
    request = urllib.request.Request(URL + "?" + params)
    request.add_header(
        "User-Agent",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.6.01001)")

    def transformDict(name, value_dict):
        for (key, value) in value_dict.items():
            if key in ['timestamp', 'dayForecast']:
                continue
            if value == "k. A.":
                continue
            elif re.match("^-?[1-9]+[0-9]*$", value) or value == "0":
                value = int(value)
            elif re.match("^-?[1-9]+[0-9]*.?[0-9]*$", value):
                value = float(value)
            yield {'name' : "{}.{}.{}".format(basename, name, key), 'value': value}

    with urllib.request.urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        response = json.load(f2)
        basename = "swr_wetter.{stateCode}.{regionCode}.{name}".format(
            **response['availableLocations'][cc])

        for d in transformDict("current", response['current'][cc]):
            yield d

        for (day, value) in response['forecast'].items():
            value = value[cc]
            for d in transformDict("forecast." + day, value):
                yield d


if __name__ == "__main__":
    from pprint import pprint

    pprint(list(job("DE0008834")))
