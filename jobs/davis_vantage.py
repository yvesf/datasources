import codecs
import json
from urllib.request import urlopen, Request


def load(url: str):
    request = Request(url)
    request.add_header(
        "User-Agent",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.6.01001)")
    with urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        data = json.load(f2)
        data = {i: data[i] for i in data if type(data[i]) in (int, float)}
        return data


if __name__ == "__main__":
    from pprint import pprint

    pprint(list(load('http://wettermichel.de/davis/con_davis.php')))
