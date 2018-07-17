import codecs
import re
import urllib.request
from collections import namedtuple

Data = namedtuple('Data', ['hostname', 'value'])

URL = "http://{}/hp/device/info_suppliesStatus.html"


def job(host: str) -> Data:
    url = URL.format(host)
    name = host.replace(".", "_")
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        for line in f2.readlines():
            m = re.match(".*>([0-9]*)%<br", line)
            if m:
                return Data(name, int(m.groups()[0]))


if __name__ == "__main__":
    from pprint import pprint

    pprint(job("10.1.0.10"))
