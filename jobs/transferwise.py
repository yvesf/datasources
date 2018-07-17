import codecs
import json
import random
import re
import urllib.parse
import urllib.request
from collections import namedtuple

APP_URL = "https://transferwise.com/fr/"
URL = "https://transferwise.com/api/v1/payment/calculate"
UA = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0"

Data = namedtuple('Data', ['curr_from', 'curr_to', 'rate'])

def get_token():
    request = urllib.request.Request(APP_URL)
    request.add_header("User-Agent", UA)

    with urllib.request.urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        for line in f2.readlines():
            m = re.match(".*config.appToken.*'(.+)'.*", line)
            if m:
                g = m.groups()
                return g[0]


def job(currFrom, currTo):
    token = get_token()
    MULT = random.randint(100, 100000)
    data = urllib.parse.urlencode({
        'amount': str(MULT),
        'amountCurrency': 'source',
        'hasDiscount': 'false',
        'isFixedRate': 'false',
        'isGuaranteedFixedTarget': 'false',
        'sourceCurrency': currFrom,
        'targetCurrency': currTo,
    })
    # print (URL + "?" + data)
    request = urllib.request.Request(URL + "?" + data)
    request.add_header("X-Authorization-key", token)
    request.add_header("X-Authorization-token", "")
    request.add_header("User-Agent", UA)
    with urllib.request.urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        response = json.load(f2)
        return Data(currFrom, currTo, float(response['transferwiseRate']))


if __name__ == "__main__":
    from pprint import pprint

    pprint(job("CHF", "EUR"))
    pprint(job("CHF", "GBP"))
