import codecs
import json
import random
import re
import urllib.parse
import urllib.request
from collections import namedtuple
from decimal import Decimal

from currencies.config import *

URL = "https://telexoo.tegona.com/convert/"

Quote = namedtuple('Quote', ['curr_from', 'curr_to', 'rate'])


def execute(curr_from, curr_to):
    MULT = random.randint(1000, 9999)
    CURRENCY = {
        MONEY_CURRENCY_EUR: "EUR",
        MONEY_CURRENCY_CHF: "CHF",
        MONEY_CURRENCY_USD: "USD",
        MONEY_CURRENCY_GBP: "GBP",
        MONEY_CURRENCY_PLN: "PLN"
    }
    curr_from = CURRENCY[curr_from]
    curr_to = CURRENCY[curr_to]
    params = urllib.parse.urlencode({
        's1': curr_from,
        's2': curr_to,
        'amount': str(MULT),
        'action': 'sell',
        'language': 'en',
        'verbose': '0',
    })
    request = urllib.request.Request(URL + "?" + params)
    request.add_header(
        "User-Agent",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.6.01001)")
    with urllib.request.urlopen(request) as f:
        f2 = codecs.getreader('utf-8')(f)
        response = json.load(f2)
        result_raw = response[0]['result'].replace(",", "")
        match = re.match("^{} ([0-9\.]*)$".format(curr_to), result_raw)
        if not match:
            raise Exception("Invalid response in 'result' field")
        result = Decimal(match.groups()[0]) / MULT
        return Quote(curr_from, curr_to, float(result))


if __name__ == "__main__":
    from pprint import pprint

    pprint(execute("EUR", "CHF"))
    pprint(execute("CHF", "EUR"))
