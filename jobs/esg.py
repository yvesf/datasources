import codecs
import logging
import urllib.parse
import urllib.request
from enum import Enum
from html.parser import HTMLParser


class Product:
    def __init__(self):
        self.price = ""
        self.name = ""
        self.sku = None

    def __repr__(self):
        return "<{} name={} price={} sku={}>".format(
            self.__class__, self.name, self.price, self.sku)


State = Enum('State', 'parsing product product_name price idle')


class Parser(HTMLParser):
    def error(self, message):
        logging.error("Parser error: %s", message)

    def __init__(self):
        super().__init__()
        self.products = []
        self.current = None
        self.state = State.idle

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.state == State.idle and tag == "tr" and "data-sku" in attrs:
            self.current = Product()
            self.current.sku = attrs["data-sku"]
            self.state = State.parsing
        elif self.state == State.parsing and tag == 'h3' and \
                "class" in attrs and attrs['class'] == 'product-name':
            self.state = State.product_name
        elif self.state == State.parsing and tag == 'span' and \
                "class" in attrs and attrs['class'] == "price":
            self.state = State.price

    def handle_endtag(self, tag):
        if self.state == State.product_name and tag == 'a':
            self.state = State.parsing
        elif self.state == State.price and tag == 'span':
            self.state = State.parsing

        if self.current and self.current.name and \
                self.current.price and self.current.sku:
            self.current.name = self.current.name.strip()
            price = self.current.price
            price = price.replace(".", "").replace(",", ".").split("\xa0")[0]
            self.current.price = float(price)
            self.products += [self.current]
            self.current = None
            self.state = State.idle

    def handle_data(self, data):
        if self.state == State.product_name:
            self.current.name += data
        if self.state == State.price:
            self.current.price += data


URL = "http://www.edelmetall-handel.de/quickbuy/twozero/"


def execute():
    """Always fetches full catalog"""
    request = urllib.request.Request(URL)
    with urllib.request.urlopen(request) as f:
        # with open("index.html", 'rb') as f:
        f2 = codecs.getreader('utf-8')(f)
        f2.errors = 'ignore'
        parser = Parser()
        for line in f2.readlines():
            parser.feed(line)
        return parser.products


if __name__ == "__main__":
    from pprint import pprint

    pprint(execute())
