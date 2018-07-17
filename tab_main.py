#!/usr/bin/env python3
import logging

from pyinflux.client import Line

import jobs.clever_tanken
import jobs.davis_vantage
import jobs.esg
import jobs.hplq1300n
import jobs.prix_carburant
import jobs.swr_wetter
import jobs.tankerkoenig
import jobs.telexoo
import jobs.transferwise
import scheduler.influxdb

logging.basicConfig(level=logging.INFO)

s = scheduler.Scheduler()

transform_swr = lambda v: Line(v['name'], {}, {'value': v['value']})
s.add_job(scheduler.at(minute='0,15,30,45', name='SWR Wetter',
                       action=lambda: map(transform_swr, jobs.swr_wetter.job('DE0008834'))))

transform_laserjet = lambda v: Line('hplq1300n.toner.{}'.format(v.hostname), {}, {'value': v.value})
s.add_job(scheduler.at(minute='*/5', name="Laserjet Status",
                       action=lambda: transform_laserjet(jobs.hplq1300n.job('10.1.0.10'))))

transform_telexoo = lambda qoute: Line('telexoo.{}{}_X'.format(qoute.curr_from, qoute.curr_to), {},
                                       {'value': qoute.rate})
s.add_job(scheduler.every(minutes=10, name="Telexoo.com-CHFGBP",
                          action=lambda: transform_telexoo(jobs.telexoo.execute("CHF", "GBP"))))
s.add_job(scheduler.every(minutes=10, name="Telexoo.com-CHFEUR",
                          action=lambda: transform_telexoo(jobs.telexoo.execute("CHF", "EUR"))))
s.add_job(scheduler.every(minutes=10, name="Telexoo.com-EURCHF",
                          action=lambda: transform_telexoo(jobs.telexoo.execute("EUR", "CHF"))))
s.add_job(scheduler.every(minutes=10, name="Telexoo.com-CHFPLN",
                          action=lambda: transform_telexoo(jobs.telexoo.execute("CHF", "PLN"))))

transform_transferwise = lambda d: Line('transferwise.{}{}_X'.format(d.curr_from, d.curr_to), {}, {
    'value': d.rate})
s.add_job(scheduler.every(minutes=10, name='Transferwise-CHFEUR',
                          action=lambda: transform_transferwise(jobs.transferwise.job('CHF', 'EUR'))))
s.add_job(scheduler.every(minutes=10, name='Transferwise-EURCHF',
                          action=lambda: transform_transferwise(jobs.transferwise.job('EUR', 'CHF'))))

transform_esg = lambda products: [Line('esg', {'sku': product.sku, 'product_name': product.name},
                                       {'price': product.price}) for product in products]
s.add_job(scheduler.at(minute="0", hour="8,10,12,14,16,18,20", name="ESG",
                       action=lambda: transform_esg(jobs.esg.execute())))

s.add_job(scheduler.every(hours=2, name="Wettermichel.de", action=
lambda: [Line('wettermichel.{}'.format(name), {}, {'value': value})
         for name, value in
         jobs.davis_vantage.load('http://wettermichel.de/davis/con_davis.php').items()]))


def execute_prix_carburant():
    for station in jobs.prix_carburant.execute('1630001', '1210003', '1630003', '1210002', '1710001',
                                               '67760001', '67240002', '67452001',
                                               '68740001',  # Fessenheim
                                               '67500009',  # Hagenau
                                               '67116002'):  # Reichstett
        for fuelname, price in station.prices.items():
            tags = {'name': station.station_name, 'id': 'prix_carburant:{}'.format(station.id)}
            fields = {'value': price}
            if fuelname == "SP95":
                yield Line('tankstelle.SP95-E5', tags, fields)
            elif fuelname == "SP95-E10":
                yield Line('tankstelle.SP95-E10', tags, fields)
            elif fuelname == "Gazole":
                yield Line('tankstelle.Diesel', tags, fields)
            elif fuelname == "E85":
                yield Line('tankstelle.E85', tags, fields)


s.add_job(scheduler.at(minute='10', hour='5-22', name="prix_carburant",
                       action=execute_prix_carburant))


def transform_clever(tankstelle: jobs.clever_tanken.Tankstelle):
    for fuelname, price in tankstelle.preise.items():
        tags = {'name': tankstelle.name, 'id': 'clever_tanken:{}'.format(tankstelle.id)}
        fields = {'value': price}
        if fuelname == "Super E5":
            yield Line('tankstelle.SP95-E5', tags, fields)
        elif fuelname == "Super E10":
            yield Line('tankstelle.SP95-E10', tags, fields)
        elif fuelname == "Diesel":
            yield Line('tankstelle.Diesel', tags, fields)


s.add_job(scheduler.at(minute='*/15', hour='5-24', name='Clever-Tanken', action=
lambda: [line for station in map(transform_clever, map(jobs.clever_tanken.execute, [
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
    '3819',  # JET Freiburg
])) for line in station]))


def transform_tankerkoenig(job):
    api_key = job.properties['api_key']
    for data in jobs.tankerkoenig.execute(api_key, 48.651822, 7.927891, 15.0):
        yield Line("tankerkoenig.{}".format(data.type), {'name': data.name, 'id': data.id}, {'value': data.price})


s.add_job(scheduler.at(minute='*/10', hour='5-24', name='Tankerkönig',
                       action=transform_tankerkoenig))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--influx-url', nargs=1, default=None)
    parser.add_argument('--tankerkoenig', nargs=1, default='00000000-0000-0000-0000-000000000002')
    args = parser.parse_args()

    tanker: scheduler.Job = s.get_job_by_name('Tankerkönig')
    tanker.properties['api_key'] = args.tankerkoenig[0]

    if args.influx_url is not None:
        s.add_processor(scheduler.influxdb.Inserter(args.influx_url[0]))
    else:
        s.add_processor(scheduler.influxdb.Dumper())
    s.start(True)
