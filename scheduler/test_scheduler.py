import unittest

from scheduler import *


class TestPeriodicJob(unittest.TestCase):
    def test1(self):
        job = every(seconds=10)
        next_run = job.next(0, 0, 20 * 1000 * 1000 * 1000)
        self.assertEqual(next_run, 10 * 1000 * 1000 * 1000)

        next_run = job.next(0, next_run, next_run + (20 * 1000 * 1000 * 1000))
        self.assertEqual(next_run, 20 * 1000 * 1000 * 1000)

        self.assertIsNone(job.next(0, next_run, 10))
        self.assertIsNone(job.next(0, next_run, next_run + (10 * 1000 * 1000 * 1000)))
        next_run = job.next(0, next_run, next_run + (10 * 1000 * 1000 * 1000) + 1)
        self.assertEqual(next_run, 30 * 1000 * 1000 * 1000)


class TestProcessor(unittest.TestCase):
    def test1(self):
        class P:
            def __init__(self) -> None:
                self.values = None

            def __call__(self, *args, **kwargs) -> None:
                self.values = args

        s = Scheduler()
        p = P()
        s.add_processor(p)

        job = every(seconds=10, action=lambda: 1234)
        s._process_func(job)()

        self.assertEqual(p.values, (job, 1234))


class TestCronJob(unittest.TestCase):
    def testas(self):
        now = 1531173610000000000
        next_run = at(minute='10').next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=10))

    def testAt2(self):
        now = 1531173610000000000
        next_run = at(minute="0", hour="8,10,12,14,16,18,20", name="Test").next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=8, minute=0))

        next_run += datetime.timedelta(minutes=1).total_seconds() * 1000 * 1000 * 1000
        next_run = at(minute="0", hour="8,10,12,14,16,18,20", name="Test").next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=10, minute=0))

    def test3(self):
        start = datetime.datetime(year=2018, month=7, day=10, hour=10, minute=0).timestamp() * 1000 * 1000 * 1000
        next_run = start
        runs = []
        while next_run < start + timedelta_ns(days=60):
            runs.append(next_run)
            next_run = at(minute="0", hour="8,10,14", name="Test").next(0, next_run, next_run + timedelta_ns(days=2))

        self.assertEqual(len(runs), 60 * 3)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=9, day=8, hour=10, minute=0, second=0))
        self.assertTrue(
            all(map(lambda t: t.hour in [8, 10, 14] and t.minute == 0 and t.second == 0, map(datetime_from_ns, runs))))


    def testas3(self):
        now = 1531173610000000000
        next_run = cron('10,20,30 * * * *').next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=10))

        now = next_run
        next_run = cron('10,20,30 * * * *').next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=20))

        now = next_run
        next_run = cron('10,20,30 * * * *').next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=30))

        now = next_run
        next_run = cron('10,20,30 * * * *').next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=1, minute=10))


    def test4(self):
        expr = '10-15 * * * *'
        now = 1531173610000000000
        next_run = cron(expr).next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=10))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=11))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=12))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=13))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=14))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=15))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=1, minute=10))


    def test5(self):
        expr = '10 */20 * * *'
        now = int(datetime.datetime(year=2018, month=7, day=10, hour=0, minute=0,
                                    second=0).timestamp() * 1000 * 1000 * 1000)
        next_run = cron(expr).next(0, now, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=0, minute=10))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=10, hour=20, minute=10))
        next_run = cron(expr).next(0, next_run, 999000000000000000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=11, hour=0, minute=10))

    def test6(self):
        job = at(minute='*/15', hour='5-24', name='Clever-Tanken', action=None)
        next_run = int(datetime.datetime(year=2018, month=7, day=17, hour=11, minute=51,
                                         second=10).timestamp() * 1000 * 1000 * 1000)

        next_run = job.next(0, next_run, next_run + 60 * 60 * 1000 * 1000 * 1000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=17, hour=12, minute=0))

        next_run = job.next(0, next_run, next_run + 60 * 60 * 1000 * 1000 * 1000)
        self.assertEqual(datetime_from_ns(next_run),
                         datetime.datetime(year=2018, month=7, day=17, hour=12, minute=15))


    def test6(self):
        job = at(minute="0", hour="8,10,12,14,16,18,20", name="ESG", action=None)
        next_run = time_ns() #now
        hours = [8,10,12,14,16,18,20]

        while hours != []:
            next_run = job.next(0, next_run, next_run + 60 * 60 * 1000 * 1000 * 1000)
            next_run_dt = datetime_from_ns(next_run)
            self.assertTrue(next_run_dt.hour in hours)
            self.assertTrue(next_run_dt.minute == 0)
            hours.remove(next_run_dt.hour)

class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler()

    def test(self):
        self.scheduler.add_job(at(minute='0', name='Test'))
        try:
            self.scheduler.add_job(at(minute='0', name='Test'))
            self.assertFalse(True, 'must not happen')
        except:
            pass
