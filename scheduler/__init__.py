import calendar
import datetime
import inspect
import logging
import re
import reprlib
import sched
import time
import typing


def time_ns() -> int:
    """:return: the current time in nanoseconds"""
    t = time.clock_gettime(time.CLOCK_REALTIME)
    t_ns = int(t * 1000 * 1000 * 1000)
    return t_ns


def datetime_from_ns(ns: int) -> datetime.datetime:
    """:return: nanoseconds converted to python datetime class"""
    return datetime.datetime.fromtimestamp(ns / 1000 / 1000 / 1000)


def timedelta_ns(**kwargs) -> int:
    """:class:`datetime.timedelta` converted to nanoseconds"""
    return int(datetime.timedelta(**kwargs).total_seconds() * 1000 * 1000 * 1000)


def sleep_ns(t) -> None:
    time.sleep(t / 1000 / 1000 / 1000)


class Job(object):
    """Base-Class for jobs that are scheduled in :class:`Scheduler`"""
    def __init__(self, name: str, **kwargs) -> None:
        self.name: str = name
        self.properties = kwargs
        self._execute_funcs: typing.List[typing.Callable[..., typing.Any]] = []

    def next(self, start_ns: int, t_ns: int, t_max_ns: int) -> typing.Optional[int]:
        """
        :param start_ns: start of scheduler
        :return: next run of this job after time 't' or None if no run can be calculated in t_max_ns time
        """
        raise NotImplementedError()

    def add_action(self, func):
        if func is not None:
            sig = inspect.signature(func)
            if not len(sig.parameters) in [0, 1, 2]:
                raise Exception("Wrong number of parameters to action")
            self._execute_funcs.append(func)
        return self

    def execute(self, scheduler) -> typing.Any:
        for e in self._execute_funcs:
            sig = inspect.signature(e)
            if len(sig.parameters) == 0:
                return e()
            elif len(sig.parameters) == 1:
                return e(self)
            elif len(sig.parameters) == 2:
                return e(scheduler, self)

    def __repr__(self) -> str:
        return "<{cls.__name__} name={name} {conf}>".format(cls=self.__class__, name=repr(self.name),
                                                            conf=self.__repr_config__())

    def __repr_config__(self) -> str:
        return " "


def every(seconds: int = 0, minutes: int = 0, hours: int = 0, name='Unnamed-Job', action=None) -> Job:
    """
    Run a job in intervals.

    :param seconds: add seconds to the interval. Defalut: 0
    :param minutes: add minutes to the interval. Default: 0
    :param hours: add hours to the interval. Default: 0
    :param name: Name of the Job
    :param action: a function to be executed, see :func:`Job.execute`
    :return: The job to be added to :class:`Scheduler`
    """
    n = seconds * 1000 * 1000 * 1000 + \
        minutes * 1000 * 1000 * 1000 * 60 + \
        hours * 1000 * 1000 * 1000 * 60 * 60
    j = PeriodicJob(name, n)
    j.add_action(action)
    return j


class PeriodicJob(Job):
    def __init__(self, name: str, interval: float, **kwargs) -> None:
        super().__init__(name, **kwargs)
        self.interval: float = interval

    def next(self, start_ns: int, t_ns: int, t_max_ns: int) -> typing.Optional[int]:
        t_since_start = t_ns - start_ns
        tn = t_since_start % self.interval
        t_next_ns = int(t_ns + (self.interval - tn))
        if t_next_ns < t_max_ns:
            return t_next_ns
        else:
            return None

    def __repr_config__(self) -> str:
        return " interval=" + str(self.interval)


_pattern_value = re.compile(r'^[0-9]+$')
_pattern_range = re.compile(r'([0-9]+)-([0-9]+)$')
_pattern_asterisk = re.compile(r'\*/([0-9]+)$')


def make_test_expr(expr: str) -> typing.Callable[[int], bool]:
    def parse(s: str):
        if s == '*':
            return lambda val: True
        else:
            m = _pattern_value.match(s)
            if m is not None:
                return lambda val: str(val) == s
            m = _pattern_range.match(s)
            if m is not None:
                start, end = map(int, m.groups())
                return lambda val: start <= val <= end
            m = _pattern_asterisk.match(s)
            if m is not None:
                mod = int(m.groups()[0])
                return lambda val: val % mod == 0
            raise Exception("More complex cron expression is not supported")

    exprs = expr.split(',')
    if len(expr) == 1:
        return parse(expr)
    else:
        funcs = map(parse, exprs)
        return lambda val: any(map(lambda func: func(val), funcs))


def _generator(timedelta_func, datetime_func, expr_func):
    def f(start_ns: int, stop_ns: int):
        dt = datetime_from_ns(start_ns)
        while int(dt.timestamp() * 1000 * 1000 * 1000) < stop_ns:
            _dt = datetime_func(dt)
            delta = timedelta_func(_dt)
            # print("{}           -       {}".format(_dt, delta))
            t_ns = int(dt.timestamp() * 1000 * 1000 * 1000)
            if t_ns >= start_ns and expr_func(_dt):
                yield (int(dt.timestamp() * 1000 * 1000 * 1000),
                       int(((_dt + delta - datetime.timedelta(minutes=1)).timestamp() * 1000 * 1000 * 1000)))
            dt = _dt + delta

    return f


def combine(start_ns: int, stop_ns: int, funcs):
    f = funcs[0]
    for (_start_ns, stop_ns) in f(start_ns, stop_ns):
        # print("{}: {} {}  -> {} {}".format(repr(f), _start_ns, datetime_from_ns(start_ns), stop_ns, datetime_from_ns(stop_ns)))
        if (len(funcs)) > 1:
            r = combine(_start_ns, stop_ns, funcs[1:])
            if r is not None:
                return r
        elif _start_ns >= start_ns:
            # print("Found {} ".format(start_ns))
            return _start_ns


class CronJob(Job):
    def __init__(self, name: str, minute: str, hour: str, dow: str, dom: str, month: str, **kwargs) -> None:
        super().__init__(name, **kwargs)
        self.minute = minute
        self.minute_f = _generator(lambda dt: datetime.timedelta(minutes=1),
                                   lambda dt: datetime.datetime(year=dt.year, month=dt.month, day=dt.day,
                                                                hour=dt.hour, minute=dt.minute),
                                   lambda dt: make_test_expr(minute)(dt.minute))
        self.hour = hour
        self.hour_f = _generator(lambda dt: datetime.timedelta(hours=1),
                                 lambda dt: datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour),
                                 lambda dt: make_test_expr(hour)(dt.hour))
        self.dow = dow
        self.dow_f = _generator(lambda dt: datetime.timedelta(days=1),
                                lambda dt: datetime.datetime(year=dt.year, month=dt.month, day=dt.day),
                                lambda dt: make_test_expr(dow)(dt.isoweekday()))
        self.dom = dom
        self.dom_f = _generator(lambda dt: datetime.timedelta(days=1),
                                lambda dt: datetime.datetime(year=dt.year, month=dt.month, day=dt.day),
                                lambda dt: make_test_expr(dom)(dt.day))
        self.month = month
        self.month_f = _generator(lambda dt: datetime.timedelta(days=calendar.monthrange(dt.year, dt.month)[1]),
                                  lambda dt: datetime.datetime(year=dt.year, month=dt.month, day=1),
                                  lambda dt: make_test_expr(month)(dt.month))

    def next(self, start_ns: int, t_ns: int, t_max_ns: int):
        stop_ns = t_ns + t_max_ns
        t_ns = t_ns - t_ns % (1000 * 1000 * 1000) # round current timestamp down to minutes
        n = combine(t_ns + timedelta_ns(minutes=1), stop_ns,
                    [self.month_f, self.dom_f, self.dow_f, self.hour_f, self.minute_f])
        return n

    def __repr_config__(self):
        return " minute={_.minute} hour={_.hour} dow={_.dow} dom={_.dom} month={_.month}".format(_=self)


def at(minute: str = '*', hour: str = '*', day_of_week: str = '*', day_of_month: str = '*', month: str = '*',
       name='Unnamed-Job', action=None, **kwargs):
    job = CronJob(name, minute, hour, day_of_week, day_of_month, month, **kwargs)
    job.add_action(action)
    return job


def cron(cron_expr: str, name='Unnamed-Job', action=None, **kwargs):
    groups = list(filter(lambda x: x != '', cron_expr.split(' ')))
    if len(groups) != 5:
        raise Exception("Invalid cron expression, failed to find minute-hour-dow-dom-month pattern")

    minute, hour, day_of_month, month, day_of_week = groups
    return at(minute, hour, day_of_week, day_of_month, month, name, action, **kwargs)


# noinspection PyProtectedMember
class Scheduler(object):
    def __init__(self):
        self._scheduler = sched.scheduler(timefunc=time_ns, delayfunc=sleep_ns)
        self._jobs : typing.Dict[Job, typing.Optional[int, None]] = {}
        self._processors : typing.List[typing.Callable[[Job, typing.Any], None]] = []
        self._time_start_ns :int = time_ns()
        self._lookahead_ns : int = 1000 * 1000 * 1000 * 60 * 120
        self._repr = reprlib.Repr()

    def remove_job_by_name(self, name : str):
        with self._scheduler._lock:
            remove = []
            for job in filter(lambda j: j.name == name, self._jobs.keys()):
                remove.append(job)
                self._scheduler.cancel(self._jobs[job])

            for job in remove:
                del self._jobs[job]

    def get_job_by_name(self, name : str) -> typing.Optional[Job]:
        with self._scheduler._lock:
            for job in filter(lambda j: j.name == name, self._jobs.keys()):
                return job
        return None

    def add_job(self, job: Job):
        with self._scheduler._lock:
            if job.name in map(lambda j: j.name, self._jobs.keys()):
                raise Exception("Job with name '{}' exists".format(job.name))
            self._jobs[job] = None
            self._schedule_job_run(job)
            return self

    def add_processor(self, processor : typing.Callable[[Job, typing.Any], None]) -> None:
        with self._scheduler._lock:
            logging.info("Add processor %s", processor)
            self._processors.append(processor)

    def remove_processor(self, processor : typing.Callable[[Job, typing.Any], None]) -> None:
        with self._scheduler._lock:
            self._processors.remove(processor)

    def _process_func(self, job: Job):
        def execute():
            try:
                logging.info("Execute job %s", job)
                result = job.execute(self)
                for p in self._processors:
                    value_repr = self._repr.repr(result)
                    logging.info("Execute result processor %s for job %s result: %s", p, job, value_repr)
                    try:
                        p(job, result)
                    except:
                        logging.exception("Execute result processor %s for job %s failed", p, job)
                logging.info("Execution finished for job %s", job)
            except:
                logging.exception("Exception while job %s", job)
            finally:
                # re-schedule for next execution
                self._schedule_job_run(job)

        return execute

    def _schedule_job_run(self, job):
        now_ns = time_ns()
        stop_ns = now_ns + self._lookahead_ns
        next_ns = job.next(self._time_start_ns, now_ns, stop_ns)
        if next_ns is not None:
            logging.info("Schedule {} in {}ns / at {}".format(job, next_ns - now_ns, datetime_from_ns(next_ns)))
            id = self._scheduler.enterabs(next_ns, 0, self._process_func(job))
        else:
            logging.info("No next schedule for job {}. Retry in 10min".format(job))
            id = self._scheduler.enterabs(now_ns + (1000 * 1000 * 1000 * 10 * 60), 0, lambda: self._schedule_job_run(job))
        self._jobs[job] = id

    def start(self, blocking: bool = True):
        logging.info("Start scheduler (blocking=%s)", blocking)
        self._scheduler.run(blocking)
