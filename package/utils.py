import datetime
import random, time
from typing import Callable

from environs import Env

env = Env()

now = datetime.datetime.combine(datetime.date.today(), datetime.time())


def divide_or_zero(a, b):
    """
    simple division but take into account divide by zero exception
    """
    try:
        return a / b
    except ZeroDivisionError:
        return 0


def retry_with_backoff(fn: Callable, retries=5, backoff_in_seconds=1):
    x = 0
    while True:
        try:
            return fn()
        except:
            if x == retries:
                raise
            else:
                sleep = backoff_in_seconds * 2 ** x + random.uniform(0, 1)
                time.sleep(sleep)
                x += 1
