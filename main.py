import logging
import queue

from datetime import datetime
from logging.handlers import QueueHandler, QueueListener
from time import sleep


class StreamHandlerSlow(logging.StreamHandler):
    def emit(self, record):
        sleep(1)
        return super().emit(record)


class QueueLogger:
    def __init__(self, name, level):
        self.que = queue.Queue(-1)
        queue_handler = QueueHandler(self.que)
        handler = StreamHandlerSlow()
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(threadName)s] %(name)s: %(message)s')
        handler.setFormatter(formatter)
        self.listener = QueueListener(self.que, handler)

        self.logger = logging.getLogger(name)
        self.logger.addHandler(queue_handler)
        self.logger.setLevel(level)

    def __enter__(self):
        self.listener.start()
        return self.logger

    def __exit__(self, exc_type, exc_value, traceback):
        self.listener.stop()


def factorial(n):
    if n < 1:
        raise ValueError("Natural number expected")
    res = 1

    with QueueLogger('test_task', 'DEBUG') as log:
        print(f'Start calc {datetime.now()}')
        for i in range(2, n+1):
            res *= i
            log.debug(f'i = {i}')
            log.debug(f'res = {res}')
        print(f'End calc {datetime.now()}')
    return res


if __name__ == '__main__':
    assert factorial(1) == 1
    assert factorial(5) == 120


