import aiohttp
import asyncio
import logging
import sys
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Dict

from tranalyzer.metrics.base import bql_to_metrics_cls, table
from tranalyzer.lib.util import load_yaml, spinner
from tranalyzer.lib.nad_session import Nad


PRJ_DIR = Path(__file__).parent
CONF_PATH = PRJ_DIR / 'config.yaml'
BQLQUERY_PATH = PRJ_DIR / 'bqlquery.yaml'
TMP_DIR = PRJ_DIR / 'tmp'
log = logging.getLogger()


def init_logging(conf):
    log_level = conf['log_level'].upper()
    log.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    lof_file = str(TMP_DIR / 'tranalyzer.log')
    fh = logging.FileHandler(lof_file, mode='w')
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    print(f"Log file {lof_file}")
    return fh


@contextmanager
def logging_context(conf):
    try:
        fh = init_logging(conf)
        yield
    finally:
        log.removeHandler(fh)
        fh.close()


async def nad_main(conf):
    with logging_context(conf):
        start = conf['start']
        end = conf['end']
        if start - end > timedelta(days=conf['max_time_range']):
            log.error('Time range overflow error')
            exit(f'Time range overflow error. Current range is {(end - start)};'
                 f' {conf["max_time_range"]} days available.')
        with Nad(host=conf['host'], username=conf['username'], password=conf['password']) as nad:
            sources = nad.live_sources()
        if not sources:
            log.error('No live sources found')
            exit('No live sources found')

        print(f'Host {conf["host"]}')
        print(f'Start {start.strftime(" %d/%m/%Y, %H:%M")} ')
        print(f'End   {end.strftime("%d/%m/%Y, %H:%M")} ')
        log.info(f'Host {conf["host"]}; Start {start.strftime("%d/%m/%Y, %H:%M")}; '
                 f'End {end.strftime("%d/%m/%Y, %H:%M")}')
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(conf['username'],
                                                                conf['password']),
                                         connector=aiohttp.TCPConnector(ssl=False)) as session:
            for section in ['main', 'custom']:
                log.debug(f'Section {section} processed')
                metric_cls_lst = bql_to_metrics_cls(BQLQUERY_PATH, section)
                metric_lst = []

                for source, name in sources.items():
                    log.debug(f'Source {source} processed')
                    for metric_cls in metric_cls_lst:
                        metric_lst.append(metric_cls(conf['start'], conf['end'], source,
                                                     conf['host'], conf['location']))

                    if sys.version_info >= (3, 7):
                        spin = asyncio.create_task(spinner(300))
                        await asyncio.gather(*[m.get(session)() for m in metric_lst])
                        spin.cancel()
                    else:
                        async def SpinnerCoroutine(future):
                            await spinner(3000)

                        future = asyncio.Future()
                        asyncio.ensure_future(SpinnerCoroutine(future))
                        await asyncio.gather(*[m.get(session)() for m in metric_lst])
                        future.cancel()

                    print(table(metric_lst, section, f'Source {name}'))


def run(conf):
    default = load_yaml(CONF_PATH)
    default.update(conf)
    conf = default
    bvt_test(conf)
    if conf['mode'] == 'nad':
        if sys.version_info >= (3, 7):
            asyncio.run(nad_main(conf))
        else:
            import signal
            from asyncio import Task
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(nad_main(conf))

                loop.stop()
                loop.run_forever()
                tasks = Task.all_tasks()
                # give canceled tasks (spinners) the last chance to run
                for t in [t for t in tasks if not (t.done() or t.cancelled())]:
                    t.cancel()
                    loop.run_until_complete(t)
            except (KeyboardInterrupt, SystemExit):
                log.info("Got terminate signal. Stopping...")
            finally:
                loop.close()


def bvt_test(conf: Dict) -> None:
    """
        Verify if NAD is accessible over given IP and user credentials.
        Exits in case of failure
    :param conf: config structure
    """
    resp = Nad(conf['host'], username=conf['username'], password=conf['password']).login()
    if resp.status != Nad.OK:
        exit(f"NAD not available at {conf['host']} {conf['username']}:{conf['password']}")

