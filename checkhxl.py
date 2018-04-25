import asyncio
import logging
import sys
import time
from urllib.parse import quote_plus, unquote

import aiohttp
import tqdm
import uvloop as uvloop
from hdx.data.dataset import Dataset
from hdx.facades.simple import facade

import retry

logger = logging.getLogger(__name__)


def main():
    schema_url = quote_plus(
        'https://raw.githubusercontent.com/OCHA-DAP/tools-datacheck-validation/master/validation-schema.json')
#    base_url = 'https://beta.proxy.hxlstandard.org/data/validate.json?url=%s&schema_url=%s'

    datasets = Dataset.search_in_hdx(fq='tags:hxl')
    print('Number of datasets: %d' % len(datasets))
    urls = list()
    for i, dataset in enumerate(datasets):
        for resource in dataset.get_resources():
#            url = base_url % (quote_plus(unquote(resource['url'])), schema_url)
            urls.append((url, resource['id'], dataset['id']))
    start_time = time.time()
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(check_urls(urls, loop))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()

    dataset_counts = {'info': 0, 'warning': 0, 'error': 0, 'total': 0, 'other': 0, 'toobig': 0}
    resource_counts = {'info': 0, 'warning': 0, 'error': 0, 'total': 0, 'other': 0, 'toobig': 0}
    for dataset_id in results:
        resources = results[dataset_id]
        inc_dataset_count = {'info': False, 'warning': False, 'error': False, 'total': False, 'other': False,
                             'toobig': False}
        for resource_id in resources:
            resource = resources[resource_id]
            print(resource[0])
            for key in resource[2]:
                value = resource[2][key]
                if value:
                    resource_counts[key] = resource_counts[key] + 1
                    inc_dataset_count[key] = True
        for key in inc_dataset_count:
            if inc_dataset_count[key]:
                dataset_counts[key] = dataset_counts[key] + 1
    print('Number of Resources with:')
    for key in ('info', 'warning', 'error', 'toobig', 'other'):
        print('%s: %d' % (key, resource_counts[key]))
    print('Number of Datasets with:')
    for key in ('info', 'warning', 'error', 'toobig', 'other'):
        print('%s: %d' % (key, dataset_counts[key]))


async def check_urls(urls, loop):
    tasks = list()

    conn = aiohttp.TCPConnector(limit=100, limit_per_host=10, loop=loop)
    async with aiohttp.ClientSession(connector=conn, read_timeout=300, loop=loop) as session:
        for metadata in urls:
            task = fetch(metadata, session)
            tasks.append(task)
        responses = dict()
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            resource_issues = dict()
            resource_id, dataset_id, url, err, resource_issues['info'], resource_issues['warning'], \
                resource_issues['error'], resource_issues['total'], resource_issues['toobig'],\
                resource_issues['other'] = await f
            dataset_info = responses.get(dataset_id, dict())
            dataset_info[resource_id] = (url, err, resource_issues)
            responses[dataset_id] = dataset_info
        return responses


async def fetch(metadata, session):
    url = metadata[0]
    resource_id = metadata[1]
    dataset_id = metadata[2]

    async def fn(response):
        length = response.headers.get('Content-Length')
        if length and int(length) > 1048576:
            response.close()
            err = 'File too large to validate!'
            return resource_id, dataset_id, url, err, False, False, False, False, True, False
        try:
            json = await response.json()
            hxl.data(url).validate('my-schema.csv')
            stats = json['stats']
            resource_issues = {'info': False, 'warning': False, 'error': False, 'total': False, 'toobig': False,
                               'other': False}
            for key in stats:
                value = stats[key]
                if value != 0:
                    resource_issues[key] = True
            asyncio.sleep(0.250)
            return resource_id, dataset_id, url, None, resource_issues['info'], resource_issues['warning'], \
                   resource_issues['error'], resource_issues['total'], resource_issues['toobig'], \
                   resource_issues['other']
        except Exception as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ''
            err = 'Exception during hashing: code=%s message=%s raised=%s.%s url=%s' % (code, exc,
                                                                                         exc.__class__.__module__,
                                                                                         exc.__class__.__qualname__,
                                                                                         url)
            raise type(exc)(err).with_traceback(sys.exc_info()[2])

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=1,
                                     interval=1,
                                     backoff=4,
                                     http_status_codes_to_retry=[429, 500, 502, 503, 504],
                                     fn=fn)
    except Exception as e:
        return resource_id, dataset_id, url, str(e), False, False, False, False, False, True


if __name__ == '__main__':
    facade(main, user_agent='test', preprefix='HDXINTERNAL', hdx_site='prod')
