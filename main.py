from bs4 import BeautifulSoup
from multiprocessing import Pool
import asyncio
import aiohttp
from aiohttp.client import ClientTimeout
from aiohttp.client_exceptions import ClientConnectionError
# from asyncio.exceptions import TimeoutError
from datetime import datetime
import re
import time
import logging


logging.basicConfig(
    filename='errors.log',
    filemode='a',
    level=logging.ERROR,
)

# url data
URLS = [line.rstrip('\n') for line in open('wildberries_urls.txt')]
all_processes = 8
process = 0
urls_list = [[] for _ in range(all_processes)]
for url in URLS:
    if process == (all_processes - 1):
        process = 0
    else:
        process += 1
    urls_list[process - 1].append(url)

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
}

current_items = []

def my_timer(func):
    def wrappered(*args, **kwargs):
        start_time = datetime.now()
        func(*args, **kwargs)
        print(datetime.now() - start_time)

    return wrappered


async def parse_page(item):
    item_title = item.select_one('.dtlist-inner-brand-name').text
    item_title = re.sub("^\s+|\n|\r|\s+$|₽|,| ", '', item_title)
    current_items.append(item_title)


async def get_page_data(session, url):
    try:
        async with session.get(url) as response:
            tasks = []
            text_response = await response.text()
            soup = BeautifulSoup(text_response, 'lxml')
            items = soup.select('div.dtList.i-dtList.j-card-item')
            for item in items:
                task = asyncio.create_task(parse_page(item))
                tasks.append(task)
            await asyncio.gather(*tasks)
    except asyncio.TimeoutError as e:
        logging.error(f'{datetime.now()}\n{e}')
        print(f'timeout error')


async def get_page(urls, loop):
    async with aiohttp.ClientSession(headers=headers, loop=loop, timeout=ClientTimeout(total=15)) as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(get_page_data(session, url))
            tasks.append(task)
        await asyncio.gather(*tasks)


# @my_timer # if use only 1 process
def main(urls):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_page(urls, loop))
    # asyncio.run(get_page(), debug=False)
    print(len(current_items))


if __name__ == '__main__':
    while True:
        # main()
        start_time = datetime.now()
        with Pool(all_processes) as process:
            process.map(main, urls_list)
        print(datetime.now() - start_time)
