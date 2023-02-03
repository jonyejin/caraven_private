import argparse
import asyncio
import warnings
from asyncio import Semaphore
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional, TypeVar, Union, Tuple, TextIO
import tqdm
from aiohttp import ClientSession, ClientTimeout

import utils as utils

# Ignore warnings from `aiohttp` module.
warnings.filterwarnings("ignore", module="aiohttp")

T = TypeVar("T")


class Crawler:
    def __init__(
            self,
            concurrent_tasks: int = 500,
            num_parsing_processes: int = 1,
            request_headers: Optional[Dict[str, str]] = None,
            request_timeout: Optional[float] = None,
            folder_name: str = 'news/'

    ):
        self.concurrent_tasks = concurrent_tasks
        self.num_parsing_processes = num_parsing_processes
        self.request_headers = request_headers
        self.request_timeout = request_timeout

    comparing_with_last_page_first_url = ""

    async def is_duplicate(
            self,
            pool: Executor,
            sess: ClientSession,
            url: str,
            parse_fn: Optional[Callable[[str], T]] = None,
    ) -> bool:
        # 하나의 날에 대해서 중복인 부분까지 실행하고, False를 리턴해주는 함수.

        try:
            async with sess.get(url, ssl=False) as resp:
                content = await resp.text()

                first_url = await asyncio.get_event_loop().run_in_executor(
                    pool, parse_fn, content
                )
                first_url = first_url[0]
                if first_url == self.comparing_with_last_page_first_url:
                    print(url)
                    raise ValueError("same page from now on")
                else:
                    self.comparing_with_last_page_first_url = first_url
                    return False
        except Exception:
            return True

    async def _fetch_and_parse(
            self,
            sem: Semaphore,
            pool: Executor,
            sess: ClientSession,
            url: str,
            include_reporter_name: bool,
            parse_fn: Optional[Callable[[str, bool], T]] = None,
    ) -> Tuple[str, str]:
        try:
            async with sess.get(url, ssl=False) as resp:
                content = await resp.text()

            # Run `parse_fn` in subprocess from process-pool for parallelism.
            if parse_fn is not None:
                content = await asyncio.get_event_loop().run_in_executor(
                    pool, parse_fn, content, include_reporter_name
                )
        except Exception:
            content = None

        sem.release()
        return (url, content)

    async def _preprocess_urls_for_day(self, same_date_urls, pool, sess, parse_fn, callback_fn) -> [str]:
        final_url = []
        for index, url in enumerate(same_date_urls):
            dup = await self.is_duplicate(pool, sess, url, parse_fn)
            if dup:
                # add to list
                final_url = same_date_urls[:index]
                break
            elif index == len(same_date_urls)-1:
                final_url = same_date_urls[:index]
        callback_fn()
        return final_url

    async def _preprocess_urls(
            self,
            urls: Iterable[Iterable[str]],  # 모든 url
            include_reporter_name: bool,
            sess: ClientSession,
            parse_fn: Optional[Callable[[str], T]] = None,
            callback_fn: Optional[Callable[[Tuple[str, str]], None]] = None,
    ) -> Iterable[str]:
        res = []
        # yejin: 하루 안에서는 순서대로 해 겹치지 말고
        
        pool = ProcessPoolExecutor(max_workers=10)

        # without_overlaps
        final_urls: Iterable[Iterable[str]] = []
        futures = []

        with tqdm.tqdm(urls, desc="[*] check duplicated urls and make flattened list") as tbar:
            # 하루에 대해서
            for same_date_urls in urls:
                f = asyncio.ensure_future(self._preprocess_urls_for_day(same_date_urls=same_date_urls,
                                                                        pool=pool,
                                                                        sess=sess,
                                                                        parse_fn=parse_fn,
                                                                        callback_fn=tbar.update
                                                                        ))
                futures.append(f)
            # flatten처리
            done, _ = await asyncio.wait(futures) # done: [Task]
            for task in done:
                res += task.result()

        # done.result()
        # Q) 이거 맞나?
        return res

    async def _crawl_and_reduce(
            self,
            urls: Iterable[str],  # flatten한 모든 url
            include_reporter_name: bool,
            parse_fn: Optional[Callable[[str], T]] = None,
            callback_fn: Optional[Callable[[Tuple[str, str]], None]] = None,
    ):
        # yejin: 하루 안에서는 순서대로 해 겹치지 말고
        # # Create a semaphore to limit the number of concurrent tasks, a process-pool
        # # executor to run `parse_fn` in parallel and a http client session for
        # # asynchronous HTTP requests.
        sem = Semaphore(self.concurrent_tasks)
        pool = ProcessPoolExecutor(max_workers=self.num_parsing_processes)
        sess = ClientSession(
            headers=self.request_headers,
            timeout=ClientTimeout(total=self.request_timeout),
        )
        futures = []
        # 하루에 대해서
        pool = ProcessPoolExecutor(max_workers=self.num_parsing_processes)
        for url in urls:
            await sem.acquire()

            # Create a fetching future.
            f = asyncio.ensure_future(
                self._fetch_and_parse(sem, pool, sess, url, include_reporter_name, parse_fn)
            )
            # Add done-callback function to the future.
            if callback_fn is not None:
                f.add_done_callback(lambda k: callback_fn(data=k.result()))
            futures.append(f)

            # Wait for the tasks to be complete and close the http client session and
            # process-pool executor
        await asyncio.wait(futures)
        await sess.close()
        pool.shutdown(wait=True)

    def reduce_to_array(
            self,
            sess:ClientSession,
            urls: Iterable[str],  # 모든 페이지
            include_reporter_name: bool,
            parse_fn: Optional[Callable[[str], T]] = None,
            update_fn: Optional[Callable[[], None]] = None,
    ) -> List[T]:
        # A callback function to reduce collected data to the array.
        def callback_fn(data: Tuple[Optional[str], Optional[str]]):
            if update_fn is not None:
                update_fn()

            if data[1] is not None:
                results.append(data[1])

        results = []

        # 중복 제거

        # Get event loop and set to ignore `SSLError`s from `aiohttp` module.
        loop = asyncio.get_event_loop()
        utils.ignore_aiohttp_ssl_error(loop)
        done = loop.run_until_complete(self._preprocess_urls(urls, False, sess, parse_fn)) # 리턴값; 하루에 봐야할 페이지들
        loop.close()

        loop = asyncio.get_event_loop()
        utils.ignore_aiohttp_ssl_error(loop)
        loop.run_until_complete(self._crawl_and_reduce(done, include_reporter_name, parse_fn, callback_fn))
        loop.close()
        return results

    def reduce_to_file(
            self,
            urls: Iterable[str],
            filename: str,
            include_reporter_name: bool,
            parse_fn: Optional[Callable[[str], T]] = None,
            update_fn: Optional[Callable[[], None]] = None,
    ) -> int:
        local = True
        if local:
            written = 0

            # A callback function to reduce collected data to the output file.
            def callback_fn(data: Tuple[Optional[str], Optional[str]]):
                nonlocal written
                single_fp = open(f"news/{written}.txt", "w")
                if update_fn is not None:
                    update_fn()

                if data[1] is not None:
                    # Increase the counter which indicates the number of actual reduced
                    # items.
                    written += 1

                    single_fp.write(str(data[1]) + "\n")
                    single_fp.close()

                # Get event loop and set to ignore `SSLError`s from `aiohttp` module.

            loop = asyncio.get_event_loop()
            # utils.ignore_aiohttp_ssl_error(loop)
            loop.run_until_complete(self._crawl_and_reduce(urls, include_reporter_name, parse_fn, callback_fn))
        else:
            with open(filename, "w") as fp:
                # A callback function to reduce collected data to the output file.
                def callback_fn(data: Tuple[Optional[str], Optional[str]]):
                    if update_fn is not None:
                        update_fn()

                    if data is not None:
                        # Increase the counter which indicates the number of actual reduced
                        # items.
                        nonlocal written
                        written += 1

                        fp.write(str(data) + "\n")

                # Get event loop and set to ignore `SSLError`s from `aiohttp` module.
                loop = asyncio.get_event_loop()
                # utils.ignore_aiohttp_ssl_error(loop)

                written = 0
                loop.run_until_complete(self._crawl_and_reduce(urls, include_reporter_name, parse_fn, callback_fn))

        return written
