import time
import socket
import os

from typing import Union

from aioredis import ConnectionsPool, RedisConnection
from aioredis.commands import Redis

from aiopyrq import helpers
from aiopyrq.script import Script

CHUNK_SIZE = 10
PROCESSING_SUFFIX = '-processing'
PROCESSING_TIMEOUT_SUFFIX = '-timeouts'
PROCESSING_TIMEOUT = 7200  # seconds

DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100


class Queue(object):
    """
    Queue is a simple queue implemented using List as the queue, multiple Lists as the processing queues and a Hash as
    a storage for processing queue timeouts (so you can tell which processing queue is expired).
    There is no priority whatsoever - items are processed as they were inserted into the queue.

    Queue needs a garbage collector process because the queue creates a processing queues every time you request items
    from it. This process is implemented by the methods reEnqueue* and drop* of this class and they should be called
    before getting the items or periodically (if you don't care about the order of the items).

    author: Heureka.cz <vyvoj@heureka.cz>
    """

    def __init__(self, name: str, redis: Union[ConnectionsPool, RedisConnection, Redis], **kwargs):
        """
        :param name: Name of the queue
        :param redis: Redis connection
        :param **kwargs: [
            synced_slaves_enabled: bool Enables slave synchronous syncing
            synced_slaves_count: int Number of slaves that need to be synced in order to continue
            synced_slaves_timeout: int Timeout for syncing slaves. If reached, exception is raised
        ]
        :return:
        """
        self.client_id = '{0}[{1}][{2}]'.format(socket.gethostname(), os.getpid(), int(time.time()))
        self.name = name
        self.options = kwargs

        if isinstance(redis, (ConnectionsPool, RedisConnection)):
            redis = Redis(redis)

        self.redis = redis
        self._register_commands()

    def _register_commands(self) -> None:
        self.ack_command = self._register_script(self.QueueCommand.ack())
        self.get_command = self._register_script(self.QueueCommand.get())
        self.reject_command = self._register_script(self.QueueCommand.reject())
        self.re_enqueue_command = self._register_script(self.QueueCommand.re_enqueue())

    def _register_script(self, script: str) -> Script:
        return Script(self.redis, script)

    async def get_count(self) -> int:
        """
        :return: Number of items in the queue
        """
        return await self.redis.llen(self.name)

    async def add_item(self, item) -> bool:
        """
        :param item: Anything that is convertible to str
        :return: Returns true if item was inserted into queue, false otherwise
        """
        result = await self.redis.lpush(self.name, item)
        await self._wait_for_synced_slaves()

        return result

    async def add_items(self, items: list):
        """
        :param items: List of items to be added via pipeline
        """
        pipeline = self.redis.pipeline()

        for chunk in helpers.create_chunks(items, CHUNK_SIZE):
            pipeline.lpush(self.name, *chunk)
        await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return await self.get_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name],
                                      args=[count, int(time.time())])

    async def ack_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        :return: Success
        """
        await self.ack_command(keys=[self.processing_queue_name, self.timeouts_hash_name],
                               args=[str(item)])
        await self._wait_for_synced_slaves()

    async def ack_items(self, items: list) -> None:
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in items:
            await self.ack_command(keys=[self.processing_queue_name, self.timeouts_hash_name], args=[str(item)],
                                   client=pipeline)
        await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def reject_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        """
        await self.reject_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name],
                                  args=[str(item)])
        await self._wait_for_synced_slaves()

    async def reject_items(self, items: list) -> None:
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in reversed(items):
            await self.reject_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name], args=[str(item)],
                                      client=pipeline)
        await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def re_enqueue_timeout_items(self, timeout: int=PROCESSING_TIMEOUT) -> None:
        """
        :param timeout: int seconds
        """
        sorted_processing_queues = await self._get_sorted_processing_queues()

        for queue, value_time in sorted_processing_queues:
            if int(float(value_time)) + timeout < int(time.time()):
                await self.re_enqueue_command(keys=[self.name, queue, self.timeouts_hash_name])
        await self._wait_for_synced_slaves()

    async def re_enqueue_all_items(self) -> None:
        sorted_processing_queues = await self._get_sorted_processing_queues()

        for queue, value_time in sorted_processing_queues:
            await self.re_enqueue_command(keys=[self.name, queue, self.timeouts_hash_name])
        await self._wait_for_synced_slaves()

    async def drop_timeout_items(self, timeout: int=PROCESSING_TIMEOUT) -> None:
        """
        :param timeout: int seconds
        """
        sorted_processing_queues = await self._get_sorted_processing_queues()

        for queue, value_time in sorted_processing_queues:
            if int(float(value_time)) + timeout < int(time.time()):
                await self.redis.delete(queue)
                await self.redis.hdel(self.timeouts_hash_name, queue)
        await self._wait_for_synced_slaves()

    async def drop_all_items(self) -> None:
        sorted_processing_queues = await self._get_sorted_processing_queues()
        for queue, value_time in sorted_processing_queues:
            await self.redis.delete(queue)
            await self.redis.hdel(self.timeouts_hash_name, queue)
        await self._wait_for_synced_slaves()

    async def _get_sorted_processing_queues(self) -> list:
        return sorted(await helpers.async_iterate_to_list(self.redis.ihscan(self.timeouts_hash_name)), reverse=True)

    @property
    def processing_queue_name(self) -> str:
        """
        :return: Name of the processing queue
        """
        return self.name + PROCESSING_SUFFIX + '-' + self.client_id

    @property
    def timeouts_hash_name(self) -> str:
        """
        :return: Name of the timeouts hash
        """
        return self.name + PROCESSING_TIMEOUT_SUFFIX

    async def _wait_for_synced_slaves(self) -> None:
        if not self.options.get('synced_slaves_enabled'):
            return

        count = DEFAULT_SYNC_SLAVES_COUNT
        timeout = DEFAULT_SYNC_SLAVES_TIMEOUT

        if self.options['synced_slaves_count']:
            count = self.options['synced_slaves_count']

        if self.options['synced_slaves_timeout']:
            timeout = self.options['synced_slaves_timeout']

        await helpers.wait_for_synced_slaves(self.redis, count, timeout)

    class QueueCommand(object):

        @staticmethod
        def ack():
            """
            :return: LUA Script for ACK command
            """
            return """
            local processing = KEYS[1]
            local timeouts = KEYS[2]
            local item = ARGV[1]

            local result = redis.call('lrem', processing, -1, item)

            local count = redis.call('llen', processing)
            if count == 0 then
               redis.call('hdel', timeouts, processing)
            end
            """

        @staticmethod
        def get():
            """
            :return: LUA Script for GET command
            """
            return """
            local queue = KEYS[1]
            local processing = KEYS[2]
            local timeouts = KEYS[3]
            local size = ARGV[1]
            local time = ARGV[2]

            local item
            local items = {}

            redis.call('hset', timeouts, processing, time)

            for i = 1, size, 1 do
                item = redis.call('rpoplpush', queue, processing)

                if not item then
                    break
                end

                table.insert(items, item)
            end

            return items
            """

        @staticmethod
        def reject():
            """
            :return: LUA Script for REJECT command
            """
            return """
            local queue = KEYS[1]
            local processing = KEYS[2]
            local timeouts = KEYS[3]
            local item = ARGV[1]

            local removed = redis.call('lrem', processing, -1, item)

            if removed == 1 then
                redis.call('rpush', queue, item)
            end

            local count = redis.call('llen', processing)
            if count == 0 then
                redis.call('hdel', timeouts, processing)
            end
            """

        @staticmethod
        def re_enqueue():
            """
            :return: LUA Script for reject queue
            """
            return """
            local queue = KEYS[1]
            local processing = KEYS[2]
            local timeouts = KEYS[3]

            local item
            while true do
                item = redis.call('lpop', processing);

                if not item then
                    break
                end

                redis.call('rpush', queue, item)
            end

            redis.call('hdel', timeouts, processing)
            """
