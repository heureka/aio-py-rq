"""
Copyright (c) 2021 Heureka Group a.s. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import time
import socket
import os

from typing import Union

from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool, Connection
from redis.commands.core import AsyncScript

from aiopyrq import helpers

CHUNK_SIZE = 10
SET_QUEUE_SUFFIX = '-unique'
RETRY_SUFFIX = '-retry-count'
TIMEOUT_SUFFIX = '-retry-timeout'
PROCESSING_SUFFIX = '-processing'
PROCESSING_TIMEOUT_SUFFIX = '-timeouts'
PROCESSING_TIMEOUT = 7200  # seconds

DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100


class UniqueQueue(object):
    """
    UniqueQueue is a queue implemented using List as the queue, multiple Lists as the processing queues and a Hash as
    a storage for processing queue timeouts (so you can tell which processing queue is expired).
    UniqueQueue items are always unique, adding the same item more than once will be ignored.
    There is no priority whatsoever - items are processed as they were inserted into the queue.

    Queue needs a garbage collector process because the queue creates a processing queues every time you request items
    from it. This process is implemented by the methods reEnqueue* and drop* of this class and they should be called
    before getting the items or periodically (if you don't care about the order of the items).

    author: Heureka.cz <vyvoj@heureka.cz>
    """

    def __init__(self, queue_name: str, redis: Union[ConnectionPool, Connection, Redis], **kwargs):
        """
        :param queue_name: Name of the queue
        :param redis: Redis client
        :param **kwargs: [
            synced_slaves_enabled: bool Enables slave synchronous syncing
            synced_slaves_count: int Number of slaves that need to be synced in order to continue
            synced_slaves_timeout: int Timeout for syncing slaves. If reached, exception is raised
        ]
        :return:
        """
        self.client_id = '{0}[{1}][{2}]'.format(socket.gethostname(), os.getpid(), int(time.time()))

        if isinstance(redis, (ConnectionPool, Connection)):
            redis = Redis(connection_pool=redis)

        self.redis = redis
        self.queue_name = queue_name
        self.options = kwargs
        self._register_commands()

    def _register_commands(self) -> None:
        self.add_command = self._register_script(self.QueueCommand.add())
        self.ack_command = self._register_script(self.QueueCommand.ack())
        self.get_command = self._register_script(self.QueueCommand.get())
        self.delete_command = self._register_script(self.QueueCommand.delete())
        self.reject_command = self._register_script(self.QueueCommand.reject())
        self.re_enqueue_command = self._register_script(self.QueueCommand.re_enqueue())
        self.get_retry_and_timeout_command = self._register_script(self.QueueCommand.get_retry_and_timeout())
        self.reset_retry_and_timeout_command = self._register_script(self.QueueCommand.reset_retry_and_timeout())

    def _register_script(self, script: str) -> AsyncScript:
        return AsyncScript(self.redis, script)

    async def get_count(self) -> int:
        """
        :return: Number of items in the queue
        """
        return await self.redis.llen(self.queue_name)

    async def add_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        """
        await self.add_command(keys=[self.queue_name, self.set_name], args=[str(item)])

        await self._wait_for_synced_slaves()

    async def add_items(self, items: list) -> None:
        """
        :param items: List of items to be added via pipeline
        """
        for chunk in helpers.create_chunks(items, CHUNK_SIZE):
            pipeline = self.redis.pipeline()
            for item in chunk:
                await self.add_command(keys=[self.queue_name, self.set_name], args=[str(item)], client=pipeline)
            await pipeline.execute()

        await self._wait_for_synced_slaves()

    async def delete_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        """
        await self.delete_command(keys=[self.queue_name, self.set_name], args=[str(item)])

        await self._wait_for_synced_slaves()

    async def delete_items(self, items: list) -> None:
        """
        :param items: List of items to be deleted via pipeline
        """
        for chunk in helpers.create_chunks(items, CHUNK_SIZE):
            pipeline = self.redis.pipeline()
            for item in chunk:
                await self.delete_command(keys=[self.queue_name, self.set_name], args=[str(item)], client=pipeline)
            await pipeline.execute()

        await self._wait_for_synced_slaves()

    async def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return await self.get_command(keys=[self.queue_name, self.set_name, self.processing_queue_name,
                                      self.timeouts_hash_name], args=[count, int(time.time())])

    async def ack_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
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

    async def can_rollback_item(self, item) -> bool:
        """
        Check if `item` has not been rolledback too many times (`self.options['max_retry_rollback']`).
        Each item is, however, allowed to stay in the queue for some specific time (`self.options['max_timeout_in_queue']`).
        Because item which is rolled back several times in a short period should not be removed.

        :param item: Anything that is convertible to str
        """

        if not self.options['max_retry_rollback'] or not self.options['max_timeout_in_queue']:
            raise ValueError('Arguments `max_retry_rollback` or `max_timeout_in_queue` not supplied to UniqueQueue.')

        now_time = int(time.time()/60)
        current_retry_count, current_timeout = await self.get_retry_and_timeout_command(keys=[self.retry_count_name, self.retry_timeout_name],
                                                                          args=[str(item), now_time])
        current_retry_count, current_timeout = int(current_retry_count), int(current_timeout)

        if current_retry_count > self.options['max_retry_rollback'] and now_time - current_timeout > self.options['max_timeout_in_queue']:
            # too many rollbacks
            return False
        return True

    async def reset_blocked_item(self, item) -> None:
        """
        Remove the retry counter and reset the timeout for `item` is either of them exist.

        :param item: Anything that is convertible to str
        """
        await self.reset_retry_and_timeout_command(keys=[self.retry_count_name, self.retry_timeout_name], args=[str(item)])

    async def reject_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        """
        await self.reject_command(keys=[self.queue_name, self.set_name, self.processing_queue_name, self.timeouts_hash_name],
                                  args=[str(item)])
        await self._wait_for_synced_slaves()

    async def reject_items(self, items: list) -> None:
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in reversed(items):
            await self.reject_command(keys=[self.queue_name, self.set_name, self.processing_queue_name,
                                      self.timeouts_hash_name], args=[str(item)], client=pipeline)
        await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def re_enqueue_timeout_items(self, timeout: int=PROCESSING_TIMEOUT) -> None:
        """
        :param timeout: int seconds
        """
        sorted_processing_queues = await self._get_sorted_processing_queues()

        for queue, value_time in sorted_processing_queues:
            if int(float(value_time)) + timeout < int(time.time()):
                await self.re_enqueue_command(keys=[self.queue_name, self.set_name, queue, self.timeouts_hash_name])
        await self._wait_for_synced_slaves()

    async def re_enqueue_all_items(self) -> None:
        sorted_processing_queues = await self._get_sorted_processing_queues()

        for queue, value_time in sorted_processing_queues:
            await self.re_enqueue_command(keys=[self.queue_name, self.set_name, queue, self.timeouts_hash_name])
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
        return sorted(await helpers.async_iterate_to_list(self.redis.hscan_iter(self.timeouts_hash_name)), reverse=True)

    @property
    def set_name(self) -> str:
        """
        :return: Name of the set queue
        """
        return self.queue_name + SET_QUEUE_SUFFIX

    @property
    def retry_count_name(self) -> str:
        """
        :return: Name of the retry counter hash
        """
        return self.queue_name + RETRY_SUFFIX

    @property
    def retry_timeout_name(self) -> str:
        """
        :return: Name of the retry timeout hash
        """
        return self.queue_name + TIMEOUT_SUFFIX

    @property
    def processing_queue_name(self) -> str:
        """
        :return: Name of the processing queue
        """
        return self.queue_name + PROCESSING_SUFFIX + '-' + self.client_id

    @property
    def timeouts_hash_name(self) -> str:
        """
        :return: Name of the timeouts hash
        """
        return self.queue_name + PROCESSING_TIMEOUT_SUFFIX

    async def _wait_for_synced_slaves(self) -> None:
        if self.options.get('synced_slaves_enabled'):
            count = self.options['synced_slaves_count'] if self.options['synced_slaves_count'] \
                else DEFAULT_SYNC_SLAVES_COUNT
            timeout = self.options['synced_slaves_timeout'] if self.options['synced_slaves_timeout'] \
                else DEFAULT_SYNC_SLAVES_TIMEOUT
            await helpers.wait_for_synced_slaves(self.redis, count, timeout)

    class QueueCommand(object):

        @staticmethod
        def add():
            """
            :return: LUA Script for ACK command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local item = ARGV[1]

            local inQueue = redis.call('sismember', set, item)
            if inQueue == 0 then
                redis.call('lpush', queue, item)
                redis.call('sadd', set, item)
            end
            """

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
        def get_retry_and_timeout():
            """
            :return: LUA Script for getting timeout and retry count
            """
            return """
            local hash_retry = KEYS[1]
            local hash_timeout = KEYS[2]

            local item = ARGV[1]
            local now_time = ARGV[2]

            local count = redis.call('hget', hash_retry, item)

            if not count then
                redis.call('hset', hash_retry, item, 0)
                redis.call('hset', hash_timeout, item, now_time)
            end

            redis.call('hincrby', hash_retry, item, 1)

            local orig_time = redis.call('hget', hash_timeout, item)
            local retry_count = redis.call('hget', hash_retry, item)

            return {retry_count, orig_time}
            """

        @staticmethod
        def get():
            """
            :return: LUA Script for GET command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local size = ARGV[1]
            local time = ARGV[2]
            redis.call('hset', timeouts, processing, time)
            local item
            local items = {}
            for i = 1, size, 1 do
                item = redis.call('rpoplpush', queue, processing)
                if not item then
                    break
                end
                redis.call('srem', set, item)
                table.insert(items, item)
            end
            return items
            """

        @staticmethod
        def delete():
            """
            :return: LUA Script for DELETE command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local item = ARGV[1]
            local removed = redis.call('lrem', queue, 1, item)
            redis.call('srem', set, item)
            """

        @staticmethod
        def reject():
            """
            :return: LUA Script for REJECT command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local item = ARGV[1]
            local removed = redis.call('lrem', processing, -1, item)
            if removed == 1 then
                local inQueue = redis.call('sismember', set, item)
                if inQueue == 0 then
                    redis.call('rpush', queue, item)
                    redis.call('sadd', set, item)
                end
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
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local item
            local inQueue
            while true do
                item = redis.call('lpop', processing);
                if not item then
                    break
                end
                inQueue = redis.call('sismember', set, item)
                if inQueue == 0 then
                    redis.call('rpush', queue, item)
                    redis.call('sadd', set, item)
                else
                    redis.call('lrem', queue, -1, item)
                    redis.call('rpush', queue, item)
                end
            end
            redis.call('hdel', timeouts, processing)
            """

        @staticmethod
        def reset_retry_and_timeout():
            """
            :return: LUA Script for getting timeout and retry count
            """
            return """
            local hash_retry = KEYS[1]
            local hash_timeout = KEYS[2]

            local item = ARGV[1]

            redis.call('hdel', hash_retry, item)
            redis.call('hdel', hash_timeout, item)
            """
