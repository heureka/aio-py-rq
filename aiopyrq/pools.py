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

DEFAULT_CHUNK_SIZE = 100
DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100
DEFAULT_ACK_TTL = 600  # seconds
DEFAULT_ACK_VALID_FOR = 129600  # seconds


class Pool(object):
    def __init__(self, name: str, redis: Union[ConnectionPool, Connection, Redis], **kwargs):
        """
        Pool is intended for "queues" which are most of the time the same - the items need to be processed periodically.
        This is exactly how the pool works - it processes items that are "outdated". When the item is processed (ACKed),
        the validity of the item is set accordingly to the options (e.g. for 36 hours as of default).

        There is no need for a garbage collector process - items that were failed to process are automatically processed
        after the ACK_TTL time has passed.

        :param name: Name of the pool
        :param redis: Redis client
        :param **kwargs: [
            chunk_size: int Size of chunks
            synced_slaves_enabled: bool Enables slave synchronous syncing
            synced_slaves_count: int Number of slaves that need to be synced in order to continue
            synced_slaves_timeout: int Timeout for syncing slaves. If reached, exception is raised
            ack_ttl: int Acknowledge timeout of the just processed items
        ]
        """
        self.client_id = '{0}[{1}][{2}]'.format(socket.gethostname(), os.getpid(), int(time.time()))

        if isinstance(redis, (ConnectionPool, Connection)):
            redis = Redis(connection_pool=redis)

        self.redis = redis
        self.name = name
        self.options = self._load_options(kwargs)
        self._register_commands()

    @staticmethod
    def _load_options(kwargs):
        return {
            'chunk_size': kwargs.get('chunk_size', DEFAULT_CHUNK_SIZE),
            'synced_slaves_enabled': kwargs.get('synced_slaves_enabled', False),
            'synced_slaves_count': kwargs.get('synced_slaves_count', DEFAULT_SYNC_SLAVES_COUNT),
            'synced_slaves_timeout': kwargs.get('synced_slaves_timeout', DEFAULT_SYNC_SLAVES_TIMEOUT),
            'ack_ttl': kwargs.get('ack_ttl', DEFAULT_ACK_TTL),
            'ack_valid_for': kwargs.get('ack_valid_for', DEFAULT_ACK_VALID_FOR)
        }

    def _register_commands(self):
        self.ack_command = self._register_script(self.PoolCommand.ack())
        self.get_command = self._register_script(self.PoolCommand.get())
        self.remove_command = self._register_script(self.PoolCommand.remove())

    def _register_script(self, script: str) -> AsyncScript:
        return AsyncScript(self.redis, script)

    async def get_count(self) -> int:
        """
        :return: Number of items in the pool
        """
        return await self.redis.zcard(self.name)

    async def get_count_to_process(self) -> int:
        """
        :return: Number of items in the pool which should be processed
        """
        return await self.redis.zcount(self.name, float('-inf'), int(time.time()))

    async def is_in_pool(self, item) -> bool:
        """
        :return: Checks if the given item is present in the pool
        """
        return await self.redis.zscore(self.name, item) is not None

    async def add_item(self, item) -> None:
        """
        :param item: Anything that is convertible to str
        """
        await self.redis.zadd(self.name, {item: int(time.time())})
        await self._wait_for_synced_slaves()

    async def add_items(self, items) -> None:
        """
        :param items: List of items to be added via pipeline
        """
        pipeline = self.redis.pipeline()
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            current_time = int(time.time())
            prepared_items = {}
            for item in chunk:
                prepared_items.update({
                    item: current_time,
                })
            pipeline.zadd(self.name, prepared_items)
        await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return await self.get_command(keys=[self.name], args=[count, int(time.time()), self.options['ack_ttl']])

    async def get_all_items(self) -> list:
        """
        :return: List of all items
        """
        result = []
        while True:
            chunk = await self.get_items(self.options['chunk_size'])
            result += chunk

            if len(chunk) < self.options['chunk_size']:
                break
        return result

    async def ack_item(self, item) -> None:
        """ Acknowledges an item that was processed correctly
        :param item: Anything that is convertible to str
        """
        await self.ack_command(keys=[self.name], args=[item, int(time.time()) + self.options['ack_valid_for']])
        await self._wait_for_synced_slaves()

    async def ack_items(self, items) -> None:
        """ Acknowledges items that were processed correctly
        :param items: List of items that are convertible to str
        """
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            pipeline = self.redis.pipeline()
            for item in chunk:
                await self.ack_command(keys=[self.name], args=[item, int(time.time()) + self.options['ack_valid_for']])
            await pipeline.execute()
            await self._wait_for_synced_slaves()

    async def remove_item(self, item) -> None:
        """ Removes an item that is no longer valid
        :param item: Anything that is convertible to str
        """
        await self.remove_command(keys=[self.name], args=[item])
        await self._wait_for_synced_slaves()

    async def remove_items(self, items) -> None:
        """ Removes an item that is no longer valid
        :param items: List of items that are convertible to str
        """
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            pipeline = self.redis.pipeline()
            for item in chunk:
                await self.remove_command(keys=[self.name],
                                          args=[item])
            await pipeline.execute()
            await self._wait_for_synced_slaves()

    async def clear_pool(self) -> None:
        """ Clears all the items from the pool """
        while True:
            removed = await self.redis.zremrangebyrank(self.name, 0, self.options['chunk_size'])
            if not removed:
                break

    async def delete_item(self, item) -> None:
        """ Will delete item from pool.
        :param item: Anything that is convertible to str
        """
        await self.redis.zrem(self.name, item)

    async def delete_items(self, items: list) -> None:
        """
        Will delete items from the pool via pipeline.
        :param items: List of items to be deleted via pipeline
        """
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            pipeline = self.redis.pipeline()
            for item in chunk:
                pipeline.zrem(self.name, item)
            await pipeline.execute()
        await self._wait_for_synced_slaves()

    async def _wait_for_synced_slaves(self):
        if self.options['synced_slaves_enabled']:
            await helpers.wait_for_synced_slaves(self.redis, self.options['synced_slaves_count'],
                                                 self.options['synced_slaves_timeout'])

    class PoolCommand(object):

        @staticmethod
        def ack():
            """
            :return: LUA Script for ACK command
            """
            return """
            local pool = KEYS[1]
            local item = ARGV[1]
            local validUntil = ARGV[2]

            local score = redis.call('zscore', pool, item)
            if score and score - math.floor(score) > 0.01 then
                redis.call('zadd', pool, validUntil, item)
            end
            """

        @staticmethod
        def get():
            """
            :return: LUA Script for GET command
            """
            return """
            local pool = KEYS[1]
            local size = ARGV[1]
            local time = ARGV[2]
            local ackTTL = ARGV[3]

            local result = redis.call('zrangebyscore', pool, '-inf', time, 'WITHSCORES', 'LIMIT', 0, size)
            local finalResult = {}
            local i
            local value
            local score
            for i = 1, #result, 2 do
                value = result[i]
                score = math.floor(result[i + 1])
                redis.call('zadd', pool, score + tonumber(ackTTL) + 0.1, value)
                table.insert(finalResult, value)
            end

            return finalResult
            """

        @staticmethod
        def remove():
            """
            :return: LUA Script for REMOVE command
            """
            return """
            local pool = KEYS[1]
            local item = ARGV[1]

            local score = redis.call('zscore', pool, item)
            if score and score - math.floor(score) > 0.01 then
                redis.call('zrem', pool, item)
            end
            """
