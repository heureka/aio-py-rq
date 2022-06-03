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
limitations under the License.governing permissions and limitations
under the License.
"""
import time
import socket
import os
import pytest

from asynctest import patch

from aiopyrq.unique_queues import UniqueQueue, CHUNK_SIZE
from aioredis.connection import create_connection

QUEUE_NAME = os.getenv('QUEUE_NAME', 'test-queue')
PROCESSING_QUEUE_SCHEMA = QUEUE_NAME + '-processing-{}[{}][{}]'
TIMEOUT_QUEUE = QUEUE_NAME + '-timeouts'
SET_QUEUE_NAME = QUEUE_NAME + '-unique'

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASS', None)


async def init_test(**kwargs):

    synced_slaves_count = 1
    synced_slaves_timeout = 2
    client = await create_connection(address=(REDIS_HOST, REDIS_PORT), db=REDIS_DB, password=REDIS_PASSWORD,
                                     encoding='utf-8')

    await remove_all_test_queues(client)

    queue_instance = UniqueQueue(QUEUE_NAME, client, synced_slaves_enabled=True,
                                 synced_slaves_count=synced_slaves_count, synced_slaves_timeout=synced_slaves_timeout, **kwargs)

    return client, queue_instance


async def deactivate_test(client):
    await remove_all_test_queues(client)

    client.close()
    await client.wait_closed()


async def remove_all_test_queues(client):
    await client.execute('eval', """
            local keys = redis.call("keys", ARGV[1])
            for i, key in ipairs(keys) do
                redis.call("del", key)
            end
        """, 0, QUEUE_NAME + '*')


@pytest.mark.asyncio
async def test_add_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        items = ['first-message', 'second-message', 'first-message']

        await queue_instance.add_items(items)
        assert items[0] == await client.execute('rpop', QUEUE_NAME)
        assert items[1] == await client.execute('rpop', QUEUE_NAME)
        assert await client.execute('rpop', QUEUE_NAME) is None
        assert 1 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_add_items_with_multiple_chunks():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        chunks_count = 3
        items = ['item-{}'.format(i) for i in range(chunks_count * CHUNK_SIZE)]
        await queue_instance.add_items(items)
        assert slaves_mock.call_count == 1

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_add_item():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        items = [3, 5, 3, 1]
        for i in items:
            await queue_instance.add_item(i)
        actual_items = await client.execute('lrange', QUEUE_NAME, 0, 5)
        for item in ['5', '3', '1']:
            assert item in actual_items

        assert 4 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_get_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        for i in [3, 5, 3, 1]:
            await client.execute('lpush', QUEUE_NAME, i)
        assert ['3', '5', '3'] == await queue_instance.get_items(3)
        assert ['1'] == await queue_instance.get_items(1)
        assert [] == await queue_instance.get_items(1)
        await client.execute('del', queue_instance.processing_queue_name)
        await client.execute('del', queue_instance.timeouts_hash_name)
        assert 0 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_delete_item():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        for i in [3, 5, 6, 3, 1, 4]:
            await client.execute('lpush', QUEUE_NAME, i)
        await queue_instance.delete_item(3)
        assert ['4', '1', '6', '5'] == await client.execute('lrange', QUEUE_NAME, 0, -1)

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_delete_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        for i in [3, 5, 6, 3, 1, 4]:
            await client.execute('lpush', QUEUE_NAME, i)
        await queue_instance.delete_items([1, 3, 4, 5])
        assert ['6'] == await client.execute('lrange', QUEUE_NAME, 0, -1)

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_ack_item():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        await client.execute('lpush', queue_instance.processing_queue_name, *[1, 5, 5, 3])

        saved_time = int(time.time())
        await client.execute('hset', queue_instance.timeouts_hash_name, queue_instance.processing_queue_name, saved_time)
        for i in [1, 5, 1]:
            await queue_instance.ack_item(i)

        assert ['3', '5'] == await client.execute('lrange', queue_instance.processing_queue_name, 0, 5)
        assert [queue_instance.processing_queue_name,  str(saved_time)] == \
            await client.execute('hgetall', queue_instance.timeouts_hash_name)

        for i in [5, 3]:
            await queue_instance.ack_item(i)

        assert 0 == await client.execute('llen', queue_instance.processing_queue_name)
        assert 5 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_ack_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        await client.execute('lpush', queue_instance.processing_queue_name, *[1, 5, 5, 3, 6, 7])
        saved_time = int(time.time())
        await client.execute('hset', queue_instance.timeouts_hash_name, queue_instance.processing_queue_name, saved_time)
        await queue_instance.ack_items([1, 5])
        await queue_instance.ack_items([1])

        assert ['7', '6', '3', '5'] == await client.execute('lrange', queue_instance.processing_queue_name, 0, 5)
        assert [queue_instance.processing_queue_name,  str(saved_time)] == await client.execute('hgetall', queue_instance.timeouts_hash_name)

        await queue_instance.ack_items([5, 3, 6])
        await queue_instance.ack_items([7])
        assert 0 == await client.execute('llen', queue_instance.processing_queue_name)
        assert 4 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_reject_item():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        await client.execute('lpush', queue_instance.processing_queue_name, *[1, 5, 5, 3])
        saved_time = int(time.time())
        await client.execute('hset', queue_instance.timeouts_hash_name, queue_instance.processing_queue_name, saved_time)

        await queue_instance.reject_item(1)
        await queue_instance.reject_item(5)
        await queue_instance.reject_item(1)

        assert ['1', '5'] == await client.execute('lrange', QUEUE_NAME, 0, 5)
        assert ['3', '5'] == await client.execute('lrange', queue_instance.processing_queue_name, 0, 5)
        assert [queue_instance.processing_queue_name,  str(saved_time)] == await client.execute('hgetall', queue_instance.timeouts_hash_name)

        await queue_instance.reject_item(3)
        await queue_instance.reject_item(5)
        assert ['1', '5', '3'] == await client.execute('lrange', QUEUE_NAME, 0, 5)
        assert 0 == await client.execute('llen', queue_instance.processing_queue_name)
        assert 5 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_reject_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        await client.execute('lpush', queue_instance.processing_queue_name, *[1, 5, 5, 3, 6, 7])
        saved_time = int(time.time())
        await client.execute('hset', queue_instance.timeouts_hash_name, queue_instance.processing_queue_name, saved_time)

        await queue_instance.reject_items([1, 5])
        await queue_instance.reject_items([5])
        await queue_instance.reject_items([9])

        assert ['5', '1'] == await client.execute('lrange', QUEUE_NAME, 0, 5)
        assert ['7', '6', '3'] == await client.execute('lrange', queue_instance.processing_queue_name, 0, 5)
        assert [queue_instance.processing_queue_name,  str(saved_time)] == await client.execute('hgetall', queue_instance.timeouts_hash_name)

        await queue_instance.reject_items([3, 6, 7])
        assert ['5', '1', '7', '6', '3'] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert 0 == await client.execute('llen', queue_instance.processing_queue_name)
        assert 4 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_integration():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        await queue_instance.add_items([1, 5, 2, 6, 7])
        assert ['1', '5', '2', '6', '7'] == await queue_instance.get_items(5)
        assert [] == await queue_instance.get_items(1)
        await queue_instance.ack_items([1, 5])
        assert [] == await queue_instance.get_items(1)
        await queue_instance.reject_items([2, 6, 7])
        assert ['2', '6', '7'] == await queue_instance.get_items(5)
        await queue_instance.ack_items([2, 6, 7])
        assert 0 == await client.execute('llen', QUEUE_NAME)
        assert 4 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_re_enqueue_timeout_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        await client.execute('lpush', processing_queue1, 1, 5, 3)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        await client.execute('lpush', processing_queue2, 1, 4, 6)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        await client.execute('lpush', processing_queue3, 4, 7, 8)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        await queue_instance.re_enqueue_timeout_items(7)

        assert ['6', '4', '3', '5', '1'] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert ['8', '7', '4'] == await client.execute('lrange', processing_queue3, 0, 5)
        assert [processing_queue3,  str(microtimestamp - 5)] == await client.execute('hgetall', TIMEOUT_QUEUE)
        assert [QUEUE_NAME, processing_queue3, TIMEOUT_QUEUE, SET_QUEUE_NAME] == \
               sorted(await client.execute('keys', QUEUE_NAME + '*'))

        await queue_instance.re_enqueue_timeout_items(0)

        assert ['6', '3', '5', '1', '8', '7', '4'] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert [QUEUE_NAME, SET_QUEUE_NAME] == sorted(await client.execute('keys', QUEUE_NAME + '*'))

        assert 2 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_re_enqueue_all_times():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        await client.execute('lpush', processing_queue1, 1, 5, 3)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        await client.execute('lpush', processing_queue2, 1, 4, 6)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        await client.execute('lpush', processing_queue3, 4, 7, 8)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        await queue_instance.re_enqueue_all_items()

        assert ['8', '7', '6', '4', '3', '5', '1'] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert [QUEUE_NAME, SET_QUEUE_NAME] == sorted(await client.execute('keys', QUEUE_NAME + '*'))

        assert 1 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_drop_timeout_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        await client.execute('lpush', processing_queue1, 1, 5, 3)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        await client.execute('lpush', processing_queue2, 1, 4, 6)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        await client.execute('lpush', processing_queue3, 4, 7, 8)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        await queue_instance.drop_timeout_items(7)

        assert [] == await client.execute('lrange', QUEUE_NAME, 0, 5)
        assert ['8', '7', '4'] == await client.execute('lrange', processing_queue3, 0, 5)
        assert [processing_queue3,  str(microtimestamp - 5)] == await client.execute('hgetall', TIMEOUT_QUEUE)
        assert [processing_queue3, TIMEOUT_QUEUE] == sorted(await client.execute('keys', QUEUE_NAME + '*'))

        await queue_instance.drop_timeout_items(0)

        assert [] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert [] == await client.execute('keys', QUEUE_NAME + '*')

        assert 2 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_drop_all_items():
    client, queue_instance = await init_test()
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        await client.execute('lpush', processing_queue1, 1, 5, 3)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        await client.execute('lpush', processing_queue2, 1, 4, 6)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        await client.execute('lpush', processing_queue3, 4, 7, 8)
        await client.execute('hset', TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        await queue_instance.drop_all_items()

        assert [] == await client.execute('lrange', QUEUE_NAME, 0, 10)
        assert [] == await client.execute('keys', QUEUE_NAME + '*')

        assert 1 == slaves_mock.call_count

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_rollback_timeout(patch_time):
    max_retry = 3
    max_minutes = 10
    client, queue_instance = await init_test(max_retry_rollback=max_retry, max_timeout_in_queue=max_minutes)
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        items = [1,2,3,4]

        await client.execute('lpush', queue_instance.processing_queue_name, *items)

        await queue_instance.drop_all_items()
        unrolledback_items = []

        time_start = int(time.time()/60)
        time_now = int(time.time()/60)
        rollback_counter = 0
        for _ in range(100):  # add one for possible inconsistencies in timing
            for item in items[1:]:
                can_rollback = await queue_instance.can_rollback_item(item)
                if can_rollback:
                    rollback_counter += 1
                    # a reject would happen here in normal situation
                else:
                    unrolledback_items.append(item)
            if len(set(unrolledback_items)) == 3:
                break
            time_now = int(time.time()/60)

        assert len(set(unrolledback_items)) == 3

        can_rollback = await queue_instance.can_rollback_item(items[0])
        assert can_rollback == True

    await deactivate_test(client)

if __name__ == 'main':
    pytest.main()
