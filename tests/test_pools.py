import time
import os
import pytest

from asynctest import patch
from aioredis.commands import create_redis

from aiopyrq.pools import Pool


POOL_NAME = os.getenv('POOL_NAME', 'test-pool')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASS', None)

TEST_TIME = 1444222459.0


async def init_test():

    synced_slaves_count = 1
    synced_slaves_timeout = 2
    client = await create_redis(address=(REDIS_HOST, REDIS_PORT), db=REDIS_DB, password=REDIS_PASSWORD,
                                encoding='utf-8')

    await remove_all_test_queues(client)

    pool_instance = Pool(POOL_NAME, client, synced_slaves_enabled=True, synced_slaves_count=synced_slaves_count,
                         synced_slaves_timeout=synced_slaves_timeout)

    return client, pool_instance


async def deactivate_test(client):
    await remove_all_test_queues(client)

    client.close()
    await client.wait_closed()


async def remove_all_test_queues(client):
    await client.delete(POOL_NAME)


@pytest.mark.asyncio
async def test_get_count():
    client, pool_instance = await init_test()
    await _load_items_to_pool(client, 'a', 'b')

    assert 2 == await pool_instance.get_count()
    assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_get_count_to_process():
    client, pool_instance = await init_test()
    timestamp = int(time.time())
    pairs = [timestamp - 5, 'a', timestamp - 3, 'b', timestamp + 5, 'c']
    await client.zadd(POOL_NAME, *pairs)

    assert 2 == await pool_instance.get_count_to_process()
    assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_is_in_pool():
    client, pool_instance = await init_test()
    await _load_items_to_pool(client, 'a', 'b')
    assert await pool_instance.is_in_pool('a')
    assert await pool_instance.is_in_pool('b')
    assert await pool_instance.is_in_pool('whatever') is False

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_add_item():
    client, pool_instance = await init_test()
    with patch('aiopyrq.pools.time.time') as time_mock, patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        time_mock.return_value = TEST_TIME

        await pool_instance.add_item('test1')
        await pool_instance.add_item('test2')
        await pool_instance.add_item('test3')
        await pool_instance.add_item('test2')

        assert [('test1', TEST_TIME), ('test2', TEST_TIME), ('test3', TEST_TIME)] == \
               await client.zrange(POOL_NAME, 0, 5, withscores=True)
        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_add_items():
    client, pool_instance = await init_test()
    with patch('aiopyrq.pools.time.time') as time_mock, patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        time_mock.return_value = TEST_TIME

        await pool_instance.add_items(['test1', 'test2', 'test3', 'test2'])

        assert [('test1', TEST_TIME), ('test2', TEST_TIME), ('test3', TEST_TIME)] == \
               await client.zrange(POOL_NAME, 0, 5, withscores=True)

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_get_items():
    client, pool_instance = await init_test()
    await _load_items_to_pool(client, 'a', 'b', 'c')

    assert ['a', 'b'] == await pool_instance.get_items(2)
    assert ['c'] == await pool_instance.get_items(2)
    assert [] == await pool_instance.get_items(2)

    assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_get_all_items():
    client, pool_instance = await init_test()
    items = ['a', 'b', 'c']
    await _load_items_to_pool(client, *items)

    assert items == await pool_instance.get_all_items()

    assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_ack_item():
    client, pool_instance = await init_test()
    with patch('aiopyrq.pools.time.time') as time_mock, patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        time_mock.return_value = TEST_TIME
        await _load_test_data_to_pool(client)

        await pool_instance.ack_item('a')
        await pool_instance.ack_item('c')
        await pool_instance.ack_item('b')

        assert 5 == await client.zcard(POOL_NAME)
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'a'))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'b'))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'c'))
        assert TEST_TIME + 600.1 == await client.zscore(POOL_NAME, 'd')
        assert TEST_TIME + 5 == int(await client.zscore(POOL_NAME, 'e'))

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_ack_items():
    client, pool_instance = await init_test()
    with patch('aiopyrq.pools.time.time') as time_mock, patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        time_mock.return_value = TEST_TIME
        await _load_test_data_to_pool(client)

        await pool_instance.ack_items(['a'])
        await pool_instance.ack_items(['c', 'b'])

        assert 5 == await client.zcard(POOL_NAME)
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'a'))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'b'))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 'c'))
        assert TEST_TIME + 600.1 == await client.zscore(POOL_NAME, 'd')
        assert TEST_TIME + 5 == int(await client.zscore(POOL_NAME, 'e'))

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_remove_item():
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        client, pool_instance = await init_test()
        await _load_test_data_to_pool(client)

        await pool_instance.remove_item('a')
        await pool_instance.remove_item('d')
        await pool_instance.remove_item('c')

        assert 2 == await client.zcard(POOL_NAME)
        assert ['e', 'b'] == await client.zrange(POOL_NAME, 0, 5)

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_remove_items():
    with patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        client, pool_instance = await init_test()
        await _load_test_data_to_pool(client)

        await pool_instance.remove_items(['a'])
        await pool_instance.remove_items(['d', 'c'])

        assert 2 == await client.zcard(POOL_NAME)
        assert ['e', 'b'] == await client.zrange(POOL_NAME, 0, 5)

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_clear_pool():
    client, pool_instance = await init_test()
    await _load_test_data_to_pool(client)

    await pool_instance.clear_pool()

    assert [] == await client.keys('*')

    await deactivate_test(client)


@pytest.mark.asyncio
async def test_real_use_case_example():
    client, pool_instance = await init_test()
    with patch('aiopyrq.pools.time.time') as time_mock, patch('aiopyrq.helpers.wait_for_synced_slaves') as slaves_mock:
        time_mock.return_value = TEST_TIME

        test_data = [1, 2, 3, 4, 5, 6, 7]
        await pool_instance.add_items(test_data)

        assert 7 == await client.zcard(POOL_NAME)
        assert [str(item) for item in test_data] == await client.zrange(POOL_NAME, 0, 10)

        await client.zadd(POOL_NAME, TEST_TIME + 5, 7)

        assert ['1', '2', '3'] == await pool_instance.get_items(3)

        await pool_instance.ack_item(1)
        await pool_instance.ack_items([3])

        assert ['4', '5', '6'] == await pool_instance.get_items(3)

        await pool_instance.ack_items([2, 4, 5])

        assert 7 == await client.zcard(POOL_NAME)
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 1))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 2))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 3))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 4))
        assert TEST_TIME + 129600 == int(await client.zscore(POOL_NAME, 5))
        assert TEST_TIME + 600.1 == await client.zscore(POOL_NAME, 6)
        assert TEST_TIME + 5 == int(await client.zscore(POOL_NAME, 7))

        await pool_instance.remove_item(7)

        assert 7 == await client.zcard(POOL_NAME)

        await pool_instance.remove_items([6])

        assert 6 == await client.zcard(POOL_NAME)

        assert [POOL_NAME] == await client.keys('*')

    await deactivate_test(client)


async def _load_test_data_to_pool(client):
    prepared_items = [
        TEST_TIME - 10 + 600.1, 'a',
        TEST_TIME - 5 + 600.1, 'b',
        TEST_TIME - 2 + 600.1, 'c',
        TEST_TIME + 600.1, 'd',
        TEST_TIME + 5, 'e'
    ]
    await client.zadd(POOL_NAME, *prepared_items)


async def _load_items_to_pool(client, *args):
    timestamp = int(time.time())
    prepared_items = []
    for item in args:
        prepared_items.append(timestamp)
        prepared_items.append(item)
    await client.zadd(POOL_NAME, *prepared_items)


if __name__ == 'main':
    pytest.main()

