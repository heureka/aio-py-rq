async def wait_for_synced_slaves(redis, count: int, timeout: int):
    synced = await redis.execute('WAIT', count, timeout)
    if synced < count:
        raise NotEnoughSyncedSlavesError('There are only {} synced slaves. Required {}'.format(synced, count))


def create_chunks(items, chunk_size):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


async def async_iterate_to_list(cmd) -> list:
    lst = []
    async for i in cmd:
        lst.append(i)
    return lst


class NotEnoughSyncedSlavesError(Exception):
    pass

