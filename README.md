# The Asynchronous python library  **aio-py-rq**

This library is set of asynchronous Python and Lua scripts which enables you to easily implement queuing system based on Redis.
All the queues works well in multi-threaded environment. The only thing you have to keep in mind is that with multiple consumers the order of the items is impossible to preserve.
E.g. if multiple consumers exits unexpectedly and then you use re-enqueue method to get the items back to the queue then you will most probably lose the order of the items.
If you want to rely on the order of the items then you are required to use only one consumer at a time, reject whole batch after failure and re-enqueue everything before getting another chunk of items.

## Data structures:
 - Queue
 - Pool
 - Unique queue

## Requirements
 - Python 3.7+
 - Redis
 
## How to install over Pip/Pypi
```bash
$ pip install aio-py-rq
```

## How to install over Poetry
```bash
$ poetry add aio-py-rq
```
 
## Requirements for local tests
 - Python 3.7+
 - Redis
 - py.test
 
## Docker building
Just use `make`.

## Docker tests
Just use `make test`.

## Basic usage
### Queue
Use `from aiopyrq import Queue`.

If you need slave synchronization, use *synced_slaves_count* and *synced_slaves_timeout* arguments.

This data structure serves for managing queues of **serializable** items. Typical data flow consist of several phases:
 1. Adding item (via `add_item(s)`)
 2. Getting item (via `get_items`)
 3. Acknowledging item (via `ack_item(s)`) when item was successfully processed **OR** rejecting item (via `reject_item(s)`) when error occurs.

**BEWARE!**. You must either acknowledge item or reject item. If you fail to do this, you have to clean internal processing queues created by **py-RQ**.

#### Example

```python
import asyncio
import aioredis
from aiopyrq import Queue

async def go():
    redis_connection_pool = await aioredis.create_redis_pool('redis://localhost')
    queue = Queue(QUEUE_NAME, redis_connection_pool, synced_slaves_enabled=True, synced_slaves_count=COUNT_OF_SLAVES, synced_slaves_timeout=TIMEOUT)
    
    queue.add_item(value) # adding item
    
    list_of_values = queue.get_items(10) # getting items
    queue.ack_items(list_of_values) # acknowledging items or
    queue.revert_items(list_of_values) # reverting items to the queue

asyncio.run(go())
```

### UniqueQueue
Use `from aiopyrq import UniqueQueue`.

If you need slave synchronization, use *synced_slaves_count* and *synced_slaves_timeout* arguments.

This data structure serves for managing queues of **serializable** items. Typical data flow consist of several phases:
 1. Adding item (via `add_item(s)`)
 2. Getting item (via `get_items`)
 3. Acknowledging item (via `ack_item(s)`) when item was successfully processed **OR** rejecting item (via `reject_item(s)`) when error occurs.

**BEWARE!**. You must either acknowledge item or reject item. If you fail to do this, you have to clean internal processing queues created by **py-RQ**.

#### Example

```python
import asyncio
import aioredis
from aiopyrq import UniqueQueue

async def go():

    redis_connection_pool = await aioredis.create_redis_pool('redis://localhost')
    queue = UniqueQueue(QUEUE_NAME, redis_connection_pool, synced_slaves_enabled=True, synced_slaves_count=COUNT_OF_SLAVES, synced_slaves_timeout=TIMEOUT)
    
    queue.add_item(value) # adding item
    
    list_of_values = queue.get_items(10) # getting items
    queue.ack_items(list_of_values) # acknowledging items or
    queue.revert_items(list_of_values) # reverting items to the queue

asyncio.run(go())
```

### Pool
Use `from aiopyrq import Pool`.

If you need slave synchronization, use *synced_slaves_count* and *synced_slaves_timeout* arguments.

This data structure is designed to manage an infinite ordered set of **serializable** items. Typical data flow consist of several phases:
 1. Adding item (via `add_item(s)`)
 2. Getting item (via `get_items`)
 3. Acknowledging item (via `ack_item(s)`) when item was successfully processed.
 4. Removing item (via `remove_item(s)`) when you want to delete item from the pool

#### Example

```python
import asyncio
import aioredis
from aiopyrq import Pool

async def go():

    redis_connection_pool = await aioredis.create_redis_pool('redis://localhost')
    queue = Pool(POOL_NAME, self.client, synced_slaves_enabled=True, synced_slaves_count=COUNT_OF_SLAVES, synced_slaves_timeout=TIMEOUT)

    queue.add_item(value) # adding item
    
    list_of_values = queue.get_items(10) # getting items
    queue.ack_items(list_of_values) # acknowledging items
    queue.remove_items(list_of_values) # removing items from the pool

asyncio.run(go())
```
