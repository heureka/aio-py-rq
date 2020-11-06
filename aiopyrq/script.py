from aioredis.commands import Redis, Pipeline
from aioredis.connection import RedisConnection
from aioredis.pool import ConnectionsPool

from typing import Union


class Script(object):
    """An executable Lua script object returned by ``register_script``"""

    def __init__(self, registered_client: Union[ConnectionsPool, RedisConnection, Redis], script: str):
        self.registered_client = registered_client
        self.script = script

    async def __call__(self, keys: list=list(), args: list=list(),
                       client: Union[Pipeline, ConnectionsPool, RedisConnection, Redis]=None):
        """Execute the script, passing any required ``args``"""
        if client is None:
            client = self.registered_client
        if isinstance(client, (RedisConnection, ConnectionsPool)):
            args = tuple(keys) + tuple(args)
            return await client.execute('eval', self.script, len(keys), *args)
        if isinstance(client, Pipeline):
            return client.eval(self.script, keys=keys, args=args)
        else:
            return await client.eval(self.script, keys=keys, args=args)
