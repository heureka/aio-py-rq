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
