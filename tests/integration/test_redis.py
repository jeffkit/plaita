import unittest

from fakeredis import FakeStrictRedis

from plaita.core.flow import Flow
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.redis import RedisNode

TARGET = "redis://user:123qwe1@127.0.0.1:6379/0"

redis_client = FakeStrictRedis()


class RedisNodeTestCase(unittest.TestCase):
    def create_flow(self):
        flow = Flow(
            flow_id="redis-get",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "target": Property(data_type=types.STRING, is_required=True),
                    "command": Property(data_type=types.STRING, is_required=True),
                    "arguments": Property(data_type=types.STRING, is_required=True),
                },
            ),
            output_type=Property(data_type=types.STRING, is_required=True),
        )
        nodes = [
            Start(id="start", next="redis-get"),
            RedisNode(
                id="redis-get",
                flow=flow,
                target="$INPUT.target",
                command="$INPUT.command",
                arguments="$INPUT.arguments",
                redis_client=redis_client,
                next="end",
            ),
            End(id="end", flow=flow, **{"resultType": "success", "output": "$NODE.redis-get"}),
        ]
        flow.nodes = nodes
        return flow

    def test_set(self):
        self.assertEqual(
            True,
            self.create_flow().run(
                target=TARGET,
                command="set",
                arguments="test_key test_value",
            ),
        )

        self.assertEqual(
            b"test_value",
            self.create_flow().run(
                target=TARGET,
                command="get",
                arguments="test_key",
            ),
        )

        self.assertEqual(
            True,
            self.create_flow().run(
                target=TARGET,
                command="mset",
                arguments=" test_key   test_value   test_key1  test_value1 ",
            ),
        )

        self.assertEqual(
            [b"test_value", b"test_value1"],
            self.create_flow().run(
                target=TARGET,
                command="mget",
                arguments="test_key test_key1",
            ),
        )

        self.assertEqual(
            True,
            self.create_flow().run(
                target=TARGET,
                command="del",
                arguments="test_key",
            ),
        )

        self.assertEqual(
            None,
            self.create_flow().run(
                target=TARGET,
                command="get",
                arguments="test_key",
            ),
        )


if __name__ == "__main__":
    unittest.main()
