import unittest

from plaita.core.flow import Flow


class MyTestCase(unittest.TestCase):

    def test_from_json_file(self):
        with open("tests/fixture/assigment.json", "r") as f:
            content = f.read()
            flow = Flow.from_string(content)
            rs = flow.run({"bb": "123456"})
            print(rs)
            self.assertEqual(rs["dd"], "123456")


if __name__ == "__main__":
    unittest.main()
