# Usage Guide

This guide covers how to install, accept, and run flows using `plaita`.

## Installation

### From PyPI

```bash
pip install plaita
```

### From Source

```bash
git clone <repository_url>
cd plaita
python3 setup.py install
```

## Defining a Flow

Flows are defined in JSON format. A basic flow consists of `input`, `output`, `nodes`, and `links` (though `links` are often implicit in `next` pointers within nodes).

### Example: Echo Flow

```json
{
    "id": "echo_flow",
    "input": {
        "name": "name",
        "type": "string"
    },
    "nodes": [
        {
            "id": "start",
            "type": "start",
            "next": "end"
        },
        {
            "id": "end",
            "type": "end",
            "response": {
                "type": "success",
                "value": "${INPUT.name}"
            }
        }
    ]
}
```

## Running a Flow

### Local Execution

You can load a JSON file and execute it directly in your Python code.

```python
import json
from plaita import Flow

# Load flow definition
with open('echo.json', 'r') as f:
    flow_dict = json.load(f)

# Initialize Flow object
flow = Flow.parse_flow(flow_dict)
flow_obj = Flow.model_validate(flow)

# Run the flow
result = flow_obj.run(name="World")
print(result)
# Output: World
```

### Using Plaita Client (Remote)

If you are running your own Plaita server, you can execute flows remotely. The `url` argument
defaults to the `/api/flowVersion/semver/detail` contract endpoint served by this repo's
`plaita-console` (`http://localhost:8080/api/flowVersion/semver/detail` for a local deploy);
in production, pass `url` to point at your deployed console.

```python
from plaita.client import PlaitaClient

# Local console deploy: url can be omitted (defaults to the console contract endpoint)
client = PlaitaClient('your secret id', 'your secret key')

# Or specify a remote address explicitly:
# client = PlaitaClient(
#     'your secret id',
#     'your secret key',
#     url='https://your-plaita-server/api/flowVersion/semver/detail',
# )
# Run flow with ID '259'
result = client.run_flow('259', '0.0.2', {"age": 14})
print(result)
```

## Advanced Features

### Timeout Control

You can set a timeout for the entire flow or individual nodes.

```json
{
    "id": "my_flow",
    "timeout": "PT5S", // 5 seconds
    ...
}
```

### Debugging

Use the `debug` method to run the flow in debug mode (if supported by your setup), or simply set up logging to view execution traces.

```python
import logging
logging.basicConfig(level=logging.INFO)
# Logs will show [node start] and [node end] events
```
