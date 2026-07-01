# Extension Guide

This guide explains how to extend `plaita` by creating custom nodes.

## Creating a Custom Node

To create a new node type, inherit from the `Node` base class and implement the `execute` method.

### 1. Define the Node Class

```python
from plaita import Node, types

class MyCustomNode(Node):
    # Specific type identifier for the node
    node_type = "my_custom_node"
    
    # Define input schema (optional but recommended)
    input_schema = {
        "text": types.String
    }

    def run(self, execution):
        """
        Execute the node logic.
        
        Args:
            execution: The FlowExecution instance, giving access to context.
        """
        # 1. Evaluate inputs
        # Assuming the node configuration has a 'text' field which might be an expression
        text_value = execution.evaluate(self.config.get('text'))
        
        # 2. Perform business logic
        result = f"Processed: {text_value}"
        
        # 3. Return result
        return result
```

**Note**: The current `Node` implementation in `plaita` might require specific attribute definitions. Ensure you look at `plaita/node/basic.py` for the exact signature of the base `Node` class.

### 2. Register the Node

Before executing a flow that uses your custom node, you must register it.

```python
from plaita import node_register

node_register(MyCustomNode)
```

### 3. Use in Flow

Now you can use your custom node in the JSON definition.

```json
{
    "nodes": [
        {
            "id": "node1",
            "type": "my_custom_node",
            "config": {
                "text": "Hello Custom Node"
            },
            "next": "end"
        },
        ...
    ]
}
```

## Plugin System

`plaita` automatically bundles standard nodes. For domain-specific logic, it is recommended to package your nodes as a separate Python library and register them at application startup.

### Distributing Nodes

Use the `plaita dist-node` command (if configured) or standard PyPI distribution methods to share your custom node libraries.
