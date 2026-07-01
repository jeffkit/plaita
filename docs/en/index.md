# plaita Documentation

Welcome to the technical documentation for `plaita`, the Python runtime for the Plaita logic orchestration system.

## Table of Contents

-   **[Architecture](architecture.md)**: Deep dive into the system design, core components, and execution flow. Features detailed diagrams.
-   **[Checkpoint Architecture](checkpoint-architecture.md)**: Detailed design for Checkpoint feature supporting long-running workflow suspend and resume.
-   **[Usage Guide](usage.md)**: Instructions on how to install `plaita`, write flow definitions, and execute them in your applications.
-   **[Extension Guide](extension.md)**: Learn how to extend the system by creating and registering custom nodes.

## Quick Start

```bash
pip install plaita
```

```python
from plaita import Flow

with open('flow.json', 'r') as f:
    flow = Flow.model_validate_json(f.read())
    result = flow.run(name="World")
    print(result)
```

Check the [Usage Guide](usage.md) for more details.

## Core Concepts

| Concept | Description |
|---------|-------------|
| **Flow** | A workflow definition composed of multiple nodes |
| **Node** | The smallest execution unit in a flow |
| **Context** | State storage during flow execution |
| **Expression** | References to context data, e.g., `${INPUT.name}` |

## Execution Modes

`plaita` supports three execution modes:

- **Normal**: Synchronous blocking execution, ideal for quick flows
- **Generator**: Supports step-by-step debugging and state inspection
- **Distributed**: Supports cross-process execution for long-running workflows

See [Architecture - Execution Modes](architecture.md#execution-modes) for details.

