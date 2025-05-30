# WiredTiger MCP Server

A Model Context Protocol (MCP) server that provides tools to interact with WiredTiger databases. This server exposes WiredTiger functionality through MCP tools, allowing easy analysis and interaction with WiredTiger files and databases.

## Overview

The WiredTiger MCP Server is designed to provide capabilities for analysing, inspecting, and interacting with WiredTiger databases through a standardised protocol. This enables integration with AI assistants, agents and other tools that support the Model Context Protocol.

### What is MCP?

The Model Context Protocol (MCP) is a standardised way for AI assistants to interact with external tools, resources, and environments. It enables AI models to:

- Execute code in various environments
- Access and manipulate files
- Interact with databases and APIs
- Retrieve relevant information from large codebases

For more information, visit [modelcontextprotocol.io](https://modelcontextprotocol.io).

## Installation

### Prerequisites

- Python 3.13
- uv (a fast Python package installer and resolver from Astral - see [astral.sh/uv](https://astral.sh/uv) for installation instructions)
- WiredTiger Python API
- MCP Python SDK

### Setup

0. Install `uv` if you haven't already. `uv` is a fast Python package installer.
   You can install it using pip:
   ```bash
   pip install uv
   ```
   Or using the standalone installer:
   ```bash
   # On macOS and Linux.
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   Alternatively, follow the official installation instructions at [astral.sh/uv](https://astral.sh/uv).

1. Create and activate a virtual environment using `uv` (recommended):
   ```bash
   uv venv .venv
   source .venv/bin/activate
   ```

2. Sync the project dependencies:
   ```bash
   uv sync
   ```

3. Ensure the WiredTiger Python API is available:
   - Set the `WT_BUILDDIR` environment variable to the path of your WiredTiger build directory.
   - To maintain a persistent environment variable available to the script, create a `.env` file in the mcp-server root with the following content:
     ```bash
     WT_BUILDDIR=/path/to/wiredtiger/build
     ```

## Usage

### Running the Server in VS Code
Create a `mcp.json` file in the `.vscode` directory with the following content:

```json
{
    "servers": {
        "wt-mcp": {
            "type": "stdio",
            "command": "path/to/uv",
            "args": [
                "--directory",
                "path/to/wiredtiger/tools/wt-mcp",
                "run", 
                "server.py"
            ],
        }
    }
}
```

See the [MCP servers in VS Code](https://code.visualstudio.com/docs/copilot/chat/mcp-servers) documentation for more details.

### Starting the Server

To start the server with the MCP Inspector tool (recommended for testing):

```bash
npx @modelcontextprotocol/inspector uv --directory ~/wiredtiger/tools/wt-mcp run server.py
```

Or run it directly:

```bash
uv run server.py
```

### Testing with the Inspector

The MCP Inspector provides a convenient way to test your tools without needing to integrate with an AI assistant:

1. Install the Inspector:
   ```bash
   npm install -g @modelcontextprotocol/inspector
   ```

2. Start the Inspector with your server:
   ```bash
   npx @modelcontextprotocol/inspector uv --directory ~/wiredtiger/tools/wt-mcp run server.py
   ```

3. The Inspector will open a web interface (typically at http://localhost:3000) where you can:
   - View available tools and their documentation
   - Test tools with custom inputs
   - See tool execution results
   - Review server logs and debugging information

## Available Tools

The server provides the following tools for interacting with WiredTiger databases:

### WiredTiger API Operations
- **list_files**: List all files in a WiredTiger home directory
- **get_file_metadata**: Retrieve metadata for a specific WiredTiger file
- **get_file_statistics**: Get statistics for a specific WiredTiger file
- **get_connection_statistics**: Retrieve connection-level statistics for a WiredTiger database
- **run_compact_dryrun**: Estimate space savings from compact operation without modifying data
- **get_key_value**: Look up the value for a specific key in a table or file


### WT Verify tools
- **get_btree_shape**: Analyse the structure of a B-tree
- **get_btree_layout**: Get detailed layout information for a B-tree and checkpoint information

### Tool Scripts
- **decode_wt_binary_file**: Decode a single block in a WiredTiger binary file
- **checkpoint_decode**: Decode a checkpoint address to retrieve offset, size, and checksum information

## Debugging and Logging

The server provides detailed logging to help with debugging and understanding the operations being performed. You can adjust the log level using the `--log-level` command-line argument.

## Extending the Server

### Adding New Tools

To add new tools to the server:

1. Define a new async function with the `@mcp.tool()` decorator
2. Implement the tool's functionality
3. Document the tool with a clear docstring explaining when to use it
4. Return results in the proper MCP format

Example:

```python
@mcp.tool()
async def my_new_tool(
    ctx: Context, 
    param1: str = Field(description="Description of param1"),
    param2: Optional[str] = None
    ) -> Dict:
    """
    Description of what my_new_tool does.

    When to use this tool:
        - Use case 1
        - Use case 2
    """
    # Tool implementation
    # ...
    
    return {
        "content": [{
            "type": "text",
            "text": "Result of my_new_tool"
        }]
    }
```

### Best Practices for Tool Development

1. **Proper Error Handling**: Always use try/except blocks and clean up resources in finally blocks
2. **Context Logging**: Use `ctx.info()`, `ctx.debug()`, and `ctx.error()` for effective logging
3. **Clear Documentation**: Always include:
   - A concise description of what the tool does
   - "When to use this tool" section with specific use cases
   - Parameter descriptions that explain the expected input
4. **Structured Output**: Return results in the proper MCP format for optimal display to users:
   ```python
   return {
       "content": [{
           "type": "text",  # Can also be "image", "html", etc.
           "text": "Your output here"
       }]
   }
   ```

### Configuration Options

The FastMCP server supports various configuration options:

```python
mcp = FastMCP(
    name="WiredTiger Tools",  # Server name shown to users
    log_level="DEBUG",        # Log verbosity (DEBUG, INFO, WARNING, ERROR)
    debug=True                # Enable detailed debug logging
)
```

See the [MCP Python SDK Documentation](https://github.com/modelcontextprotocol/python-sdk) for more advanced options.

## Related Tools

### WiredTiger Tools

The server leverages other WiredTiger tools such as:

- `wt_binary_decode.py`: For decoding WiredTiger binary files
- `wt_ckpt_decode.py`: For decoding checkpoint addresses

These tools should be available in the Python path for the full functionality of the MCP server.

## Additional Resources

- [MCP Python SDK Documentation](https://github.com/modelcontextprotocol/python-sdk)
- [Model Context Protocol Specification](https://modelcontextprotocol.io)
- [MCP Quickstart Guide](https://modelcontextprotocol.io/quickstart/server)
