#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

"""
WiredTiger MCP Server

This is a Model Context Protocol (MCP) server that provides tools to interact with
WiredTiger databases, including file metadata retrieval, statistics, and data operations.
"""
import argparse
from contextlib import redirect_stdout, redirect_stderr
from dotenv import load_dotenv
import io
import json
import os
from pydantic import Field
import sys
from typing import Dict, Optional

# Import MCP server library
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.utilities.logging import configure_logging, get_logger
    
load_dotenv()
build_dir = os.getenv("WT_BUILDDIR")
if not build_dir:
    print("Error: WT_BUILDDIR environment variable not set. Please set it to the WiredTiger build directory.")
    sys.exit(1)
    
# Add the build directory to sys.path
sys.path.insert(0, build_dir)
# Add the lang/python directory to sys.path
lang_python_dir = os.path.join(build_dir, "lang", "python")
sys.path.insert(0, lang_python_dir)
# Add the tools directory to sys.path
tools_dir = os.path.join(build_dir, "..", "tools")
sys.path.insert(0, tools_dir)

try:
    # Import WiredTiger Python API
    import wiredtiger
except ImportError:
    print("Error: WiredTiger Python API not found. Please ensure it is built and available in the Python path.")
    sys.exit(1)

# Create a logger for this module
logger = get_logger(__name__)

# Create an MCP server with logging configuration
mcp = FastMCP(
    name="WiredTiger Tools",
    log_level="INFO",
    debug=True          # Enable debug mode
)

# Argument descriptions
URI_DESCRIPTION = """
The URI of the file or table. Must have a prefix of "file:".
"""
HOME_DESCRIPTION = """
The WiredTiger home directory. This is the directory where the WiredTiger database files are stored.
"""

@mcp.tool()
async def list_files(
    ctx: Context, 
    home: str = Field(description=HOME_DESCRIPTION), 
    config: Optional[str] = None
    ) -> Dict:
    """
    List files in a WiredTiger home directory.

    When to use this tool:
        - You want to see all the WiredTiger files in a specific home directory.
        - You need to know the names of the files to perform further operations on them.

    After calling this tool, you can use the get-file-metadata tool to get metadata for a specific file.
    """
    conn = None
    
    try:
        await ctx.info(f"Listing files in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")
            
        # Open a WiredTiger metadata cursor.
        conn = wiredtiger.wiredtiger_open(home, config or "")
        session = conn.open_session()
        cursor = session.open_cursor("metadata:")

        # Iterate over the cursor and collect file names
        files = []
        while cursor.next() == 0:
            key = cursor.get_key()
            files.append(key)
            await ctx.debug(f"Found file: {key}")
        
        await ctx.info(f"Found {len(files)} files in WiredTiger directory")
        
        return {
            "content": [{
                "type": "text",
                "text": "\n".join(files)
            }]
        }
    
    except Exception as e:
        await ctx.error(f"Error listing files: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error listing files: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def get_file_metadata(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
    ) -> Dict:
    """
    Get metadata for a specific WiredTiger file.
    
    When to use this tool:
        - You want to retrieve metadata for a specific WiredTiger file.
        - You need to know the details of a file, such as its size, type, or other properties.
    """
    conn = None
    
    try:
        await ctx.info(f"Getting metadata for {uri} in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")
            
        # Open a WiredTiger connection
        conn = wiredtiger.wiredtiger_open(home, config or "")
        session = conn.open_session()
        cursor = session.open_cursor("metadata:")

        # Iterate over the cursor and find the specified file
        file_metadata = None
        while cursor.next() == 0:
            key = cursor.get_key()
            value = cursor.get_value()
            
            # Check if the current file matches the requested file
            if key == uri:
                file_metadata = {"key": key, "value": value}
                break
        
        # If a matching file was found, return its metadata
        if file_metadata:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps(file_metadata, indent=2)
                }]
            }
        
        # If the file was not found, return an error message
        await ctx.warning(f"File not found in metadata: {uri}")
        return {
            "content": [{
                "type": "text",
                "text": f"File not found in metadata: {uri}"
            }]
        }
    
    except Exception as e:
        await ctx.error(f"Error retrieving metadata for file: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving metadata for file: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def get_file_statistics(
    ctx: Context, 
    uri: str = Field(description=URI_DESCRIPTION), 
    home: str = Field(description=HOME_DESCRIPTION), 
    config: Optional[str] = None
    ) -> Dict:
    """
    Get statistics for a specific WiredTiger file.

    When to use this tool:
        - You want to analyze performance metrics for a specific WiredTiger file
        - You need to gather statistics about file usage, cache behavior, or other performance indicators
    """
    conn = None
    
    try:
        await ctx.info(f"Getting statistics for {uri} in {home}")
        if config:
            await ctx.info(f"Using configuration: {config}")
            
        # Open a WiredTiger connection with statistics enabled
        conn_config = config or "statistics=(all)"
        if "statistics" not in conn_config:
            conn_config += ",statistics=(all)"
        
        conn = wiredtiger.wiredtiger_open(home, conn_config)
        session = conn.open_session()
        stat_cursor = session.open_cursor(f"statistics:{uri}", None, None)
        
        # Collect statistics
        stats = []
        while stat_cursor.next() == 0:
            desc = stat_cursor.get_key()
            value = stat_cursor.get_value()
            if isinstance(value, tuple):
                value_str = str(value[0])
            else:
                value_str = str(value)
            
            stats.append({
                "description": desc,
                "value": value,
                "printableValue": value_str
            })
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(stats, indent=2)
            }]
        }
    
    except Exception as e:
        await ctx.error(f"Error retrieving statistics for file {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving statistics for file {uri}: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def get_connection_statistics(
    ctx: Context, 
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
    ) -> Dict:
    """
    Get connection-level statistics for a WiredTiger database.

    When to use this tool:
        - You want to analyse overall database performance metrics
        - You need to gather global statistics about cache usage, connections, and system behavior
    """
    conn = None
    
    try:
        await ctx.info(f"Getting connection statistics for {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")
        # Open a WiredTiger connection with statistics enabled
        conn_config = "statistics=(all)"
        if config:
            conn_config = config
        
        conn = wiredtiger.wiredtiger_open(home, conn_config)
        session = conn.open_session()
        stat_cursor = session.open_cursor("statistics:", None, None)
        
        # Collect statistics
        stats = []
        while stat_cursor.next() == 0:
            desc = stat_cursor.get_key()
            value = stat_cursor.get_value()
            if isinstance(value, tuple):
                value_str = str(value[0])
            else:
                value_str = str(value)
            
            stats.append({
                "description": desc,
                "value": value,
                "printableValue": value_str
            })
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(stats, indent=2)
            }]
        }
    
    except Exception as e:
        await ctx.error(f"Error retrieving connection statistics: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving connection statistics: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def run_compact_dryrun(
    ctx: Context, 
    uri: str = Field(description=URI_DESCRIPTION), 
    home: str = Field(description=HOME_DESCRIPTION), 
    config: Optional[str] = None
    ) -> Dict:
    """
    Run compact in dryrun mode on a WiredTiger file or table.

    When to use this tool:
        - You want to estimate how much space could be reclaimed by compaction
        - You need to evaluate whether compaction would be beneficial without actually modifying data
        - You're analysing file fragmentation
    """
    conn = None
    
    try:
        await ctx.info(f"Running compact dryrun for {uri} in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")
        
        # Set up redirection to capture stdout and stderr

        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()
        
        # Use redirection context managers to capture output
        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            # Open a WiredTiger connection
            conn = wiredtiger.wiredtiger_open(home, config or "verbose=[compact:2]")
            session = conn.open_session()
            
            compact_config = "dryrun=true"
            session.compact(uri, compact_config)
            await ctx.info(f"Compact dryrun completed for {uri}")
            
            return {
                "content": [{
                    "type": "text",
                    "text": f"Compact dryrun completed successfully for {uri}\nLogs:\n{captured_stdout.getvalue()}\nErrors:\n{captured_stderr.getvalue()}"
                }]
            }
    
    except Exception as e:
        await ctx.error(f"Error running compact dryrun for {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error running compact dryrun for {uri}: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def get_key_value(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    key: str = Field(description="The key to look up in the table or file"),
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
) -> Dict:
    """
    Retrieve the value for a specific key from a WiredTiger table or file.

    When to use this tool:
        - You need to retrieve a specific value for a known key
        - You want to check if a key exists in a table or file
        - You need to validate data integrity for specific records
    """
    conn = None
    
    try:
        await ctx.info(f"Retrieving value for key '{key}' from {uri} in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")
        
        # Open a WiredTiger connection
        conn = wiredtiger.wiredtiger_open(home, config or "")
        session = conn.open_session()
        
        metadata_cursor = session.open_cursor("metadata:")
        metadata_cursor.set_key(uri)
        
        if metadata_cursor.search() == 0:
            metadata_value = metadata_cursor.get_value()
            
            # Extract format information from the metadata
            schema = {}
            metadata_parts = metadata_value.split(",")
            for part in metadata_parts:
                if "=" in part:
                    meta_key, meta_value = part.split("=", 1)
                    if meta_key.strip() in ["key_format", "value_format"]:
                        schema[meta_key.strip()] = meta_value.strip()
                        await ctx.debug(f"Found format information: {meta_key.strip()}={meta_value.strip()}")
            
            if len(schema) != 2:
                error_msg = f"Could not retrieve key_format and value_format from metadata for {uri}"
                await ctx.error(error_msg)
                raise ValueError(error_msg)
        else:
            error_msg = f"URI {uri} not found in metadata"
            await ctx.error(error_msg)
            raise ValueError(error_msg)

        await ctx.info(f"Schema for {uri}: {schema}")

        # Close the metadata cursor as we're done with it
        metadata_cursor.close()
        
        # Convert the key string to the appropriate data type
        converted_key = key
        
        key_format = schema.get("key_format")
        if len(key_format) == 1:
            format_char = key_format[0]
            
            # Convert based on the format character
            if format_char in 'bBhHiIlLqQrt':  # Integer types
                converted_key = int(key)
                await ctx.debug(f"Converted key from string '{key}' to integer {converted_key}")
            elif format_char == 'u':  # Raw byte array
                converted_key = bytes(key, 'utf-8')
                await ctx.debug(f"Converted key from string '{key}' to bytes")
            elif format_char in 'sS':  # String types - already a string, no conversion needed
                await ctx.debug(f"Key format is string, no conversion needed")
            else:
                await ctx.warning(f"Unsupported key format '{format_char}', using string representation")
        else:
            # For composite keys or complex formats, provide a warning and use as is
            await ctx.warning(f"Complex key format '{key_format}' not handled, using key as provided")
        
        # Open a cursor for the URI
        cursor = session.open_cursor(uri, None, None)
        
        # Look up the key
        cursor.set_key(converted_key)
        ret = cursor.search()
        
        if ret == 0:
            # Key found, get the value
            value = cursor.get_value()
            
            # Return the key-value pair
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({"key": key, "value": value}, indent=2)
                }]
            }
        else:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Key not found: {key} in {uri}"
                }]
            }
    
    except Exception as e:
        await ctx.error(f"Error retrieving key-value pair: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving key-value pair: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()

@mcp.tool()
async def get_btree_shape(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
) -> Dict:
    """
    Get the B-tree shape for a specific WiredTiger table or file.

    When to use this tool:
        - You want to analyze the structure of a specific B-tree
        - You need to understand how data is organized within a table or file
    """
    conn = None
    
    try:
        if config:
            await ctx.debug(f"Using configuration: {config}")
        
        # Set up redirection to capture stdout and stderr
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()
        
        # Use redirection context managers to capture output
        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            
            # Open a WiredTiger connection
            conn = wiredtiger.wiredtiger_open(home, config or "")
            session = conn.open_session()
            
            # Get the B-tree layout for the specified URI
            session.verify(uri, 'dump_tree_shape=true')
            
            # Return the B-tree layout
            return {
                "content": [{
                    "type": "text",
                    "text": f"B-tree shape for {uri}:\n{captured_stdout.getvalue()}\nErrors:\n{captured_stderr.getvalue()}"
                }]
            }
    
    except Exception as e:
        await ctx.error(f"Error retrieving B-tree shape for {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving B-tree shape for {uri}: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()
            
@mcp.tool()
async def get_btree_layout(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
) -> Dict:
    """
    Get the B-tree layout for a specific WiredTiger table or file.

    When to use this tool:
        - You want to analyse the structure of a specific B-tree
        - You need to understand how data is organized within a table or file
        - You want to know information about the latest checkpoint
    """
    conn = None
    
    try:
        if config:
            await ctx.debug(f"Using configuration: {config}")
        
        # Set up redirection to capture stdout and stderr
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()
        
        # Use redirection context managers to capture output
        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            
            # Open a WiredTiger connection
            conn = wiredtiger.wiredtiger_open(home, config or "")
            session = conn.open_session()
            
            # Get the B-tree layout for the specified URI
            session.verify(uri, 'dump_layout=true')
            
            # Return the B-tree layout
            return {
                "content": [{
                    "type": "text",
                    "text": f"B-tree layout for {uri}:\n{captured_stdout.getvalue()}\nErrors:\n{captured_stderr.getvalue()}"
                }]
            }
    
    except Exception as e:
        await ctx.error(f"Error retrieving B-tree layout for {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving B-tree layout for {uri}: {str(e)}"
            }]
        }
    
    finally:
        if conn:
            conn.close()
            
@mcp.tool()
async def decode_wt_binary_file(
    ctx: Context, 
    file_path: str = Field(description="The absolute path to the wiredtiger data file."), 
    offset: int = Field(description="The offset in the file to start decoding from.")
) -> Dict:
    """
    Decodes a single page in a WiredTiger binary file.

    When to use this tool:
        - You need to gather page header information about a specific block in a WiredTiger file.
        - You want to decode a specific page starting at a known offset.
    """
    if not file_path:
        errmsg = "File path provided to decode_wt_binary_file is missing or empty."
        await ctx.error(errmsg)
        return {
            "content": [{
                "type": "text",
                "text": f"Error: {errmsg}"
            }]
        }

    try:
        from wt_binary_decode import wtdecode

        # Simulate argparse options for wtdecode
        opts = argparse.Namespace(
            filename=file_path,
            offset=offset,
            pages=1,            # Only decode one page
            ext=True,           # Dump the extent lists
            fragment=True,
            verbose=True,
            bytes=False,
            bson=False,
            output=None,
            cont=False,
            debug=False,
            dumpin=False,
            split=False,
            skip_data=False,
            version=False
        )

        # Capture stdout and stderr from the wtdecode function
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()

        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            wtdecode(opts)

        output = captured_stdout.getvalue()
        errors = captured_stderr.getvalue()

        if errors:
            await ctx.warning(f"Errors/warnings from wt_binary_decode: {errors}")

        return {
            "content": [{
                "type": "text",
                "text": f"Binary decode results for {file_path} at offset {offset}:\n\n{output}\n\nErrors/Warnings:\n{errors}"
            }]
        }
        
    except ImportError as ie:
        await ctx.error(f"Import error during binary decode: {str(ie)}")
        await ctx.error("Make sure wt_binary_decode.py is in the Python path")
        
        return {
            "content": [{
                "type": "text", 
                "text": f"Error importing wt_binary_decode module: {str(ie)}\nMake sure wt_binary_decode.py is in the Python path."
            }]
        }
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"An unexpected error occurred during binary decode: {str(e)}"
            }]
        }

@mcp.tool()
async def checkpoint_decode(
    ctx: Context, 
    addr: str = Field(description="The address to decode"), 
    allocsize: Optional[int] = Field(description="The allocation size (default is 4096)", default=4096)
) -> Dict:
    """
    Decode a checkpoint address.
    
    When to use this tool:
        - You want to decode a checkpoint address to find the offset, size and checksum of the 
            checkpoint blocks. Checkpoint blocks include the root page block, and the allocated, available 
            and discard extent list blocks.
    """
    try:                        
        # Import the decode_arg function
        from wt_ckpt_decode import decode_arg
        
        # Capture the output of decode_arg since it prints to stdout
        captured_output = io.StringIO()
        
        with redirect_stdout(captured_output):
            # Decode the address
            decode_arg(addr, allocsize)
        
        output = captured_output.getvalue()
        
        # Return the captured output
        return {
            "content": [{
                "type": "text",
                "text": f"Checkpoint address decode results:\n\n{output}"
            }]
        }
    
    except ImportError as ie:
        await ctx.error(f"Import error during checkpoint decode: {str(ie)}")
        await ctx.error("Make sure wt_ckpt_decode.py is in the Python path")
        
        return {
            "content": [{
                "type": "text", 
                "text": f"Error importing wt_ckpt_decode module: {str(ie)}\nMake sure wt_ckpt_decode.py is in the Python path."
            }]
        }
    except Exception as e:
        # Log detailed error information
        await ctx.error(f"Error decoding checkpoint address: {str(e)}")
        import traceback
        tb = traceback.format_exc()
        await ctx.error(f"Traceback: {tb}")
        
        # Return error message to the client
        return {
            "content": [{
                "type": "text",
                "text": f"Error decoding checkpoint address: {str(e)}"
            }]
        }

if __name__ == "__main__":
    # Create argument parser for logging configuration
    parser = argparse.ArgumentParser(description="WiredTiger MCP Server")
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="DEBUG",
        help="Set the logging level (default: INFO)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true",
        default=True,
        help="Enable debug mode"
    )
    args = parser.parse_args()
    
    # Configure logging based on command-line arguments
    configure_logging(level=args.log_level)
    
    # Log startup information
    logger.info(f"Starting WiredTiger MCP Server with log level: {args.log_level}")
    
    if args.debug:
        logger.info("Debug mode enabled")
    
    # Run the server with the specified transport
    mcp.run(transport="stdio")
