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
from typing import Dict, List, Optional

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

def metadata_search(session, uri):
    """
    Look up a URI in the WiredTiger metadata and return its value.

    Returns the metadata value string if found, or None if the URI is not present.
    """
    cursor = session.open_cursor("metadata:")
    try:
        cursor.set_key(uri)
        if cursor.search() == 0:
            return cursor.get_value()
        return None
    finally:
        cursor.close()

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
    session = None
    cursor = None

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
        if cursor:
            cursor.close()
        if session:
            session.close()
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
    session = None

    try:
        await ctx.info(f"Getting metadata for {uri} in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")

        # Open a WiredTiger connection
        conn = wiredtiger.wiredtiger_open(home, config or "")
        session = conn.open_session()

        value = metadata_search(session, uri)
        if value is not None:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({"key": uri, "value": value}, indent=2)
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
        if session:
            session.close()
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
    session = None

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
        if session:
            session.close()
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
    session = None
    cursor = None

    try:
        await ctx.info(f"Retrieving value for key '{key}' from {uri} in {home}")
        if config:
            await ctx.debug(f"Using configuration: {config}")

        # Open a WiredTiger connection
        conn = wiredtiger.wiredtiger_open(home, config or "")
        session = conn.open_session()

        metadata_value = metadata_search(session, uri)
        if metadata_value is None:
            error_msg = f"URI {uri} not found in metadata"
            await ctx.error(error_msg)
            raise ValueError(error_msg)

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

        await ctx.info(f"Schema for {uri}: {schema}")

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
        if cursor:
            cursor.close()
        if session:
            session.close()
        if conn:
            conn.close()

@mcp.tool()
async def query_timestamps(
    ctx: Context,
    home: str = Field(description=HOME_DESCRIPTION),
    config: Optional[str] = None
) -> Dict:
    """
    Query all global transaction timestamps for a WiredTiger database.

    Returns the key timestamps that govern transaction visibility and durability:
    all_durable, oldest_timestamp, stable_timestamp, oldest_reader, pinned,
    last_checkpoint, and recovery.

    When to use this tool:
        - You need to understand the temporal state of the database
        - You are debugging transaction visibility or timestamp ordering issues
        - You want to check if timestamps have been set correctly
        - You need to verify checkpoint or recovery timestamp state
    """
    conn = None

    try:
        await ctx.info(f"Querying timestamps for {home}")

        conn = wiredtiger.wiredtiger_open(home, config or "")

        timestamp_types = [
            "all_durable",
            "last_checkpoint",
            "oldest_timestamp",
            "oldest_reader",
            "pinned",
            "recovery",
            "stable_timestamp",
        ]

        timestamps = {}
        for ts_type in timestamp_types:
            try:
                value = conn.query_timestamp(f"get={ts_type}")
                timestamps[ts_type] = value
            except wiredtiger.WiredTigerError as e:
                timestamps[ts_type] = f"unavailable ({str(e)})"

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(timestamps, indent=2)
            }]
        }

    except Exception as e:
        await ctx.error(f"Error querying timestamps: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error querying timestamps: {str(e)}"
            }]
        }

    finally:
        if conn:
            conn.close()

@mcp.tool()
async def verify_table(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    home: str = Field(description=HOME_DESCRIPTION),
    dump_address: bool = Field(default=False, description="Display page addresses, time windows, and page types"),
    dump_all_data: bool = Field(default=False, description="Display all application data found during verification"),
    dump_key_data: bool = Field(default=False, description="Display keys found during verification"),
    dump_blocks: bool = Field(default=False, description="Display on-disk block contents"),
    dump_pages: bool = Field(default=False, description="Display in-memory page contents"),
    dump_layout: bool = Field(default=False, description="Display file layout information"),
    dump_tree_shape: bool = Field(default=False, description="Display the B-tree shape"),
    read_corrupt: bool = Field(default=False, description="Continue verification after checksum errors, skipping corrupt blocks"),
    strict: bool = Field(default=False, description="Treat verification warnings as errors"),
    stable_timestamp: bool = Field(default=False, description="Verify no data exists after the stable timestamp"),
    config: Optional[str] = None
) -> Dict:
    """
    Run verify on a WiredTiger table or file with configurable dump options.

    This tool exposes the full range of session.verify() options for detailed
    inspection of on-disk and in-memory data structures.

    When to use this tool:
        - You want to check the integrity of a specific table or file
        - You need to inspect on-disk blocks, pages, or addresses for debugging
        - You are investigating data corruption and want to dump specific data
        - You want to verify no data exists beyond the stable timestamp
    """
    conn = None
    session = None

    try:
        await ctx.info(f"Running verify on {uri} in {home}")

        # Build verify configuration from boolean options
        verify_opts = []
        if dump_address:
            verify_opts.append("dump_address=true")
        if dump_all_data:
            verify_opts.append("dump_all_data=true")
        if dump_key_data:
            verify_opts.append("dump_key_data=true")
        if dump_blocks:
            verify_opts.append("dump_blocks=true")
        if dump_pages:
            verify_opts.append("dump_pages=true")
        if dump_layout:
            verify_opts.append("dump_layout=true")
        if dump_tree_shape:
            verify_opts.append("dump_tree_shape=true")
        if read_corrupt:
            verify_opts.append("read_corrupt=true")
        if strict:
            verify_opts.append("strict=true")
        if stable_timestamp:
            verify_opts.append("stable_timestamp=true")

        verify_config = ",".join(verify_opts) if verify_opts else None
        await ctx.info(f"Verify config: {verify_config}")

        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()

        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            conn = wiredtiger.wiredtiger_open(home, config or "")
            session = conn.open_session()
            session.verify(uri, verify_config)

        stdout_output = captured_stdout.getvalue()
        stderr_output = captured_stderr.getvalue()

        result_text = f"Verify completed for {uri}"
        if verify_config:
            result_text += f" (options: {verify_config})"
        if stdout_output:
            result_text += f"\n\nOutput:\n{stdout_output}"
        if stderr_output:
            result_text += f"\n\nErrors:\n{stderr_output}"
        if not stdout_output and not stderr_output:
            result_text += "\n\nVerification passed with no output."

        return {
            "content": [{
                "type": "text",
                "text": result_text
            }]
        }

    except Exception as e:
        await ctx.error(f"Error verifying {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error verifying {uri}: {str(e)}"
            }]
        }

    finally:
        if session:
            session.close()
        if conn:
            conn.close()

@mcp.tool()
async def dump_block(
    ctx: Context,
    uri: str = Field(description=URI_DESCRIPTION),
    home: str = Field(description=HOME_DESCRIPTION),
    offsets: List[str] = Field(description="List of on-disk block offsets to dump, e.g. ['0', '4096', '8192']"),
    config: Optional[str] = None
) -> Dict:
    """
    Dump the contents of specific on-disk blocks from a WiredTiger file.

    Uses session.verify() with the dump_offsets option to display the contents
    of blocks at the specified byte offsets.

    When to use this tool:
        - You are investigating corruption at known block offsets
        - You need to inspect the raw contents of specific on-disk blocks
        - You have identified suspicious blocks from a B-tree layout and want to examine them
    """
    conn = None
    session = None

    try:
        await ctx.info(f"Dumping blocks at offsets {offsets} from {uri} in {home}")

        offset_list = ",".join(offsets)
        verify_config = f"dump_offsets=[{offset_list}]"
        await ctx.info(f"Verify config: {verify_config}")

        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()

        with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
            conn = wiredtiger.wiredtiger_open(home, config or "")
            session = conn.open_session()
            session.verify(uri, verify_config)

        stdout_output = captured_stdout.getvalue()
        stderr_output = captured_stderr.getvalue()

        result_text = f"Block dump for {uri} at offsets [{offset_list}]"
        if stdout_output:
            result_text += f"\n\nOutput:\n{stdout_output}"
        if stderr_output:
            result_text += f"\n\nErrors:\n{stderr_output}"
        if not stdout_output and not stderr_output:
            result_text += "\n\nNo output produced."

        return {
            "content": [{
                "type": "text",
                "text": result_text
            }]
        }

    except Exception as e:
        await ctx.error(f"Error dumping blocks from {uri}: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error dumping blocks from {uri}: {str(e)}"
            }]
        }

    finally:
        if session:
            session.close()
        if conn:
            conn.close()

@mcp.tool()
async def get_statistics_by_category(
    ctx: Context,
    home: str = Field(description=HOME_DESCRIPTION),
    category: str = Field(description="A keyword to filter statistics by, e.g. 'cache', 'eviction', 'checkpoint', 'transaction', 'cursor', 'btree', 'log', 'lock', 'compact'"),
    uri: Optional[str] = Field(default=None, description="Optional URI to get file/table-level statistics instead of connection-level. Must have a prefix of 'file:' or 'table:'."),
    config: Optional[str] = None
) -> Dict:
    """
    Get WiredTiger statistics filtered by a keyword category.

    Retrieves connection-level or file/table-level statistics and filters them
    to only include entries whose description matches the given keyword. This
    avoids returning the full statistics set (hundreds of entries) and keeps
    results focused and context-window friendly.

    Use the optional `uri` parameter (e.g. "file:test.wt") to get file-level
    statistics instead of connection-level statistics.

    When to use this tool:
        - You want to analyse performance metrics for a specific file or the whole connection
        - You want cache statistics without sifting through hundreds of unrelated stats
        - You are debugging eviction, checkpoint, transaction, or cursor performance
        - You need a focused view of statistics for a specific subsystem
    """
    conn = None
    session = None
    stat_cursor = None

    try:
        stat_target = uri if uri else "connection"
        await ctx.info(f"Getting '{category}' statistics for {stat_target} in {home}")

        if config:
            conn_config = config if "statistics" in config else config + ",statistics=(all)"
        else:
            conn_config = "statistics=(all)"

        conn = wiredtiger.wiredtiger_open(home, conn_config)
        session = conn.open_session()

        cursor_uri = f"statistics:{uri}" if uri else "statistics:"
        stat_cursor = session.open_cursor(cursor_uri, None, None)

        category_lower = category.lower()
        stats = []
        total_count = 0
        while stat_cursor.next() == 0:
            total_count += 1
            value = stat_cursor.get_value()
            # WiredTiger stat cursor value is a 3-element sequence: (description, printable, numeric)
            try:
                desc = value[0]
                value_str = str(value[1])
            except (IndexError, TypeError):
                desc = str(stat_cursor.get_key())
                value_str = str(value)
            if category_lower in str(desc).lower():
                stats.append({
                    "description": desc,
                    "value": value,
                    "printableValue": value_str
                })

        result = {
            "category": category,
            "target": stat_target,
            "matched": len(stats),
            "total_statistics": total_count,
            "statistics": stats
        }

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result, indent=2)
            }]
        }

    except Exception as e:
        await ctx.error(f"Error retrieving statistics: {str(e)}")
        return {
            "content": [{
                "type": "text",
                "text": f"Error retrieving statistics: {str(e)}"
            }]
        }

    finally:
        if stat_cursor:
            stat_cursor.close()
        if session:
            session.close()
        if conn:
            conn.close()

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
