import re
import os
import argparse
import time
import subprocess
import sys
import tempfile

# Global variable to control debug printing
DEBUG_MODE = False

def print_debug(message):
    if DEBUG_MODE:
        print(message)

def process_and_decode_dump(dump_info, byte_list, decoder_script_path, output_dir_for_decoded_files):
    """
    Processes collected byte dump, runs wt_binary_decode.py with bytes as stdin,
    and saves the decoder's output to a file.
    """
    if not dump_info or not byte_list:
        return

    # Construct filename for the DECODED output
    file_base = dump_info['file_name_raw'].replace('.wt', '')
    
    if '.' in dump_info['raw_extent_name']:
        extent_name = dump_info['raw_extent_name'].split('.')[-1]
    else:
        extent_name = dump_info['raw_extent_name']
            
    timestamp_formatted = dump_info['timestamp'].replace(':', '-')
    
    # Filename for the output of wt_binary_decode.py
    decoded_filename = f"{file_base}-{extent_name}-{timestamp_formatted}-size{dump_info['size']}_decoded.txt"
    decoded_filepath = os.path.join(output_dir_for_decoded_files, decoded_filename)

    full_byte_string = " ".join(byte_list)
    full_byte_string = re.sub(r'\s+', ' ', full_byte_string).strip()

    if not full_byte_string:
        print_debug(f"DEBUG: No byte string to decode for {dump_info['timestamp']}.")
        return

    command = [
        sys.executable,  # Use the current python interpreter
        decoder_script_path,
        "--dumpin",
        "--ext",
        "-"
    ]

    print_debug(f"DEBUG: Running decoder for {dump_info['timestamp']}. Command: {' '.join(command)}")
    print_debug(f"DEBUG: Stdin for decoder (first 100 chars): {full_byte_string[:100]}")

    try:
        result = subprocess.run(
            command,
            input=full_byte_string,
            capture_output=True,
            text=True,  # Expect text from stdin/stdout
            check=False,  # Manually check result
            timeout=300  # Add a timeout (e.g., 5 minutes)
        )

        if result.returncode == 0:
            output_lines = []
            found_marker = False
            # Regex to parse lines like "1:   219648, 165888"
            # It captures the "219648, 165888" part.
            # ^\s*      : matches the beginning of the line with optional leading whitespace
            # \d+       : matches one or more digits (e.g., "1")
            # :\s*      : matches the colon separator with optional following whitespace
            # (\d+,\s*\d+) : captures the group of numbers and comma (e.g., "219648, 165888")
            line_parser_regex = re.compile(r"^\s*\d+:\s*(\d+,\s*\d+)")

            for line in result.stdout.splitlines():
                if found_marker:
                    match = line_parser_regex.match(line)
                    if match:
                        # If the line matches the pattern, append only the captured group
                        output_lines.append(match.group(1))
                    else:
                        # If the line doesn't match the pattern, append it as is
                        output_lines.append(line)
                elif "extent list follows:" in line:
                    found_marker = True
                
                if not output_lines and "extent list follows:" not in result.stdout: # If marker not found, write everything
                    print_debug(f"DEBUG: 'extent list follows:' not found in decoder output for {dump_info['timestamp']}. Writing full output.")
                    output_to_write = result.stdout
                elif not output_lines and found_marker: # Marker found, but no lines after it
                    print_debug(f"DEBUG: 'extent list follows:' found, but no subsequent lines for {dump_info['timestamp']}. Writing empty.")
                    output_to_write = ""
                else:
                    output_to_write = "\n".join(output_lines)

            with open(decoded_filepath, 'w', encoding="utf-8") as f_out:
                f_out.write(output_to_write)
            print(f"Successfully decoded and saved to: {decoded_filepath}")
            print_debug(f"DEBUG: Decoder stdout (first 100 chars of processed output): {output_to_write[:100]}")
        else:
            print(f"Error decoding byte dump for {dump_info['timestamp']} (file: {dump_info['file_name_raw']}):")
            print(f"  Command: {' '.join(command)}")
            print(f"  Return code: {result.returncode}")
            print(f"  Stderr: {result.stderr.strip()}")
            print_debug(f"  Stdout (if any): {result.stdout.strip()}")

    except FileNotFoundError:
        print(f"Error: Decoder script '{decoder_script_path}' not found. Please check the path provided via --decoder_script.")
    except subprocess.TimeoutExpired:
        print(f"Error: Decoder script timed out for {dump_info['timestamp']}.")
    except OSError as oe:  # Catch OS-level errors like permission issues
        print(f"OS error while running decoder for {dump_info['timestamp']}: {oe}")
    except subprocess.SubprocessError as e:  # More specific for subprocess issues
        print(f"An unexpected subprocess error occurred for {dump_info['timestamp']}: {type(e).__name__} - {e}")

def parse_log_for_byte_dumps(log_filepath="stdout.txt", output_dir="byte_dumps", decoder_script_path="wt_binary_decode.py", stream_mode=False):
    """
    Parses a log file to extract byte dumps, decodes them using an external script via stdin,
    and saves the decoded output. Optionally monitors the log file continuously.

    Args:
        log_filepath (str): Path to the log file.
        output_dir (str): Directory where extracted AND DECODED byte dump files will be saved.
        decoder_script_path (str): Path to the wt_binary_decode.py script.
        stream_mode (bool): If True, continuously monitor the log file. Otherwise, read once.
    """

    # Regex to identify the "header" line of a byte dump
    # Captures: 1=timestamp, 2=file_name, 3=raw_extent_name, 4=size
    header_regex = re.compile(
        r"^(?:0x[0-9a-fA-F]+[:\s]\s*)?"  # Optional hex address (e.g., 0x...) followed by ':' or space, and optional spaces
        r"\[(\d+:\d+)\]"                 # Group 1: Timestamp (e.g., [1747034539:956936])
        r"(?:\[\d+:0x[0-9a-fA-F]+\])?"   # Optional PID/ThreadID (e.g., [91525:0x20466cc80])
        r".*?file:([^,]+),"              # Group 2: File name (e.g., WiredTiger.wt)
        r".*?([\w.-]+): byte dump"       # Group 3: Raw extent name (e.g., live.alloc or table.foo-bar)
        r".*?size:\s*(\d+)\s*\]"         # Group 4: Size (e.g., from "[...size: 36864]")
    )

    # Regex to identify a data line of a byte dump
    # Captures: 1=block_identifier, 2=hex_bytes
    # The block_identifier ensures we are collecting chunks for the same block
    data_regex = re.compile(
        # Optional hex address (e.g., 0x...) followed by ':' or space, and optional spaces.
        # This matches prefixes like "0x140028040:"
        r"^(?:0x[0-9a-fA-F]+[:\s]\s*)?"
        # Timestamp in square brackets (e.g., [1747020001:639030])
        r"\[\d+:\d+\]"
        # Optional PID/ThreadID (e.g., [69153:0x20466cc80])
        r"(?:\[\d+:0x[0-9a-fA-F]+\])?"
        # Non-greedy match for any intervening text before the block identifier.
        # This can include various log prefixes like ", t, file:...", "WT_SESSION...", etc.
        r".*?"
        # Group 1: Block identifier.
        # Example: {0: 36864, 4096, 0xeceecc3c}:
        r"(\{0: \d+, \d+, 0x[0-9a-fA-F]+\}):"
        # Chunk information (e.g., (chunk 1 of 2): )
        r" \(chunk \d+ of \d+\):\s*"
        # Group 2: Hex bytes (e.g., 00 00 ... FF)
        r"([0-9a-fA-F\s]+)$"
    )

    if not os.path.exists(output_dir):  # This is now for decoded files
        os.makedirs(output_dir)
        print(f"Created output directory for decoded files: {output_dir}")

    current_dump_info = None
    collected_bytes = []
    
    processed_log_filepath = log_filepath
    temp_file_created_path = None

    if not stream_mode:
        print_debug(f"Preprocessing log file: {log_filepath} to filter for 'WT_VERB_BLOCK_EXT'")
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding="utf-8") as temp_f:
                temp_file_created_path = temp_f.name
                # Run grep and write to temp_f.name
                grep_command = ['grep', 'WT_VERB_BLOCK_EXT', log_filepath]
                
                # Using subprocess.Popen to redirect stdout directly to the file handle
                process = subprocess.Popen(grep_command, stdout=temp_f, text=True, stderr=subprocess.PIPE)
                _, stderr_data = process.communicate() # Wait for grep to finish and capture stderr
                
                if process.returncode != 0 and process.returncode != 1: # 0 for match, 1 for no match (which is OK)
                    print(f"Warning: grep command for preprocessing returned {process.returncode}.")
                    if stderr_data:
                        print(f"Grep stderr: {stderr_data.strip()}")
                elif process.returncode == 0:
                    print_debug("Grep found matching lines for 'WT_VERB_BLOCK_EXT'.")
                else: # returncode == 1
                    print_debug("Grep found no matching lines for 'WT_VERB_BLOCK_EXT'. Filtered file will be empty.")

            # temp_f is now closed as its 'with' block ended.
            # The file at temp_file_created_path contains the grep output (or is empty).
            processed_log_filepath = temp_file_created_path
            print_debug(f"Preprocessing complete. Using filtered log: {processed_log_filepath}")

        except FileNotFoundError:
            print(f"Error: Original log file '{log_filepath}' not found for preprocessing.")
            if temp_file_created_path and os.path.exists(temp_file_created_path): # Clean up if temp file was created before error
                os.remove(temp_file_created_path)
            return # Cannot proceed if original log is not found
        except (OSError, subprocess.SubprocessError) as e: # More specific exception handling
            print(f"Error during preprocessing: {e}")
            if temp_file_created_path and os.path.exists(temp_file_created_path):
                os.remove(temp_file_created_path) # Clean up on error
            return

    try:
        print_debug(f"Attempting to open log file: {processed_log_filepath}")
        with open(processed_log_filepath, 'r', encoding="utf-8") as f_in:
            if stream_mode: # This block will not be hit if stream_mode is False due to above logic
                # Go to the end of the file for streaming
                f_in.seek(0, os.SEEK_END)
                print(f"Monitoring log file for new entries (streaming mode): {log_filepath}") # Original path for user message
            else:
                # If not streaming, we are reading from processed_log_filepath (original or temp)
                print(f"Processing log file (read-once mode): {processed_log_filepath}")

            while True:  # Loop indefinitely for streaming, or break if not streaming
                line = f_in.readline()
                if not line:
                    if stream_mode:
                        time.sleep(0.1)  # Wait for new lines in stream mode
                        continue
                    else:
                        break # End of file in read-once mode

                line = line.strip()
                
                header_match = header_regex.match(line)
                data_match = data_regex.match(line)

                if header_match:
                    if current_dump_info and collected_bytes:
                        process_and_decode_dump(current_dump_info, collected_bytes, decoder_script_path, output_dir)
                    
                    current_dump_info = {
                        'timestamp': header_match.group(1),
                        'file_name_raw': header_match.group(2),
                        'raw_extent_name': header_match.group(3),
                        'size': header_match.group(4),
                        'expected_block_id': None
                    }
                    collected_bytes = []
                    print_debug(f"DEBUG: New header found: {current_dump_info}")

                elif data_match and current_dump_info:
                    block_id = data_match.group(1)
                    hex_data = data_match.group(2).strip()

                    if not isinstance(current_dump_info, dict):
                        print_debug(f"DEBUG: current_dump_info is not a dict (value: {current_dump_info}), skipping data line. Waiting for new header.")
                        collected_bytes = [] 
                        continue
                    else:
                        # Explicitly type hint for the linter, though it should be a dict.
                        current_dump_info_dict = dict(current_dump_info)
                        if current_dump_info_dict.get('expected_block_id') is None:
                            current_dump_info_dict['expected_block_id'] = block_id
                            print_debug(f"DEBUG: Set expected_block_id to {block_id} for {current_dump_info_dict.get('timestamp')}")
                        
                        # Update current_dump_info with the (potentially) modified dictionary
                        current_dump_info = current_dump_info_dict

                        if current_dump_info.get('expected_block_id') == block_id:
                            collected_bytes.append(hex_data)
                            print_debug(f"DEBUG: Appended data for block {block_id}, ts {current_dump_info.get('timestamp')}")
                        else:
                            print_debug(f"DEBUG: Mismatched block_id. Expected {current_dump_info.get('expected_block_id')}, got {block_id}. Processing previous dump.")
                            if collected_bytes:
                                process_and_decode_dump(current_dump_info, collected_bytes, decoder_script_path, output_dir)
                            current_dump_info = None 
                            collected_bytes = []

                elif current_dump_info and collected_bytes and line: 
                    print_debug(f"DEBUG: Non-dump line encountered. Processing previous dump for ts {current_dump_info.get('timestamp')}.")
                    process_and_decode_dump(current_dump_info, collected_bytes, decoder_script_path, output_dir)
                    current_dump_info = None
                    collected_bytes = []
            
            # Process any remaining dump after the loop (e.g., end of file in non-stream mode)
            if current_dump_info and collected_bytes:
                print_debug(f"DEBUG: End of file or stream. Processing final dump for ts {current_dump_info.get('timestamp')}.")
                process_and_decode_dump(current_dump_info, collected_bytes, decoder_script_path, output_dir)

    except FileNotFoundError:
        print(f"Error: Log file '{processed_log_filepath}' not found.")
    except KeyboardInterrupt: 
        print("\nMonitoring stopped by user.")
        if current_dump_info and collected_bytes and isinstance(current_dump_info, dict):
            print_debug(f"DEBUG: Interrupted. Processing final dump for ts {current_dump_info.get('timestamp', 'N/A')}.")
            process_and_decode_dump(current_dump_info, collected_bytes, decoder_script_path, output_dir)
    except IOError as ioe:
        print(f"An I/O error occurred: {ioe}")
    except RuntimeError as e:  # More specific for runtime issues in main loop
        print(f"An unexpected runtime error occurred in the main parsing loop: {type(e).__name__} - {e}")
    finally:
        if temp_file_created_path and os.path.exists(temp_file_created_path):
            print_debug(f"Cleaning up temporary filtered log file: {temp_file_created_path}")
            os.remove(temp_file_created_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse byte dumps from a log file and decode them.")
    parser.add_argument("log_file", help="Path to the log file to parse.")
    parser.add_argument("-o", "--output_dir", default="decoded_dumps", help="Directory to save DECODED byte dumps (default: decoded_dumps)")
    parser.add_argument("--decoder_script", default="wt_binary_decode.py", help="Path to the wt_binary_decode.py script (default: wt_binary_decode.py). Assumes it's in PATH, same dir, or an absolute path is given.")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug printing.")
    parser.add_argument("--stream", action="store_true", help="Continuously monitor the log file for new entries. If not set, reads the entire file once.")
    args = parser.parse_args()

    if args.debug:
        DEBUG_MODE = True

    # Resolve decoder script path (basic check, can be enhanced like in run_decode_on_dir.py)
    resolved_decoder_path = args.decoder_script
    if not os.path.isabs(resolved_decoder_path):
        # Try relative to this script's directory
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        potential_path = os.path.join(current_script_dir, resolved_decoder_path)
        if os.path.isfile(potential_path):
            resolved_decoder_path = potential_path
        # Else, assume it's in PATH or CWD (subprocess will find it or fail)
    
    print(f"Using decoder script: {resolved_decoder_path}")
    if not os.path.isfile(resolved_decoder_path) and not any(os.access(os.path.join(path, resolved_decoder_path), os.X_OK) for path in os.environ["PATH"].split(os.pathsep)):
         # A more robust check if it's not an absolute/relative file and not in PATH
        print(f"Warning: Decoder script '{resolved_decoder_path}' not found as a direct file. Will try to run assuming it's in PATH.")

    parse_log_for_byte_dumps(args.log_file, args.output_dir, resolved_decoder_path, args.stream)
