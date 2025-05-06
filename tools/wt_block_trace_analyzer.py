#!/usr/bin/env python3
"""
WiredTiger Block Trace Analyzer

This tool combines block trace parsing and visualization capabilities:
1. Parse WiredTiger block trace logs and extract timestamp and block information
2. Generate heatmap visualizations of block access patterns

Usage:
    # Parse WiredTiger logs and generate heatmaps in one step
    python wt_block_trace_analyzer.py --parse logs.txt --output-dir ./output --plot
    
    # Only parse logs without plotting
    python wt_block_trace_analyzer.py --parse logs.txt --output-dir ./parsed_data
    
    # Only plot existing parsed data
    python wt_block_trace_analyzer.py --plot-dir ./parsed_data --plot-output ./visualizations
    
    # Parse from stdin
    cat logs.txt | python wt_block_trace_analyzer.py --parse - --output-dir ./output --plot

Example input line (for parsing):
[1746443849:035810][44314:0x20466cc80], test_compact16.test_compact16.test_compact16, file:test_compact16.wt, connection-open-session: [WT_VERB_BLOCK_TRACE][DEBUG_1]: 90112,28672

Output visualization: Heatmaps showing block access patterns over time for each file
"""

import sys
import re
import os
import glob
import argparse
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np
import pandas as pd


# ======== BLOCK TRACE PARSING FUNCTIONS ========

def parse_block_trace_line(line):
    """
    Parse a WiredTiger block trace log line and extract timestamp, file name, and block information.
    
    Args:
        line (str): A line from WiredTiger block trace log
        
    Returns:
        tuple: (timestamp, file_name, block_offset, block_size) if successful, None otherwise
    """
    # Regular expression to extract timestamp, file name and block info
    pattern = r'\[(\d+:\d+)\].*?file:([\w.]+).*?: (\d+),(\d+)'
    match = re.search(pattern, line)
    
    if match:
        timestamp = match.group(1)
        file_name = match.group(2)
        offset = match.group(3)
        size = match.group(4)
        return (timestamp, file_name, offset, size)
    
    return None


def parse_trace_data(file_obj, output_dir='.'):
    """
    Process lines from a file object and write extracted information to separate files.
    
    Args:
        file_obj: File object to read lines from
        output_dir: Directory to write output files to
        
    Returns:
        dict: Statistics about the parsed files
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Dictionary to hold file handles for each data file
    file_handles = {}
    
    # Dictionary to track statistics
    stats = {}
    
    for line in file_obj:
        result = parse_block_trace_line(line.strip())
        if result:
            timestamp, file_name, offset, size = result
            
            # Create output file name based on the data file name
            output_file = os.path.join(output_dir, f"block_trace_{file_name}.txt")
            
            # Open file handle if we haven't seen this file before
            if file_name not in file_handles:
                file_handles[file_name] = open(output_file, 'w')
                stats[file_name] = {'count': 0, 'total_size': 0, 'output_file': output_file}
            
            # Write the timestamp, offset, and size to the file
            file_handles[file_name].write(f"{timestamp} {offset} {size}\n")
            
            # Update statistics
            stats[file_name]['count'] += 1
            stats[file_name]['total_size'] += int(size)
    
    # Close all file handles
    for fh in file_handles.values():
        fh.close()
    
    # Print summary
    print("Block trace parsing completed:")
    print(f"Found {len(file_handles)} unique data files")
    
    for file_name, file_stats in stats.items():
        print(f"  {file_name}: {file_stats['count']} blocks, {file_stats['total_size'] / (1024*1024):.2f} MB")
        
    return stats


# ======== HEATMAP VISUALIZATION FUNCTIONS ========

def parse_timestamp(ts_str):
    """Parse timestamp from format 'seconds:microseconds'."""
    seconds, microseconds = map(int, ts_str.split(':'))
    return seconds + microseconds / 1_000_000

def load_trace_data(filename):
    """Load and parse the trace data from the file."""
    data = []
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 3:
                timestamp_str, offset, size = parts
                timestamp = parse_timestamp(timestamp_str)
                data.append((timestamp, int(offset), int(size)))
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data, columns=['timestamp', 'offset', 'size'])
    
    # Calculate end_offset for each block
    df['end_offset'] = df['offset'] + df['size']
    
    return df

def plot_heatmap(df, output_file=None, title=None, show_plot=True):
    """Create a heatmap visualization of block access patterns."""
    if df.empty:
        print(f"No data to plot for {title if title else 'heatmap'}")
        return
        
    # Normalize timestamps to start from 0
    start_time = df['timestamp'].min()
    df['relative_time'] = df['timestamp'] - start_time
    
    # Create the figure
    plt.figure(figsize=(12, 8))
    
    # Create scatter plot where:
    # - x-axis is offset
    # - y-axis is time (going down)
    # - color intensity is size of the block
    
    # Use a logarithmic colormap to better visualize the sizes
    scatter = plt.scatter(
        df['offset'], 
        df['relative_time'],
        c=df['size'],
        cmap='viridis',
        s=10,  # marker size
        alpha=0.7,  # transparency
        norm=colors.LogNorm()  # logarithmic color scale
    )
    
    # Add colorbar to show size scale
    cbar = plt.colorbar(scatter)
    cbar.set_label('Block Size (bytes)')
    
    # Set axis labels and title
    plt.xlabel('Offset (bytes)')
    plt.ylabel('Time (seconds from start)')
    
    # Use provided title or generate one from the filename
    if title:
        plt.title(f'Block Access Patterns - {title}')
    else:
        plt.title('Block Access Patterns')
    
    # Format the x-axis to show values in MB
    plt.ticklabel_format(axis='x', style='plain')
    locs, labels = plt.xticks()
    new_labels = [f"{int(loc/1024/1024)}MB" if loc > 0 else "0" for loc in locs]
    plt.xticks(locs, new_labels)
    
    # Add grid for better readability
    plt.grid(True, alpha=0.3)
    
    # Invert y-axis so time flows downward
    plt.gca().invert_yaxis()
    
    # Tight layout
    plt.tight_layout()
    
    # Save plot if filename specified
    if output_file:
        plt.savefig(output_file)
        print(f"Plot saved to {output_file}")
    
    # Show plot if requested
    if show_plot:
        plt.show()
    else:
        plt.close()

def process_trace_files(input_dir, output_dir=None, show_plots=False):
    """Process all block trace files in a directory and create heatmaps."""
    if output_dir is None:
        output_dir = input_dir
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all block trace files
    pattern = os.path.join(input_dir, "block_trace_*.txt")
    trace_files = glob.glob(pattern)
    
    if not trace_files:
        print(f"No block trace files found matching {pattern}")
        return
    
    print(f"Found {len(trace_files)} block trace files to visualize")
    
    # Process each file
    results = []
    for trace_file in trace_files:
        # Extract file name for title and output file
        base_name = os.path.basename(trace_file)
        file_name = base_name.replace("block_trace_", "").replace(".txt", "")
        
        # Load data
        print(f"Loading trace data from {trace_file}...")
        df = load_trace_data(trace_file)
        print(f"Loaded {len(df)} records for {file_name}")
        
        if df.empty:
            print(f"Skipping {file_name} - no data")
            continue
        
        # Generate output filename
        output_file = os.path.join(output_dir, f"heatmap_{file_name}.png")
        results.append(output_file)
        
        # Print stats
        total_data = df['size'].sum() / (1024 * 1024)  # Convert to MB
        print(f"  Total data accessed: {total_data:.2f} MB")
        print(f"  Offset range: {df['offset'].min()} - {df['offset'].max()} bytes")
        print(f"  Time span: {df['timestamp'].max() - df['timestamp'].min():.3f} seconds")
        
        # Plot data
        print(f"Generating heatmap for {file_name}...")
        plot_heatmap(df, output_file, title=file_name, show_plot=show_plots)
    
    return results


# ======== MAIN FUNCTIONALITY ========

def main():
    """Main function to handle command line arguments and control workflow."""
    parser = argparse.ArgumentParser(
        description='WiredTiger Block Trace Analyzer - Parse and visualize block trace data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse WiredTiger logs and generate heatmaps in one step
  python wt_block_trace_analyzer.py --parse logs.txt --output-dir ./output --plot
  
  # Only parse logs without plotting
  python wt_block_trace_analyzer.py --parse logs.txt --output-dir ./parsed_data
  
  # Only plot existing parsed data
  python wt_block_trace_analyzer.py --plot-dir ./parsed_data --plot-output ./visualizations
  
  # Parse from stdin
  cat logs.txt | python wt_block_trace_analyzer.py --parse - --output-dir ./output --plot
        """
    )
    
    # Parsing options
    parser.add_argument('--parse', metavar='INPUT_FILE', 
                        help='Parse WiredTiger block trace logs (use "-" for stdin)')
    parser.add_argument('--output-dir', '-o', default='./output_traces',
                        help='Directory to store parsed trace files (default: ./output_traces)')
    
    # Plotting options
    parser.add_argument('--plot', action='store_true',
                        help='Generate heatmap visualizations after parsing')
    parser.add_argument('--plot-dir', 
                        help='Directory containing existing parsed trace files to plot')
    parser.add_argument('--plot-output', default='./output_plots',
                        help='Directory to store heatmap visualizations (default: ./output_plots)')
    parser.add_argument('--show-plots', action='store_true',
                        help='Show plots interactively (default: save to file only)')
    
    args = parser.parse_args()
    
    # Check that at least one action is specified
    if not (args.parse or args.plot_dir):
        parser.error("No action specified. Use --parse or --plot-dir")
        return
    
    # STEP 1: Parse WiredTiger block trace logs if requested
    parsed_files_stats = {}
    if args.parse:
        print(f"Parsing block trace logs from {'stdin' if args.parse == '-' else args.parse}...")
        
        # Create output directory for parsed files if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Open input file or use stdin
        if args.parse == '-':
            parsed_files_stats = parse_trace_data(sys.stdin, args.output_dir)
        else:
            try:
                with open(args.parse, 'r') as f:
                    parsed_files_stats = parse_trace_data(f, args.output_dir)
            except IOError as e:
                print(f"Error opening file {args.parse}: {e}", file=sys.stderr)
                sys.exit(1)
    
    # STEP 2: Plot heatmaps if requested
    if args.plot or args.plot_dir:
        # Determine which directory to use for plot input
        plot_input_dir = args.plot_dir if args.plot_dir else args.output_dir
        
        # Create output directory for plots if it doesn't exist
        os.makedirs(args.plot_output, exist_ok=True)
        
        print(f"Generating heatmap visualizations from {plot_input_dir}...")
        process_trace_files(plot_input_dir, args.plot_output, show_plots=args.show_plots)


if __name__ == "__main__":
    main()
