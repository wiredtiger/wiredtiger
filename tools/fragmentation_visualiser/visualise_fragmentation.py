import os
import sys
import math
import argparse
import plotly.graph_objs as go
import plotly.io as pio
import imageio.v2 as imageio

def create_fragmentation_image(input_file_path, output_folder):
    allocated_blocks = []
    with open(input_file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line:
                offset_str, size_str = line.split(",")
                allocated_blocks.append((int(offset_str), int(size_str)))

    if not allocated_blocks:
        print(f"No allocated blocks found in {input_file_path}")
        return

    allocated_blocks.pop(0)
    allocated_blocks.pop()
    allocated_blocks.sort()
    last_offset, last_size = allocated_blocks[-1]
    file_size = last_offset + last_size
    print(f"[{input_file_path}] File size: {file_size} bytes")

    # Calculate free blocks
    free_blocks = []
    last_end = 0
    for offset, size in allocated_blocks:
        if offset > last_end:
            free_blocks.append((last_end, offset - last_end))
        last_end = max(last_end, offset + size)
    if last_end < file_size:
        free_blocks.append((last_end, file_size - last_end))

    def generate_image(image_path, blocks, color):
        shapes = []

        SQUARE_BYTES = 4096  # 4KB
        total_blocks = file_size // SQUARE_BYTES
        GRID_WIDTH = math.ceil(math.sqrt(total_blocks))

        print(f"Each square = {SQUARE_BYTES} bytes (4KB)")
        print(f"Total blocks = {total_blocks}, Grid width = {GRID_WIDTH}")

        # Convert all blocks to block indexes
        block_indexes = set()
        for offset, size in blocks:
            start = offset // SQUARE_BYTES
            end = (offset + size) // SQUARE_BYTES
            for i in range(start, end):
                block_indexes.add(i)

        # Sort block indexes so we can group them
        block_indexes = sorted(block_indexes)

        current_row = -1
        row_start = None
        last_col = None

        for index in block_indexes:
            row = index // GRID_WIDTH
            col = index % GRID_WIDTH

            if row != current_row or (last_col is not None and col != last_col + 1):
                # Flush the previous segment if any
                if row_start is not None:
                    x0 = row_start
                    x1 = last_col + 1
                    y0, y1 = -current_row - 1, -current_row
                    shapes.append(dict(type="rect", x0=x0, x1=x1, y0=y0, y1=y1,
                                    line=dict(width=0), fillcolor=color))
                # Start new segment
                row_start = col
                current_row = row

            last_col = col

        # Flush last group
        if row_start is not None:
            x0 = row_start
            x1 = last_col + 1
            y0, y1 = -current_row - 1, -current_row
            shapes.append(dict(type="rect", x0=x0, x1=x1, y0=y0, y1=y1,
                            line=dict(width=0), fillcolor=color))

        # Render figure
        fig = go.Figure()
        fig.update_layout(
            title=None,
            shapes=shapes,
            xaxis=dict(showticklabels=False, range=[0, GRID_WIDTH]),
            yaxis=dict(showticklabels=False, scaleanchor="x", range=[-GRID_WIDTH, 0]),
            margin=dict(t=0, l=0, r=0, b=0),
            plot_bgcolor='white',
            hovermode=False
        )
        fig.write_image(image_path, format="png", scale=5)


    base = os.path.splitext(os.path.basename(input_file_path))[0]
    os.makedirs(output_folder, exist_ok=True)
    img_alloc = os.path.join(output_folder, f"{base}_allocated.png")
    img_free = os.path.join(output_folder, f"{base}_free.png")
    generate_image(img_alloc, allocated_blocks, "#007acc")  # blue for allocated
    generate_image(img_free, free_blocks, "#00aa55")        # green for free
    return base

def generate_html_viewer(output_folder, image_bases):
    html_path = os.path.join(output_folder, "viewer.html")
    with open(html_path, "w") as f:
        f.write("<!DOCTYPE html>\n")
        f.write("<html>\n<head>\n")
        f.write("  <meta charset='UTF-8'>\n")
        f.write("  <title>Fragmentation Viewer</title>\n")
        f.write("  <style>\n")
        f.write("    body {\n")
        f.write("      font-family: sans-serif;\n")
        f.write("      text-align: center;\n")
        f.write("      background: #f8f8f8;\n")
        f.write("      margin: 0;\n")
        f.write("      padding: 0;\n")
        f.write("      overflow: hidden;\n")
        f.write("    }\n")
        f.write("    #viewer {\n")
        f.write("      max-height: 80vh;\n")
        f.write("      max-width: 100%;\n")
        f.write("      height: auto;\n")
        f.write("      border: 2px solid #333;\n")
        f.write("      margin-top: 10px;\n")
        f.write("    }\n")
        f.write("    button {\n")
        f.write("      padding: 8px 16px;\n")
        f.write("      margin: 10px;\n")
        f.write("      font-size: 16px;\n")
        f.write("      cursor: pointer;\n")
        f.write("    }\n")
        f.write("  </style>\n</head>\n<body>\n")
        f.write("  <h2>WT Fragmentation Viewer</h2>\n")
        f.write("  <button onclick='prev()'>⬅️ Previous</button>\n")
        f.write("  <button onclick='toggle()'>🔁 Toggle View</button>\n")
        f.write("  <button onclick='next()'>➡️ Next</button>\n")
        f.write("  <br>\n")
        f.write("  <img id='viewer' src='' alt='Fragmentation Image'>\n")
        f.write("  <script>\n")
        f.write("    const imageBases = [\n")
        f.write(",\n".join(f"      '{base}'" for base in image_bases))
        f.write("\n    ];\n")
        f.write("    let index = 0;\n")
        f.write("    let mode = 0;\n")
        f.write("    function updateImage() {\n")
        f.write("      const suffix = mode === 0 ? '_allocated.png' : '_free.png';\n")
        f.write("      document.getElementById('viewer').src = imageBases[index] + suffix;\n")
        f.write("    }\n")
        f.write("    function toggle() { mode = 1 - mode; updateImage(); }\n")
        f.write("    function next() { index = (index + 1) % imageBases.length; updateImage(); }\n")
        f.write("    function prev() { index = (index - 1 + imageBases.length) % imageBases.length; updateImage(); }\n")
        f.write("    updateImage();\n")
        f.write("  </script>\n</body>\n</html>\n")

    print(f"✅ HTML viewer created: {html_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize fragmentation with linear or hilbert layout")
    parser.add_argument("input_folder", help="Folder containing .txt files with offset,size lines")
    parser.add_argument("--output", default="fragmentation_pngs", help="Output folder")

    args = parser.parse_args()
    image_bases = []
    for filename in os.listdir(args.input_folder):
        if filename.endswith(".txt"):
            path = os.path.join(args.input_folder, filename)
            base = create_fragmentation_image(path, args.output)
            if base:
                image_bases.append(base)
        else:
            print(f"Skipping non-txt file: {filename}")

    if image_bases:
        generate_html_viewer(args.output, image_bases)
