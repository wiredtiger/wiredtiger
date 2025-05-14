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
        GRID_WIDTH = 1000
        SQUARE_BYTES = file_size / (GRID_WIDTH * GRID_WIDTH)

        def add_span(offset, size):
            start_block = offset / SQUARE_BYTES
            end_block = (offset + size) / SQUARE_BYTES

            start_row = int(start_block) // GRID_WIDTH
            start_col = int(start_block) % GRID_WIDTH
            end_row = int(end_block) // GRID_WIDTH
            end_col = int(end_block) % GRID_WIDTH

            if start_row == end_row:
                x0, x1 = start_col, end_col
                y0, y1 = -start_row - 1, -start_row
                shapes.append(dict(type="rect", x0=x0, y0=y0, x1=x1, y1=y1,
                                    line=dict(width=0, color='black'),
                                    fillcolor=color))
            else:
                for row in range(start_row, end_row + 1):
                    row_start_block = row * GRID_WIDTH
                    row_end_block = row_start_block + GRID_WIDTH
                    seg_start = max(start_block, row_start_block)
                    seg_end = min(end_block, row_end_block)

                    x0 = int(seg_start) % GRID_WIDTH
                    x1 = int(seg_end) % GRID_WIDTH if row != end_row else int(seg_end - row * GRID_WIDTH)
                    if x1 <= x0:
                        x1 = GRID_WIDTH

                    y0, y1 = -row - 1, -row
                    shapes.append(dict(type="rect", x0=x0, y0=y0, x1=x1, y1=y1,
                                        line=dict(width=0, color='black'),
                                        fillcolor=color))

        for offset, size in blocks:
            add_span(offset, size)

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
        fig.write_image(image_path, format="png", scale=3)

    base = os.path.splitext(os.path.basename(input_file_path))[0]
    os.makedirs(output_folder, exist_ok=True)
    img_alloc = os.path.join(output_folder, f"{base}_allocated.png")
    img_free = os.path.join(output_folder, f"{base}_free.png")
    generate_image(img_alloc, allocated_blocks, "#000000")
    generate_image(img_free, free_blocks, "#000000")
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
    parser.add_argument("--output", default="fragmentation_svgs", help="Output folder")

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