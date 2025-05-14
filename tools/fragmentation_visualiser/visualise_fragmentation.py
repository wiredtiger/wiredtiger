import os
import sys
import math
import argparse
import plotly.graph_objs as go
import plotly.io as pio
import imageio.v2 as imageio

BLOCK_SIZE = 4096  # 4KB

def hilbert_curve(index, order):
    x = y = 0
    t = index
    s = 1
    for _ in range(order):
        rx = 1 & (t // 2)
        ry = 1 & (t ^ rx)
        if ry == 0:
            if rx == 1:
                x, y = s - 1 - x, s - 1 - y
            x, y = y, x
        x += s * rx
        y += s * ry
        t //= 4
        s *= 2
    return x, y

def create_fragmentation_image(input_file_path, output_folder, mode):
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
    last_offset, last_size = allocated_blocks[-1]
    file_size = last_offset + last_size
    print(f"[{input_file_path}] File size: {file_size} bytes")

    shapes = []

    if mode == "linear":
        GRID_WIDTH = 1000
        SQUARE_BYTES = file_size / (GRID_WIDTH * GRID_WIDTH)

        def add_span(offset, size, color):
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
                                   line=dict(width=0.2, color='black'),
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
                                       line=dict(width=0.2, color='black'),
                                       fillcolor=color))

        for offset, size in allocated_blocks:
            add_span(offset, size, "#5e60ce")

        title = "WT Fragmentation Viewer (Linear)"

    elif mode.startswith("hilbert-"):
        order = int(mode.split("-")[1])
        grid_width = 2 ** order
        max_blocks = grid_width * grid_width
        total_blocks = (file_size + BLOCK_SIZE - 1) // BLOCK_SIZE

        if total_blocks > max_blocks:
            print(f"[SKIPPED] {input_file_path} too large for Hilbert-{order} ({total_blocks} blocks > {max_blocks})")
            return

        def get_coords(i):
            return hilbert_curve(i, order)

        def get_block_range(offset, size):
            start = offset // BLOCK_SIZE
            end = (offset + size + BLOCK_SIZE - 1) // BLOCK_SIZE
            return start, end

        for offset, size in allocated_blocks:
            start, end = get_block_range(offset, size)
            for i in range(start, end):
                x, y = get_coords(i)
                shapes.append(dict(
                    type="rect", x0=x, x1=x + 1, y0=-y - 1, y1=-y,
                    line=dict(width=0.2, color='black'),
                    fillcolor="#5e60ce"
                ))

        title = f"WT Fragmentation Viewer (Hilbert Order {order})"

    else:
        print(f"[ERROR] Unknown mode: {mode}")
        return

    fig = go.Figure()
    fig.update_layout(
        title=title,
        shapes=shapes,
        xaxis=dict(showticklabels=False, range=[0, GRID_WIDTH]),
        yaxis=dict(showticklabels=False, scaleanchor="x", range=[-GRID_WIDTH, 0]),
        margin=dict(t=40, l=0, r=0, b=0),
        plot_bgcolor='white',
        hovermode=False
    )

    base = os.path.splitext(os.path.basename(input_file_path))[0]
    output_filename = f"{base}_{mode}_image.png"
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, output_filename)
    fig.write_image(output_path, format="png", scale=5)
    print(f"✅ Saved: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize fragmentation with linear or hilbert layout")
    parser.add_argument("input_folder", help="Folder containing .txt files with offset,size lines")
    parser.add_argument("--output", default="fragmentation_svgs", help="Output folder")
    parser.add_argument("--mode", default="linear", help="linear (default) or hilbert-<order> (e.g. hilbert-3)")
    parser.add_argument("--gif", action="store_true", help="Also generate a GIF from PNGs in the output folder")
    parser.add_argument("--only-gif", action="store_true", help="Skip image generation and just create GIF")

    args = parser.parse_args()

    if not args.only_gif:
        for filename in os.listdir(args.input_folder):
            if filename.endswith(".txt"):
                path = os.path.join(args.input_folder, filename)
                create_fragmentation_image(path, args.output, args.mode)
            else:
                print(f"Skipping non-txt file: {filename}")

    if args.gif or args.only_gif:
        images = []
        for file in os.listdir(args.output):
            if file.endswith(".png"):
                path = os.path.join(args.output, file)
                images.append(imageio.imread(path))
            else:
                print(f"Skipping non-png file: {file}")
        output_gif = "fragmentation.gif"
        imageio.mimsave(output_gif, images, duration=2)
        print(f"✅ GIF saved: {output_gif}")