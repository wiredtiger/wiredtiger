import os
import shutil
import sys
import math
import argparse
import plotly.graph_objs as go
import plotly.io as pio
import imageio.v2 as imageio
import time
print("", flush=True)  
from tqdm import tqdm 

def create_fragmentation_image(input_file_path, output_folder):
    allocated_blocks = []
    with open(input_file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line:
                offset_str, size_str = line.split(",")
                allocated_blocks.append((int(offset_str), int(size_str)))

    if not allocated_blocks:
        tqdm.write(f"No allocated blocks found in {input_file_path}")
        return


    last_offset, last_size = allocated_blocks[-1]
    file_size = last_offset + last_size
    tqdm.write(f"[{input_file_path}] File size: {file_size} bytes")

    # Calculate free blocks
    free_blocks = []
    last_end = 0
    for offset, size in allocated_blocks:
        if offset > last_end:
            free_blocks.append((last_end, offset - last_end))
        last_end = max(last_end, offset + size)
    if last_end < file_size:
        free_blocks.append((last_end, file_size - last_end))

    SQUARE_BYTES = 4096  # 4KB
    total_blocks = file_size // SQUARE_BYTES
    GRID_WIDTH = math.ceil(math.sqrt(total_blocks))
    tqdm.write(f"Each square = {SQUARE_BYTES} bytes (4KB)")
    tqdm.write(f"Total blocks = {total_blocks}, Grid width = {GRID_WIDTH}")

    def generate_image(image_path, blocks, color):
        shapes = []

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

        extra_start = total_blocks                    
        if extra_start < GRID_WIDTH * GRID_WIDTH:
            for index in range(extra_start, GRID_WIDTH * GRID_WIDTH):
                r, c = divmod(index, GRID_WIDTH)
                shapes.append(dict(type="rect",
                                   x0=c,  x1=c+1,
                                   y0=-r-1, y1=-r,
                                   line=dict(width=0), fillcolor="#000000"))

        fig = go.Figure()

        # keep ranges, hide everything else → no padding, shapes visible
        fig.update_xaxes(visible=False, range=[0, GRID_WIDTH], fixedrange=True)
        fig.update_yaxes(visible=False, range=[-GRID_WIDTH, 0],
                         scaleanchor='x', fixedrange=True)

        safe_grid_width = max(GRID_WIDTH, 10)
        fig.update_layout(
            shapes=shapes,
            width=safe_grid_width, height=safe_grid_width,
            margin=dict(t=0, l=0, r=0, b=0, pad=0),
            plot_bgcolor='white', paper_bgcolor='white',
            hovermode=False
        )
        fig.write_image(image_path, format="png", scale=5)


    base = os.path.splitext(os.path.basename(input_file_path))[0]
    os.makedirs(output_folder, exist_ok=True)
    img_alloc = os.path.join(output_folder, f"{base}_allocated.png")
    img_free = os.path.join(output_folder, f"{base}_free.png")
    generate_image(img_alloc, allocated_blocks, "#007acc")  # blue for allocated
    generate_image(img_free, free_blocks, "#00aa55")        # green for free
    return base, file_size, GRID_WIDTH

def generate_html_viewer(output_folder: str,
                         bases: list[str],
                         sizes: list[int],
                         grids: list[int]) -> None:
    """Write viewer.html into *output_folder* with simple JS navigation + toggle‑zoom."""

    img_list = ",\n      ".join(f"'{b}'" for b in bases)

    html = (
        """<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>Fragmentation Viewer</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{
      height:100vh;display:flex;flex-direction:column;justify-content:center;align-items:center;
      background:#ffffff;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#111;
    }
    h1{font-size:1.25rem;font-weight:600;margin-bottom:1.25rem;letter-spacing:.03em}
    /*  Compact frame: 82vmin square, up to 820px */
    .viewer{width:82vmin;max-width:820px;aspect-ratio:1/1;overflow:hidden;}
    #viewer-img{
    width:100%;
    height:100%;
    object-fit:contain;
    transition:transform .25s ease,opacity .25s;
    cursor:zoom-in;
    border:2px solid #000000;
    }
    .ctr{display:flex;gap:1.5rem;margin-top:1.25rem}
    button{all:unset;cursor:pointer;font-weight:600;font-size:1rem;position:relative;}
    button::after{content:'';position:absolute;left:0;bottom:-2px;width:0;height:2px;background:#2563eb;transition:width .2s ease;}
    button:hover::after{width:100%}
    .legend{display:flex;align-items:center;gap:.5rem;font-size:.9rem;margin-bottom:1rem}
    .swatch{width:.9rem;height:.9rem;border:1px solid #ccc;border-radius:2px;display:inline-block}
    .blue {background:#007acc}
    .green{background:#00aa55}
  </style>
</head>
<body>
  <h1>WT Fragmentation Viewer</h1>
  <p id="meta" style="margin-bottom:.5rem;font-size:.95rem;color:#555"></p>
  <div class="legend">
    <span class="swatch blue"></span> allocated
    <span class="swatch green"></span> free
  </div>
  <div class='viewer'><img id='viewer-img' alt='fragment map'></div>
  <div class='ctr'>
    <button id='prev'>Prev</button>
    <button id='toggle'>Toggle</button>
    <button id='next'>Next</button>
  </div>
<script>
const bases=[
      """ + img_list + """
];
const sizes = [ 
      """ + ",\n      ".join(str(s) for s in sizes) + """
];
const grids = [
      """ + ",\n      ".join(str(g) for g in grids) + """
];
let idx=0,mode=0;const scale=2.5;let zoomed=false;const img=document.getElementById('viewer-img');


function resetZoom(){zoomed=false;img.style.transform='scale(1)';img.style.cursor='zoom-in';}
function zoomToggle(ev){
  const r=img.getBoundingClientRect();
  const ox=((ev.clientX-r.left)/r.width)*100;
  const oy=((ev.clientY-r.top)/r.height)*100;
  if(zoomed){resetZoom();return;}
  img.style.transformOrigin=`${ox}% ${oy}%`;
  img.style.transform=`scale(${scale})`;
  img.style.cursor='zoom-out';
  zoomed=true;
}

function update(){
  img.style.opacity=0;
  const suf=mode? '_free.png':'_allocated.png';
  img.onload=()=>img.style.opacity=1;
  img.src=bases[idx]+suf;
  document.getElementById('meta').textContent =
  `Image ${idx + 1} of ${bases.length}  |  ${bases[idx]}.txt  |  ` +
  `${sizes[idx].toLocaleString()} bytes  |  ` +
  `grid ${grids[idx]}×${grids[idx]} (4 KB squares)`;

  resetZoom();
}
function next(){idx=(idx+1)%bases.length;update();}
function prev(){idx=(idx-1+bases.length)%bases.length;update();}
function tog(){mode^=1;update();}

document.getElementById('next').onclick=next;
document.getElementById('prev').onclick=prev;
document.getElementById('toggle').onclick=tog;

document.addEventListener('keydown',e=>{
  if(e.key==='ArrowRight')next();
  else if(e.key==='ArrowLeft')prev();
  else if(e.code==='Space'){e.preventDefault();tog();}
});

img.addEventListener('click',zoomToggle);
update();
</script>
</body>
</html>"""
    )

    out_path = os.path.join(output_folder, "viewer.html")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    tqdm.write(f"✅ HTML viewer written → {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize fragmentation with linear or hilbert layout")
    parser.add_argument("input_folder", help="Folder containing .txt files with offset,size lines")
    parser.add_argument("--output", default="fragmentation_pngs", help="Output folder")

    args = parser.parse_args()
    if os.path.isdir(args.output):          # delete any old copy
        shutil.rmtree(args.output)

    records = []
    for filename in tqdm(os.listdir(args.input_folder), desc="Processing files", unit="file", leave=False):
        if filename.endswith(".txt"):
            path = os.path.join(args.input_folder, filename)    
            res = create_fragmentation_image(path, args.output)
            if res:
                records.append(res)
        else:
            tqdm.write(f"Skipping non-txt file: {filename}")

    if records:
        records.sort(key=lambda t: t[0])      # alphabetical by base name
        image_bases, file_sizes, grid_sizes = map(list, zip(*records))
        generate_html_viewer(args.output, image_bases,
                             file_sizes, grid_sizes)

