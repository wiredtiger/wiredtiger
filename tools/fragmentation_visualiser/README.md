# WT Fragmentation Visualizer

This tool visualizes WiredTiger file fragmentation using either a linear grid layout or a Hilbert curve layout. It generates high-resolution PNG images and can optionally produce a GIF animation of the result.

---

## `Setup Instructions`

```
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## `Input Format`

Each .txt file in your input folder should contain one line per allocated block in the format:

```
<offset>,<size>
```

Example:

```
20480,16384
36864,8192
45056,8192
```

---

## `Running the Visualizer`

Run the tool on a folder of .txt files like this:

```
python visualise_fragmentation.py <input_folder>
```

### `Examples:`

Generate images using default linear mode:

```
python visualise_fragmentation.py input_folder/
```

Generate images with Hilbert curve of order 5 and create a GIF:

```
python visualise_fragmentation.py input_folder/ --mode hilbert-5 --gif
```

Only generate a GIF from previously generated PNGs:

```
python visualise_fragmentation.py input_folder/ --only-gif
```

---

## `Output`

- PNG files are saved in the output directory (default: `fragmentation_svgs`)
- The generated GIF is saved as `fragmentation.gif` in the working directory

---
