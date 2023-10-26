# Custom gdb scripts
Custom gdb scripts for gdb debugging and autoloading of these scripts.
individual scripts are located in the `gdb_scripts/` and the top level directory is reserved for loader scripts `load_gdb_scripts.py`.

### Usage
To loads scripts from within gdb call `source /path/to/load_gdb_scripts.py`.
If you have compiled WiredTiger with the `-DHAVE_SHARED=1` flag then these scripts 
will be auto-loaded when gdb is opened.

### Creating new scripts
Scripts can be written in either python ([hazard_pointers.py](./gdb_scripts/hazard_pointers.py)) or scheme ([dump_row_int.gdb](./gdb_scripts/dump_row_int.gdb)). If you create a new python script it is recommended you follow the style in `hazard_pointers.py` by creating a class that extends `gdb.Command`.
Don't forget to register the new command by calling its constructor at the bottom of the file.
Once created update `load_gdb_scripts.py` to include the new script.