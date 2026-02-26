* No external libraries of code is allowed to be added. Everything must be implemented as part of the project.

* Building project: `git build`.
  * Minimal build for iterations: `git build wtperf`.

* Cleaning the build directory and rebuilding project from scratch: `git rebuild`.

* Quite comprehensive test showing that there are no bugs:
  * Quick (for quick iterations): `cd build/bench/wtperf; ./wtperf -O ../../../sample-ycsb-a-trace-small.wtperf || echo "wtperf failed with code $?"`
  * A bigger one (for the final test when everything looks good): `cd build/bench/wtperf; ./wtperf -O ../../../sample-ycsb-a-trace.wtperf || echo "wtperf failed with code $?"`

* For code generation inspect scripts in the `dist/` subdirectory.
  * For generating function prototypes: `cd dist; python prototypes.py`
  * For generating configuration option parsers: `cd dist; python api_config.py`
  * For generating stat counters: `cd dist; python stat.py`
  * etc.

* For code dependencies, use the `dist/modstat` tool.
  * For instance, checking what members of eviction submodule are used by others, use the command: `dist/modstat -t evict -d full -r --color`.
  * Check `dist/modstat --help` for available options.

* Do not delete any config options that are no longer used - just mark them obsolete in the doc string.

