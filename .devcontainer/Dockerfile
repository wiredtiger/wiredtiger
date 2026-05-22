FROM ubuntu:26.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    aspell aspell-en ca-certificates ccache clang-22 clang-format-22 \
    clang-tidy-22 clangd-22 cmake curl diffutils doxygen ed file findutils g++ \
    gcc gdb git gzip liblz4-dev libsnappy-dev libsodium-dev libzstd-dev lld-22 \
    lldb-22 llvm-22 make ninja-build perl python-is-python3 python3 \
    python3-dev python3-pip python3-venv swig tar universal-ctags zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/llvm-22/bin:$PATH"

RUN useradd -m -s /bin/bash wiredtiger
USER wiredtiger

RUN python -m venv /home/wiredtiger/venv
ENV PATH="/home/wiredtiger/venv/bin:$PATH"

RUN pip install --no-cache-dir \
    find_libpython==0.4.0 \
    gcovr==5.0 \
    psutil==5.9.4 \
    ruff==0.4.5

WORKDIR /workdir
