FROM ubuntu:26.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    aspell=0.60.8.2-3 \
    aspell-en=2020.12.07-0-1.1build1 \
    ca-certificates=20260223 \
    ccache=4.12.3-1 \
    clang=1:21.1.6-71 \
    clang-format=1:21.1.6-71 \
    clang-tidy=1:21.1.6-71 \
    clangd=1:21.1.6-71 \
    cmake=4.2.3-2ubuntu2 \
    curl=8.18.0-1ubuntu2.1 \
    diffutils=1:3.12-1 \
    doxygen=1.15.0+ds1-1ubuntu3 \
    ed=1.22.4-1 \
    file=1:5.46-5build2 \
    findutils=4.10.0-3build2 \
    g++=4:15.2.0-5ubuntu1 \
    gcc=4:15.2.0-5ubuntu1 \
    gdb=17.1-2ubuntu1 \
    git=1:2.53.0-1ubuntu1 \
    gzip=1.14-1~exp2ubuntu1 \
    liblz4-dev=1.10.0-8 \
    libsnappy-dev=1.2.2-2 \
    libsodium-dev=1.0.18-2 \
    libzstd-dev=1.5.7+dfsg-3 \
    lld=1:21.1.6-71 \
    lldb=1:21.1.6-71 \
    llvm=1:21.1.6-71 \
    make=4.4.1-3 \
    ninja-build=1.13.2-1 \
    perl=5.40.1-7build1 \
    python3=3.14.3-0ubuntu2 \
    python3-dev=3.14.3-0ubuntu2 \
    python-is-python3=3.13.3-1+build1 \
    python3-pip=25.1.1+dfsg-1ubuntu2 \
    python3-venv=3.14.3-0ubuntu2 \
    swig=4.4.0-1 \
    tar=1.35+dfsg-4 \
    universal-ctags=6.2.1-1 \
    zlib1g-dev=1:1.3.dfsg+really1.3.1-1ubuntu3 \
    && rm -rf /var/lib/apt/lists/*

# libtinfo5 is required by dist/s_clang_format. It is not available on Ubuntu
# 26.04, so install the Ubuntu 22.04 version.
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ] || [ "$ARCH" = "i386" ]; then \
      BASE_URL="http://security.ubuntu.com/ubuntu/pool/universe/n/ncurses"; \
    else \
      BASE_URL="http://ports.ubuntu.com/ubuntu-ports/pool/universe/n/ncurses"; \
    fi && \
    curl -fsSL "${BASE_URL}/libtinfo5_6.3-2ubuntu0.1_${ARCH}.deb" -o libtinfo5.deb && \
    dpkg -i libtinfo5.deb && \
    rm libtinfo5.deb

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
