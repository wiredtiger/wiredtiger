FROM ubuntu
MAINTAINER me@gmail.com
WORKDIR /opt
RUN mkdir -p bin/test/format
RUN mkdir -p /data/RUNDIR
COPY cmake_build/test/format/t bin/test/format/
COPY cmake_build/test/format/CONFIG.antithesis bin/test/format/
COPY tools/voidstar/lib/libvoidstar.so tools/voidstar/lib/
COPY cmake_build/libwiredtiger.so.11.0.1 bin/
COPY cmake_build/ext bin/ext
COPY test.sh bin/
RUN apt-get update
RUN apt-get install -y libsnappy-dev
CMD ["bin/test.sh"]
