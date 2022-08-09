FROM ubuntu
MAINTAINER me@gmail.com
COPY build_cmake/test/format/t bin/t
COPY build_cmake/test/format/CONFIG.stress bin/
COPY build_cmake/libwiredtiger.so.10.0.2 lib/libwiredtiger.so.10.0.2
COPY build_cmake/ext/ lib/
ENV LD_LIBRARY_PATH={docker_path}:$LD_LIBRARY_PATH
CMD ["bin/t"]