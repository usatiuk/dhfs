# apt install g++ cmake libfuse3-dev libfuse3-3 fuse3

export CMAKE_BUILD_PARALLEL_LEVEL="$(nproc)"

cd lazyfs

cd libs/libpcache && ./build.sh && cd -

cd lazyfs && ./build.sh && cd -
