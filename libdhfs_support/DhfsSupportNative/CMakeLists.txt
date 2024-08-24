set(CMAKE_CXX_STANDARD 17)

find_package(JNI REQUIRED COMPONENTS JVM)
find_package(Java REQUIRED)
include(UseJava)

add_jar(DhfsSupportNative
        "${PROJECT_SOURCE_DIR}/../dhfs-parent/supportlib/src/main/java/com/usatiuk/dhfs/supportlib/DhfsSupportNative.java"
        "LibPathDummy.java"
        GENERATE_NATIVE_HEADERS DhfsSupportNative-native
)

add_library(dhfs_support SHARED
        src/DhfsSupportNative.cpp
)

target_compile_options(dhfs_support PRIVATE
        -Wno-unused-parameter
)

target_link_libraries(dhfs_support PRIVATE
        helpers
        DhfsSupportNative-native
)

install(TARGETS dhfs_support LIBRARY DESTINATION "${DHFS_LIB_INSTALL}/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")