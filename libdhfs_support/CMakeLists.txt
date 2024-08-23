cmake_minimum_required(VERSION 3.24)
project(libdhfs_support CXX)

set(CMAKE_CXX_STANDARD 17)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    if (NOT SANITIZE)
        set(SANITIZE YES)
    endif ()
endif ()

include(CheckCXXCompilerFlag)

if (SANITIZE STREQUAL "YES")
    message(WARNING "Enabling sanitizers!")
    add_compile_options(-Werror -Wall -Wextra -pedantic -Wshadow -Wformat=2 -Wfloat-equal -D_GLIBCXX_DEBUG -Wconversion)
    check_cxx_compiler_flag(-fsanitize-trap=all CAN_TRAP)
    if (CAN_TRAP)
        add_compile_options(-fsanitize=undefined -fsanitize-trap=all -fno-sanitize-recover)
        add_link_options(-fsanitize=undefined -fsanitize-trap=all -fno-sanitize-recover)
    else ()
        message(WARNING "Sanitizers not supported!")
    endif ()
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-flto)
    add_link_options(-flto)
endif ()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O3)
    add_link_options(-O3)
endif ()

add_compile_options(-Wno-unused-parameter)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

find_package(JNI REQUIRED COMPONENTS JVM)
find_package(Java REQUIRED)
include(UseJava)

add_jar(DhfsSupportNative
        "${PROJECT_SOURCE_DIR}/../dhfs-parent/supportlib/src/main/java/com/usatiuk/dhfs/supportlib/DhfsSupportNative.java"
        "${PROJECT_SOURCE_DIR}/LibPathDummy.java"
        GENERATE_NATIVE_HEADERS DhfsSupportNative-native)

add_library(dhfs_support SHARED DhfsSupportNative.cpp)
target_link_libraries(dhfs_support PRIVATE DhfsSupportNative-native)
install(TARGETS dhfs_support LIBRARY DESTINATION "${DHFS_LIB_INSTALL}/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")