cmake_minimum_required(VERSION 3.24)
project(libdhfs_support CXX)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    if (NOT SANITIZE)
        set(SANITIZE YES)
    endif ()
endif ()

include(CheckCXXCompilerFlag)

if (SANITIZE STREQUAL "YES")
    message(STATUS "Enabling sanitizers!")
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

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

add_subdirectory(helpers)
add_subdirectory(DhfsSupportNative)