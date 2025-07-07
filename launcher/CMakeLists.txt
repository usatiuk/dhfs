cmake_minimum_required(VERSION 3.10)
project(launcher)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    enable_language(OBJC)
    enable_language(OBJCXX)
endif ()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    #    if (NOT DEFINED SANITIZE)
    #        set(SANITIZE YES)
    #    endif ()
endif ()

if (SANITIZE STREQUAL "YES")
    message(STATUS "Enabling sanitizers!")
    add_compile_options(-Werror -O0 -Wall -Wextra -pedantic -Wno-unused-parameter -Wno-unused-variable
            -Wno-error=unused-function
            -Wshadow -Wformat=2 -Wfloat-equal -D_GLIBCXX_DEBUG -Wconversion)
    add_compile_options(-fsanitize=address -fno-sanitize-recover)
    add_link_options(-fsanitize=address -fno-sanitize-recover)
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif ()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O3)
    add_link_options(-O3)
endif ()

include(wxWidgets)

add_subdirectory(utils)
add_subdirectory(backend)
add_subdirectory(gui)
