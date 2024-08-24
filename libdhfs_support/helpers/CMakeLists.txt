set(CMAKE_CXX_STANDARD 17)

add_library(helpers
        include/MemoryHelpers.h
        src/MemoryHelpers.cpp
        include/Utils.h
)

target_include_directories(helpers PUBLIC include)
