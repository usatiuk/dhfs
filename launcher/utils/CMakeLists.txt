add_library(utils
        src/Exception.cpp
        include_public/Exception.h
)

target_include_directories(utils PRIVATE include)
target_include_directories(utils PUBLIC include_public)