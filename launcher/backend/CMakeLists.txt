add_library(backend
        src/DhfsInstance.cpp
        include_public/DhfsInstance.hpp
)

target_include_directories(backend PRIVATE include)
target_include_directories(backend PUBLIC include_public)

target_link_libraries(backend PUBLIC libjvm_wrapper)
