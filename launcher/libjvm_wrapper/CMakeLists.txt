add_library(libjvm_wrapper
        src/LibjvmWrapper.cpp
        include_public/LibjvmWrapper.hpp
)

target_include_directories(libjvm_wrapper PRIVATE include)
target_include_directories(libjvm_wrapper PUBLIC include_public)

find_package(JNI REQUIRED)
target_include_directories(libjvm_wrapper PUBLIC ${JNI_INCLUDE_DIRS})