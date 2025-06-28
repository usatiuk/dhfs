add_library(backend
        src/DhfsInstance.cpp
        include_public/DhfsInstance.hpp
        src/DhfsStartOptions.cpp
        include_public/DhfsStartOptions.hpp
        src/DhfsWxProcess.cpp
        include_public/DhfsWxProcess.hpp
)

target_include_directories(backend PRIVATE include)
target_include_directories(backend PUBLIC include_public)

target_link_libraries(backend PRIVATE ${wxWidgets_LIBRARIES})
target_link_libraries(backend PUBLIC utils)
