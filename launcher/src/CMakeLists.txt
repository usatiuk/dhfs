set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(launcher
        LauncherApp.cpp
)
target_link_libraries(launcher ${wxWidgets_LIBRARIES})
