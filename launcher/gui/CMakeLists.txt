
find_package(wxWidgets REQUIRED COMPONENTS net core base)
if (wxWidgets_USE_FILE) # not defined in CONFIG mode
    include(${wxWidgets_USE_FILE})
endif ()

add_executable(launcher
        src/LauncherApp.cpp
        src/GLauncherApp.cpp
        src/LauncherAppMainFrame.cpp
)

target_link_libraries(launcher ${wxWidgets_LIBRARIES})
target_link_libraries(launcher backend)