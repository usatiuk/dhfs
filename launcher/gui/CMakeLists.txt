
find_package(wxWidgets REQUIRED COMPONENTS net core base webview)
if (wxWidgets_USE_FILE) # not defined in CONFIG mode
    include(${wxWidgets_USE_FILE})
endif ()

add_executable(launcher
        src/LauncherApp.cpp
        src/GLauncherApp.cpp
        src/LauncherAppMainFrame.cpp
        src/DhfsGuiInstance.cpp
        src/DhfsGuiInstance.hpp
)

target_link_libraries(launcher ${wxWidgets_LIBRARIES})
target_link_libraries(launcher backend utils)