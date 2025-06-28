add_executable(launcher
        src/LauncherApp.cpp
        src/GLauncherApp.cpp
        src/LauncherAppMainFrame.cpp
        src/DhfsGuiInstance.cpp
        src/DhfsGuiInstance.hpp
)

target_link_libraries(launcher ${wxWidgets_LIBRARIES})
target_link_libraries(launcher backend utils)

set_target_properties(launcher PROPERTIES
        MACOSX_BUNDLE TRUE
        MACOSX_BUNDLE_GUI_IDENTIFIER "com.usatiuk.dhfs.launcher"
        MACOSX_BUNDLE_BUNDLE_NAME "DHFS Launcher"
)