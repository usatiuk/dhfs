add_executable(DhfsLauncher
        src/LauncherApp.cpp
        src/GLauncherApp.cpp
        src/LauncherAppMainFrame.cpp
        src/DhfsGuiInstance.cpp
        src/DhfsGuiInstance.hpp
        src/DhfsWxServer.cpp
        src/DhfsWxServer.hpp
        src/DhfsWxConnection.cpp
        src/DhfsWxConnection.hpp
)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    target_sources(DhfsLauncher PRIVATE
            src/macos/utils.m
            src/macos/utils.h
    )
endif ()


target_link_libraries(DhfsLauncher ${wxWidgets_LIBRARIES})
target_link_libraries(DhfsLauncher backend utils)

set_target_properties(DhfsLauncher PROPERTIES
        MACOSX_BUNDLE TRUE
        MACOSX_BUNDLE_GUI_IDENTIFIER "com.usatiuk.dhfs.launcher"
        MACOSX_BUNDLE_BUNDLE_NAME "DHFS Launcher"
)