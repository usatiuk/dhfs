# if linux
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    option(wxWidgets_IN_TREE_BUILD "Build wxWidgets in-tree" OFF)
else ()
    option(wxWidgets_IN_TREE_BUILD "Build wxWidgets in-tree" ON)
endif ()

if (wxWidgets_IN_TREE_BUILD)
    message(STATUS "Building wxWidgets in-tree")
    include(FetchContent)
    set(wxBUILD_SHARED OFF)
    FetchContent_Declare(wx
            GIT_REPOSITORY https://github.com/wxWidgets/wxWidgets.git
            GIT_TAG v3.2.8.1
            GIT_SHALLOW TRUE
            GIT_PROGRESS TRUE
    )
    FetchContent_MakeAvailable(wx)
    set(wxWidgets_LIBRARIES wx::core wx::base wx::webview wx::net)
else ()
    message(STATUS "Using system wxWidgets")
    find_package(wxWidgets REQUIRED COMPONENTS net core base webview)
    if (wxWidgets_USE_FILE) # not defined in CONFIG mode
        include(${wxWidgets_USE_FILE})
    endif ()
endif ()