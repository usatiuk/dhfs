cmake_minimum_required(VERSION 3.10)
project(launcher VERSION 0.1.0)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    enable_language(OBJC)
    enable_language(OBJCXX)
endif ()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

option(DHFS_PACKAGE_USE_PREBUILT "Use prebuilt DHFS server and Web UI artifacts when packaging the launcher bundle" OFF)
set(DHFS_PREBUILT_SERVER_DIR "" CACHE PATH "Path to a prebuilt Quarkus app directory (quarkus-app)")
set(DHFS_PREBUILT_WEBUI_DIR "" CACHE PATH "Path to a prebuilt Web UI distribution directory (e.g. dist)")
set(DHFS_MAVEN_JAVA_HOME "$ENV{JAVA_HOME}" CACHE PATH "Java home used for building DHFS payloads (must be Java 21)")

get_filename_component(DHFS_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(DHFS_PARENT_DIR "${DHFS_SOURCE_ROOT}/dhfs-parent")
set(DHFS_WEBUI_DIR "${DHFS_SOURCE_ROOT}/webui")

set(DHFS_BUNDLE_ROOT "${CMAKE_BINARY_DIR}/bundle")
set(DHFS_STAMP_DIR "${CMAKE_BINARY_DIR}/.dhfs-stamps")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    #    if (NOT DEFINED SANITIZE)
    #        set(SANITIZE YES)
    #    endif ()
endif ()

if (SANITIZE STREQUAL "YES")
    message(STATUS "Enabling sanitizers!")
    add_compile_options(-Werror -O0 -Wall -Wextra -pedantic -Wno-unused-parameter -Wno-unused-variable
            -Wno-error=unused-function
            -Wshadow -Wformat=2 -Wfloat-equal -D_GLIBCXX_DEBUG -Wconversion)
    add_compile_options(-fsanitize=address -fno-sanitize-recover)
    add_link_options(-fsanitize=address -fno-sanitize-recover)
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif ()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O3)
    add_link_options(-O3)
endif ()

include(wxWidgets)

add_subdirectory(utils)
add_subdirectory(backend)
add_subdirectory(gui)

file(MAKE_DIRECTORY "${DHFS_STAMP_DIR}")

set(DHFS_SERVER_PREPARE_TARGET "dhfs_prepare_server")
if (DHFS_PACKAGE_USE_PREBUILT)
    if (DHFS_PREBUILT_SERVER_DIR STREQUAL "")
        message(FATAL_ERROR "DHFS_PREBUILT_SERVER_DIR must be set when DHFS_PACKAGE_USE_PREBUILT is ON")
    endif ()
    if (NOT IS_DIRECTORY "${DHFS_PREBUILT_SERVER_DIR}")
        message(FATAL_ERROR "DHFS_PREBUILT_SERVER_DIR (${DHFS_PREBUILT_SERVER_DIR}) does not exist or is not a directory")
    endif ()
    get_filename_component(DHFS_SERVER_SOURCE_DIR "${DHFS_PREBUILT_SERVER_DIR}" ABSOLUTE)
    add_custom_target(${DHFS_SERVER_PREPARE_TARGET})
else ()
    set(DHFS_SERVER_SOURCE_DIR "${DHFS_PARENT_DIR}/dhfs-fuse/target/quarkus-app")
    set(DHFS_SERVER_BUILD_STAMP "${DHFS_STAMP_DIR}/dhfs_server_build.stamp")
    if (DHFS_MAVEN_JAVA_HOME STREQUAL "")
        find_package(Java 21 REQUIRED COMPONENTS Development)
        if (Java_JAVA_EXECUTABLE)
            get_filename_component(_dhfs_java_bin "${Java_JAVA_EXECUTABLE}" DIRECTORY)
            get_filename_component(_dhfs_java_home "${_dhfs_java_bin}" DIRECTORY)
            set(DHFS_MAVEN_JAVA_HOME "${_dhfs_java_home}" CACHE PATH "Java home used for building DHFS payloads (must be Java 21)" FORCE)
        endif ()
    endif ()
    if (DHFS_MAVEN_JAVA_HOME STREQUAL "")
        message(FATAL_ERROR "Unable to determine Java home for building DHFS artifacts. Set JAVA_HOME or DHFS_MAVEN_JAVA_HOME to a Java 21 installation.")
    endif ()
    if (NOT EXISTS "${DHFS_MAVEN_JAVA_HOME}/bin/java")
        message(FATAL_ERROR "DHFS_MAVEN_JAVA_HOME (${DHFS_MAVEN_JAVA_HOME}) does not contain bin/java")
    endif ()
    set(_DHFS_MAVEN_ENV "JAVA_HOME=${DHFS_MAVEN_JAVA_HOME}")
    add_custom_command(
            OUTPUT "${DHFS_SERVER_BUILD_STAMP}"
            COMMAND "${CMAKE_COMMAND}" -E env
                "${_DHFS_MAVEN_ENV}"
                mvn -f "${DHFS_PARENT_DIR}/pom.xml"
                -pl dhfs-fuse -am
                --batch-mode
                -DskipTests
                -DskipITs
                -Dmaven.test.skip=true
                package
            COMMAND "${CMAKE_COMMAND}" -E touch "${DHFS_SERVER_BUILD_STAMP}"
            WORKING_DIRECTORY "${DHFS_PARENT_DIR}"
            COMMENT "Building DHFS server (Quarkus)"
            VERBATIM
    )
    add_custom_target(${DHFS_SERVER_PREPARE_TARGET} DEPENDS "${DHFS_SERVER_BUILD_STAMP}")
endif ()

set(DHFS_WEBUI_PREPARE_TARGET "dhfs_prepare_webui")
if (DHFS_PACKAGE_USE_PREBUILT)
    if (DHFS_PREBUILT_WEBUI_DIR STREQUAL "")
        message(FATAL_ERROR "DHFS_PREBUILT_WEBUI_DIR must be set when DHFS_PACKAGE_USE_PREBUILT is ON")
    endif ()
    if (NOT IS_DIRECTORY "${DHFS_PREBUILT_WEBUI_DIR}")
        message(FATAL_ERROR "DHFS_PREBUILT_WEBUI_DIR (${DHFS_PREBUILT_WEBUI_DIR}) does not exist or is not a directory")
    endif ()
    get_filename_component(DHFS_WEBUI_SOURCE_DIR "${DHFS_PREBUILT_WEBUI_DIR}" ABSOLUTE)
    add_custom_target(${DHFS_WEBUI_PREPARE_TARGET})
else ()
    set(DHFS_WEBUI_SOURCE_DIR "${DHFS_WEBUI_DIR}/dist")
    set(DHFS_WEBUI_BUILD_STAMP "${DHFS_STAMP_DIR}/dhfs_webui_build.stamp")
    add_custom_command(
            OUTPUT "${DHFS_WEBUI_BUILD_STAMP}"
            COMMAND npm install
            COMMAND npm run build
            COMMAND "${CMAKE_COMMAND}" -E touch "${DHFS_WEBUI_BUILD_STAMP}"
            WORKING_DIRECTORY "${DHFS_WEBUI_DIR}"
            COMMENT "Building DHFS Web UI"
            VERBATIM
    )
    add_custom_target(${DHFS_WEBUI_PREPARE_TARGET} DEPENDS "${DHFS_WEBUI_BUILD_STAMP}")
endif ()

add_custom_target(dhfs_prepare_payload DEPENDS ${DHFS_SERVER_PREPARE_TARGET} ${DHFS_WEBUI_PREPARE_TARGET})

install(CODE [[
    set(_args --build "${CMAKE_BINARY_DIR}" --target dhfs_prepare_payload)
    if(NOT "${CMAKE_INSTALL_CONFIG_NAME}" STREQUAL "")
        list(APPEND _args --config "${CMAKE_INSTALL_CONFIG_NAME}")
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" ${_args})
]])

if (APPLE)
    set(DHFS_APP_CONTENTS "DhfsLauncher.app/Contents")
    set(DHFS_SERVER_INSTALL_DEST "${DHFS_APP_CONTENTS}/Resources/app/Server")
    set(DHFS_WEBUI_INSTALL_DEST "${DHFS_APP_CONTENTS}/Resources/app/Webui")
    install(TARGETS DhfsLauncher BUNDLE DESTINATION . COMPONENT launcher)
else ()
    set(DHFS_SHARE_ROOT "share/dhfs-launcher")
    install(TARGETS DhfsLauncher RUNTIME DESTINATION bin COMPONENT launcher)
    set(DHFS_SERVER_INSTALL_DEST "${DHFS_SHARE_ROOT}/app/Server")
    set(DHFS_WEBUI_INSTALL_DEST "${DHFS_SHARE_ROOT}/app/Webui")
endif ()

install(DIRECTORY "${DHFS_SERVER_SOURCE_DIR}/" DESTINATION "${DHFS_SERVER_INSTALL_DEST}" COMPONENT launcher)
install(DIRECTORY "${DHFS_WEBUI_SOURCE_DIR}/" DESTINATION "${DHFS_WEBUI_INSTALL_DEST}" COMPONENT launcher)

set(DHFS_INSTALL_CONFIG_ARGS "")
if (CMAKE_CONFIGURATION_TYPES)
    set(DHFS_INSTALL_CONFIG_ARGS --config $<CONFIG>)
endif ()

set(DHFS_INSTALL_PREFIX "${DHFS_BUNDLE_ROOT}")
if (NOT APPLE)
    set(DHFS_INSTALL_PREFIX "${DHFS_BUNDLE_ROOT}/dhfs-launcher")
endif ()

add_custom_target(dhfs_package_bundle
        COMMAND "${CMAKE_COMMAND}" -E rm -rf "${DHFS_BUNDLE_ROOT}"
        COMMAND "${CMAKE_COMMAND}" --install "${CMAKE_BINARY_DIR}" ${DHFS_INSTALL_CONFIG_ARGS} --component launcher --prefix "${DHFS_INSTALL_PREFIX}"
        DEPENDS dhfs_prepare_payload DhfsLauncher
        COMMENT "Preparing DHFS launcher bundle"
)

set(CPACK_PACKAGE_NAME "dhfs-launcher")
set(CPACK_PACKAGE_VENDOR "DHFS")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "DHFS Launcher with bundled server and Web UI")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")
set(CPACK_COMPONENTS_ALL launcher)
if (APPLE)
    set(CPACK_GENERATOR "ZIP")
else ()
    set(CPACK_GENERATOR "TGZ;DEB")
endif ()
include(CPack)
