cmake_minimum_required(VERSION 4.0.0)

project(SoraTransport)

# boost
list(APPEND CMAKE_PREFIX_PATH "D:/dev/boost_1_90_0/dist/lib/cmake/Boost-1.90.0")

# fltk
include(FetchContent)

set(FLTK_BUILD_TEST OFF CACHE BOOL "" FORCE)
set(FLTK_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(FLTK_BUILD_FLUID OFF CACHE BOOL "" FORCE)
set(FLTK_BUILD_HTML_DOCS OFF CACHE BOOL "" FORCE)
set(FLTK_BUILD_PDF_DOCS OFF CACHE BOOL "" FORCE)
set(FLTK_OPTION_PRINT_SUPPORT OFF CACHE BOOL "" FORCE)
set(FLTK_MSVC_RUNTIME_DLL OFF CACHE BOOL "" FORCE)

# ---- zlib (v1.3.2) ----
# Note: zlib 1.3.2 has a CMake bug where zconf.h.cmakein may not be generated
# before configure_file() is called. Work around it by writing a stub first.
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    zlib
    GIT_REPOSITORY https://github.com/madler/zlib.git
    GIT_TAG        v1.3.2
    GIT_SHALLOW    TRUE
)

# ---- zstd (v1.5.7) ----
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    zstd
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG        v1.5.7
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  build/cmake
)

# ---- libarchive (v3.8.8) ----
set(ENABLE_LZ4 OFF CACHE BOOL "" FORCE)
set(ENABLE_LZO OFF CACHE BOOL "" FORCE)
set(ENABLE_LZMA OFF CACHE BOOL "" FORCE)
set(ENABLE_BZip2 OFF CACHE BOOL "" FORCE)
set(ENABLE_LIBXML2 OFF CACHE BOOL "" FORCE)
set(ENABLE_EXPAT OFF CACHE BOOL "" FORCE)
set(ENABLE_PCREPOSIX OFF CACHE BOOL "" FORCE)
set(ENABLE_LibGCC OFF CACHE BOOL "" FORCE)
set(ENABLE_CNG OFF CACHE BOOL "" FORCE)
set(ENABLE_OPENSSL OFF CACHE BOOL "" FORCE)
set(ENABLE_NETTLE OFF CACHE BOOL "" FORCE)
set(ENABLE_TAR OFF CACHE BOOL "" FORCE)
set(ENABLE_CPIO OFF CACHE BOOL "" FORCE)
set(ENABLE_CAT OFF CACHE BOOL "" FORCE)
set(ENABLE_TEST OFF CACHE BOOL "" FORCE)
set(ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(ENABLE_WERROR OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    LibArchive
    GIT_REPOSITORY https://github.com/libarchive/libarchive.git
    GIT_TAG        v3.8.8
    GIT_SHALLOW    TRUE
)

FetchContent_Declare(
    fltk
    GIT_REPOSITORY https://github.com/fltk/fltk.git
    GIT_TAG release-1.4.4
    GIT_SHALLOW TRUE
)

FetchContent_Declare(
	nlohmann_json
	GIT_REPOSITORY https://github.com/nlohmann/json.git
	GIT_TAG v3.11.3
	GIT_SHALLOW TRUE
)

# Build order: FLTK first (creates "uninstall" target), then zlib+zstd (zstd has guard),
# then libarchive (depends on zlib+zstd).

FetchContent_MakeAvailable(fltk)

FetchContent_MakeAvailable(zlib)
FetchContent_MakeAvailable(zstd)

# Fix zlib v1.3.2 CMakeLists.txt bug: ensure zconf.h.cmakein exists
if(NOT EXISTS "${zlib_BINARY_DIR}/zconf.h.cmakein")
    file(COPY "${zlib_SOURCE_DIR}/zconf.h.in"
         DESTINATION "${zlib_BINARY_DIR}")
    file(RENAME "${zlib_BINARY_DIR}/zconf.h.in"
         "${zlib_BINARY_DIR}/zconf.h.cmakein")
endif()

# Create ALIAS targets so legacy code using ZLIB::ZLIB / zstd::libzstd works
if(TARGET zlibstatic AND NOT TARGET ZLIB::ZLIB)
    add_library(ZLIB::ZLIB ALIAS zlibstatic)
endif()
if(TARGET libzstd_static AND NOT TARGET zstd::libzstd)
    add_library(zstd::libzstd ALIAS libzstd_static)
endif()

# Expose include dirs for libarchive's internal find_package(ZLIB) / find_package(zstd)
# (libarchive uses FindZLIB.cmake / FindZSTD.cmake which look for these variables)
if(TARGET ZLIB::ZLIB)
    get_target_property(_zlib_inc ZLIB::ZLIB INTERFACE_INCLUDE_DIRECTORIES)
    if(_zlib_inc)
        set(ZLIB_INCLUDE_DIR "${_zlib_inc}" CACHE PATH "zlib include dir" FORCE)
    endif()
    get_target_property(_zlib_lib ZLIB::ZLIB IMPORTED_LOCATION)
    if(NOT _zlib_lib)
        set(_zlib_lib zlibstatic)
    endif()
    set(ZLIB_LIBRARY "${_zlib_lib}" CACHE FILEPATH "zlib library" FORCE)
    unset(_zlib_inc)
    unset(_zlib_lib)
endif()

if(TARGET zstd::libzstd)
    get_target_property(_zstd_inc zstd::libzstd INTERFACE_INCLUDE_DIRECTORIES)
    if(_zstd_inc)
        set(ZSTD_INCLUDE_DIR "${_zstd_inc}" CACHE PATH "zstd include dir" FORCE)
    endif()
    get_target_property(_zstd_lib zstd::libzstd IMPORTED_LOCATION)
    if(NOT _zstd_lib)
        set(_zstd_lib libzstd_static)
    endif()
    set(ZSTD_LIBRARY "${_zstd_lib}" CACHE FILEPATH "zstd library" FORCE)
    unset(_zstd_inc)
    unset(_zstd_lib)
endif()

FetchContent_MakeAvailable(LibArchive)

# Create ALIAS for LibArchive
if(TARGET archive_static AND NOT TARGET LibArchive::LibArchive)
    add_library(LibArchive::LibArchive ALIAS archive_static)
endif()

FetchContent_MakeAvailable(nlohmann_json)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
	set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>" CACHE STRING "MSVC runtime library" FORCE)
endif()

if(MSVC)
	add_compile_options(/utf-8)
endif()

write_file("${CMAKE_CURRENT_BINARY_DIR}/.gitignore" "**")

find_package(Boost REQUIRED CONFIG)

# ALIAS targets are already created above (after FetchContent_MakeAvailable calls).

if(TARGET fltk AND NOT TARGET fltk::fltk)
	add_library(fltk::fltk INTERFACE IMPORTED)
	target_link_libraries(fltk::fltk INTERFACE fltk)
endif()

if(TARGET Boost::headers)
	set(SORATRANSPORT_BOOST_TARGET Boost::headers)
elseif(TARGET Boost::boost)
	set(SORATRANSPORT_BOOST_TARGET Boost::boost)
else()
	message(FATAL_ERROR "Boost package was found, but no header target is exported.")
endif()

add_library(soratransport_dependencies INTERFACE)
target_link_libraries(soratransport_dependencies
	INTERFACE
		${SORATRANSPORT_BOOST_TARGET}
		ZLIB::ZLIB
		zstd::libzstd
		LibArchive::LibArchive
		nlohmann_json::nlohmann_json
)

if(WIN32)
	target_link_libraries(soratransport_dependencies
		INTERFACE
			ws2_32
			mswsock
			iphlpapi
	)
endif()

add_library(soratransport_core
	src/core.cpp
	src/core.hpp
	src/detail2/config.cpp
	src/detail2/filesystem.cpp
	src/detail2/infra.cpp
	src/detail2/orchestration.cpp
	src/detail2/stream.cpp
	src/detail2/tar.cpp
	src/detail2/writer.cpp
	src/detail2/zstd.cpp
	src/detail/runtime.cpp
	src/detail/io.cpp
	src/detail/cli.cpp
	src/detail/windows_helpers.cpp
)

target_include_directories(soratransport_core
	PUBLIC
		${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(soratransport_core
	PUBLIC
		soratransport_dependencies
)

target_compile_definitions(soratransport_core
	PUBLIC
		_WIN32_WINNT=0x0A00
		WIN32_LEAN_AND_MEAN
		NOMINMAX
)

add_executable(soratransport
	src/main.cpp
)

target_link_libraries(soratransport
	PRIVATE
		soratransport_core
)

add_executable(fasttar
	src/fasttar.cpp
)

target_link_libraries(fasttar
	PRIVATE
		soratransport_core
)

add_executable(fs_benchmark
	src/fs_benchmark.cpp
)

target_link_libraries(fs_benchmark
	PRIVATE
		soratransport_core
)

add_executable(soratransport_app WIN32
	src/gui_app.cpp
	src/detail2/gui_runtime.cpp
)

target_link_libraries(soratransport_app
	PRIVATE
		soratransport_core
		fltk
)

