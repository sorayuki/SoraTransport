cmake_minimum_required(VERSION 4.0.0)

if(NOT DEFINED VCPKG_MANIFEST_MODE)
	set(VCPKG_MANIFEST_MODE ON CACHE BOOL "Use the vcpkg manifest in the source tree")
endif()

if(NOT DEFINED VCPKG_MANIFEST_DIR)
	set(VCPKG_MANIFEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}" CACHE PATH "Path to the vcpkg manifest directory")
endif()

if(NOT DEFINED VCPKG_TARGET_TRIPLET)
	set(VCPKG_TARGET_TRIPLET "x64-windows-static" CACHE STRING "vcpkg target triplet")
endif()

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

FetchContent_MakeAvailable(fltk)
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
find_package(ZLIB REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(LibArchive REQUIRED)
# find_package(FLTK CONFIG REQUIRED)

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

