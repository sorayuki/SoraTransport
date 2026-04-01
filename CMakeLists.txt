cmake_minimum_required(VERSION 4.0.0)

project(SoraTransport)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SORATRANSPORT_VCPKG_ROOT "D:/devspace/vcpkg" CACHE PATH "Path to the local vcpkg installation")
set(SORATRANSPORT_VCPKG_TRIPLET "x64-windows" CACHE STRING "Triplet used for prebuilt vcpkg dependencies")
set(SORATRANSPORT_VCPKG_INSTALLED_DIR "${SORATRANSPORT_VCPKG_ROOT}/installed/${SORATRANSPORT_VCPKG_TRIPLET}")

if(NOT EXISTS "${SORATRANSPORT_VCPKG_INSTALLED_DIR}")
	message(FATAL_ERROR "Missing vcpkg installed directory: ${SORATRANSPORT_VCPKG_INSTALLED_DIR}")
endif()

write_file("${CMAKE_CURRENT_BINARY_DIR}/.gitignore" "**")

list(PREPEND CMAKE_PREFIX_PATH
	"D:/dev/boost_1_90_0/dist/lib/cmake/Boost-1.90.0"
	"${SORATRANSPORT_VCPKG_INSTALLED_DIR}"
	"${SORATRANSPORT_VCPKG_INSTALLED_DIR}/share/zstd"
)

find_package(Boost REQUIRED CONFIG)
find_package(ZLIB REQUIRED)
find_package(zstd CONFIG REQUIRED)

find_path(LibArchive_INCLUDE_DIR
	NAMES archive.h
	PATHS "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/include"
	NO_DEFAULT_PATH
	REQUIRED
)

find_library(LibArchive_LIBRARY_RELEASE
	NAMES archive
	PATHS "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/lib"
	NO_DEFAULT_PATH
	REQUIRED
)

find_library(LibArchive_LIBRARY_DEBUG
	NAMES archive
	PATHS "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/debug/lib"
	NO_DEFAULT_PATH
	REQUIRED
)

if(NOT TARGET LibArchive::LibArchive)
	add_library(LibArchive::LibArchive SHARED IMPORTED GLOBAL)
	set_target_properties(LibArchive::LibArchive PROPERTIES
		IMPORTED_CONFIGURATIONS "Debug;Release;RelWithDebInfo;MinSizeRel"
		IMPORTED_IMPLIB_DEBUG "${LibArchive_LIBRARY_DEBUG}"
		IMPORTED_IMPLIB_RELEASE "${LibArchive_LIBRARY_RELEASE}"
		IMPORTED_IMPLIB_RELWITHDEBINFO "${LibArchive_LIBRARY_RELEASE}"
		IMPORTED_IMPLIB_MINSIZEREL "${LibArchive_LIBRARY_RELEASE}"
		IMPORTED_LOCATION_DEBUG "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/debug/bin/archive.dll"
		IMPORTED_LOCATION_RELEASE "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/bin/archive.dll"
		IMPORTED_LOCATION_RELWITHDEBINFO "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/bin/archive.dll"
		IMPORTED_LOCATION_MINSIZEREL "${SORATRANSPORT_VCPKG_INSTALLED_DIR}/bin/archive.dll"
		INTERFACE_INCLUDE_DIRECTORIES "${LibArchive_INCLUDE_DIR}"
	)
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
)

find_package(Threads REQUIRED)

if(WIN32)
	target_link_libraries(soratransport_dependencies
		INTERFACE
			ws2_32
			mswsock
	)
endif()

add_library(soratransport_core
	src/core.cpp
	src/core.hpp
	src/detail/runtime.cpp
	src/detail/io.cpp
	src/detail/filesystem.cpp
	src/detail/tar.cpp
	src/detail/zstd.cpp
	src/detail/orchestration.cpp
	src/detail/cli.cpp
)

target_include_directories(soratransport_core
	PUBLIC
		${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(soratransport_core
	PUBLIC
		soratransport_dependencies
		Threads::Threads
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

