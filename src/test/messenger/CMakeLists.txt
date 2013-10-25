add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lboost_regex")

include_directories("."
  "${PROJECT_SOURCE_DIR}/src/")

set(simple_server_srcs
	simple_server.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)

add_executable(simple_server ${simple_server_srcs})

target_link_libraries(simple_server os global ${TCMALLOC_LIBS})

set(simple_client_srcs
	simple_client.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(simple_client ${simple_client_srcs})

target_link_libraries(simple_client os global ${TCMALLOC_LIBS})
