add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt profiler)

set(CRYPTO_LIBS cryptopp)

if(${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

set(bindir /usr/local/bin)
set(sbindir /usr/local/sbin)
set(libdir /usr/local/lib)
set(sysconfdir /usr/local/etc)
set(pkgdatadir /usr/share)
#double check with someone that pkgdatadir is correct
set(prefix /usr/local)
set(GCOV_PREFIX_STRIP 4)

configure_file(
${CMAKE_SOURCE_DIR}/src/init-ceph.in.cmake
${CMAKE_BINARY_DIR}/init-ceph
@ONLY
)

#add_custom_target(init-ceph SOURCES init-ceph)

configure_file(
${CMAKE_SOURCE_DIR}/src/mkcephfs.in.cmake
${CMAKE_BINARY_DIR}/mkcephfs
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-coverage.in.cmake
${CMAKE_BINARY_DIR}/ceph-coverage
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in.cmake
${CMAKE_BINARY_DIR}/ceph-debugpack
@ONLY
)

add_custom_target(shell_scripts ALL
COMMAND chmod 755 ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph ${CMAKE_BINARY_DIR}/ceph-debugpack ${CMAKE_BINARY_DIR}/ceph-coverage
DEPENDS ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph
COMMENT "Shell Scripts have permission")

add_subdirectory(common)
add_subdirectory(osdc)
add_subdirectory(cls/lock)
add_subdirectory(librados)
add_subdirectory(rgw)
add_subdirectory(mon)
add_subdirectory(os)
add_subdirectory(global)
add_subdirectory(osd)
add_subdirectory(mds)
add_subdirectory(client)
add_subdirectory(test)

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories(".")

#monitor
set(ceph_mon_srcs 
	ceph_mon.cc
	common/TextTable.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-mon ${ceph_mon_srcs})

target_link_libraries(ceph-mon mon boost_thread common os global ${TCMALLOC_LIBS})

# osd
set(ceph_osd_srcs
	ceph_osd.cc 
	objclass/class_debug.cc 
	objclass/class_api.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-osd ${ceph_osd_srcs})

target_link_libraries(ceph-osd osd os global ${TCMALLOC_LIBS})

#mds
set(ceph_mds_srcs
	ceph_mds.cc
	${TCMALLOC_srcs}
)
add_executable(ceph-mds ${ceph_mds_srcs})

target_link_libraries(ceph-mds mds osdc ${CMAKE_DL_LIBS} global ${TCMALLOC_LIBS} boost_thread)

# admin tools

#cephfs
set(cephfs_srcs 
	cephfs.cc
)

add_executable(cephfs ${cephfs_srcs})

target_link_libraries(cephfs common)

# synthetic client
set(ceph_syn_srcs
	ceph_syn.cc 
	client/SyntheticClient.cc
)

add_executable(ceph-syn ${ceph_syn_srcs})
target_link_libraries(ceph-syn client global)

#rados
set(rados_srcs
	rados.cc 
	rados_import.cc 
	rados_export.cc 
	rados_sync.cc 
	common/obj_bencher.cc
)
add_executable(rados ${rados_srcs})
target_link_libraries(rados cls_lock librados global)

#radosgw worry about option logic at a different time
if(${WITH_RADOSGW})
set(radosgw_srcs
	rgw/rgw_resolve.cc 
        rgw/rgw_rest.cc 
        rgw/rgw_rest_swift.cc 
        rgw/rgw_rest_s3.cc 
        rgw/rgw_rest_usage.cc 
        rgw/rgw_rest_user.cc 
        rgw/rgw_rest_bucket.cc 
        rgw/rgw_http_client.cc 
        rgw/rgw_swift.cc 
	rgw/rgw_swift_auth.cc 
	rgw/rgw_main.cc
)

add_executable(radosgw ${radosgw_srcs})

target_link_libraries(radosgw librgw_a librados libcls_rgw_client libcls_lock_client libcls_refcount_client curl expat global fcgi resolv)
install(TARGETS radosgw DESTINATION bin)
endif(${WITH_RADOSGW})

#librados_config_srcs
set(librados_config_srcs
	librados-config.cc 
)

add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados global)


#ceph-conf
set(ceph_conf_srcs
	ceph_conf.cc
)

add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)

#monmaptool
set(monmaptool_srcs
	monmaptool.cc
)

add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)

#crushtool
set(crushtool_srcs
	crushtool.cc
)

add_executable(crushtool ${crushtool_srcs})
target_link_libraries(crushtool global)

#osdmaptool
set(osdomaptool_srcs 
osdmaptool.cc
)

add_executable(osdmaptool  ${osdomaptool_srcs})
target_link_libraries(osdmaptool global)

#ceph-authtool
set(ceph_authtool_srcs
	ceph_authtool.cc
)

add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global)

#ceph-fuse
if(WITH_FUSE)
set(ceph_fuse_srcs
ceph_fuse.cc
client/fuse_ll.cc
)

add_executable(ceph-fuse ${ceph_fuse_srcs})
target_link_libraries(ceph-fuse fuse client global)
set(ceph_fuse_bin "ceph-fuse")
endif(WITH_FUSE)

#ceph_mon_store_converter
set(ceph_mon_store_converter_srcs
mon_store_converter.cc
mon/MonitorStore.cc
)

add_executable(ceph_mon_store_converter ${ceph_mon_store_converter_srcs})
target_link_libraries(ceph_mon_store_converter os global)


# add the install targets
install(TARGETS
  ceph-mon
  ceph-osd
  ceph-mds
  cephfs
  ${ceph_fuse_bin}
  ceph-syn
  rados
  librados-config
  ceph-conf
  monmaptool
  osdmaptool
  crushtool
  ceph-authtool
  ceph_mon_store_converter
  DESTINATION bin)

install(FILES
  ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_BINARY_DIR}/mkcephfs
  ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  DESTINATION bin)
