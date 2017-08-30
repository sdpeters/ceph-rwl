// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_SHMLOCK
#define CEPH_LIBRBD_CACHE_SHMLOCK

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/interprocess_upgradable_mutex.hpp>

#include "include/assert.h"



namespace librbd {
namespace cache {

class shm_lck {
struct ShareData {
    boost::interprocess::interprocess_upgradable_mutex umtx_;
    uint64_t ref_cnt = 0;
};

  boost::interprocess::managed_shared_memory seg;
  std::string shm_name;

public:
  std::pair<ShareData *, std::size_t> res;

  shm_lck(const std::string shm_name_):shm_name(shm_name_) {
    open_or_create_shm(shm_name);
    assert(seg.get_size() == 65536);
  }

  ~shm_lck() {
      res.first->ref_cnt -= 1;

      if(res.first->ref_cnt == 0) {
        std::cout << "remove shm" << std::endl;
        boost::interprocess::shared_memory_object::remove(shm_name.c_str());
      }
  }

  void open_or_create_shm(const std::string shm_name) {
    try {
      seg = boost::interprocess::managed_shared_memory(boost::interprocess::open_only, shm_name.c_str());
    } catch (...) {
      std::cout << "create shm" << std::endl;
      seg = boost::interprocess::managed_shared_memory(boost::interprocess::create_only, shm_name.c_str(), 65536);
      seg.construct<ShareData>(boost::interprocess::unique_instance)();
    }
    res = seg.find<ShareData>(boost::interprocess::unique_instance);
    res.first->ref_cnt += 1;
  }

  ShareData* get_shared_data() {
    return res.first;
  }

};

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_SHMLOCK
