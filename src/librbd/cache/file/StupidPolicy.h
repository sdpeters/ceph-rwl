// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
#define CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY

#include "librbd/cache/file/Policy.h"
#include "include/lru.h"
#include "common/Mutex.h"
#include "librbd/cache/file/Types.h"
#include <unordered_map>
#include <vector>

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

/**
 * Stupid LRU-style policy
 */
template <typename ImageCtxT>
class StupidPolicy : public Policy {
public:
  StupidPolicy(ImageCtxT &image_ctx);

  virtual void set_block_count(uint64_t block_count);

  virtual int invalidate(uint64_t block);

  virtual int map(OpType op_type, uint64_t block, bool partial_block,
                  MapResult *map_result, uint64_t *replace_cache_block);
  virtual void tick();

private:

  class Entry : public LRUObject {
  public:
    uint64_t block;
    bool dirty;
    Entry() : block(0), dirty(false) {}
  };

  //typedef std::vector<Entry> Entries;
  typedef std::unordered_map<uint64_t, Entry*> BlockToEntries;

  ImageCtxT &m_image_ctx;

  Mutex m_lock;
  uint64_t m_block_count = 0;

  static const int m_entry_count = 262441;
  Entry m_entries[m_entry_count];
  BlockToEntries m_block_to_entries;

  LRU m_free_lru;
  LRU m_clean_lru;
  LRU m_dirty_lru;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
