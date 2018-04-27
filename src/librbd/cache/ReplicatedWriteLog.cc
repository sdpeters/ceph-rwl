// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReplicatedWriteLog.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
			   <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

namespace rwl {

typedef ReplicatedWriteLog<ImageCtx>::Extent Extent;
typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

BlockExtent block_extent(const uint64_t offset_bytes, const uint64_t length_bytes)
{
  return BlockExtent(offset_bytes / MIN_WRITE_SIZE,
		     ((offset_bytes + length_bytes) / MIN_WRITE_SIZE) - 1);
}

BlockExtent block_extent(const Extent& image_extent)
{
  return block_extent(image_extent.first, image_extent.second);
}

Extent image_extent(const BlockExtent& block_extent)
{
  return Extent(block_extent.block_start * MIN_WRITE_SIZE,
		(block_extent.block_end - block_extent.block_start + 1) * MIN_WRITE_SIZE);
}

/* Defer a set of Contexts until destruct/exit */
class DeferredContexts {
public:
  Contexts contexts;
  ~DeferredContexts() {
    finish_contexts(nullptr, contexts, 0);
  }
  void add(Context* ctx) {
    contexts.push_back(ctx);
  }
};

BlockExtent WriteLogPmemEntry::block_extent() {
  return BlockExtent(librbd::cache::rwl::block_extent(image_offset_bytes, write_bytes));
}

BlockExtent WriteLogEntry::block_extent() { return ram_entry.block_extent(); }
void WriteLogEntry::add_reader() { reader_count++; }
void WriteLogEntry::remove_reader() { reader_count--; }

SyncPoint::SyncPoint(CephContext *cct, uint64_t sync_gen_num)
  : m_cct(cct), log_entry(make_shared<SyncPointLogEntry>(sync_gen_num)) {
  m_prior_log_entries_persisted = new C_Gather(cct, nullptr);
  m_sync_point_persist = new C_Gather(cct, nullptr);
  m_on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  m_on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  ldout(cct, 20) << "sync point " << sync_gen_num << dendl;
}

SyncPoint::~SyncPoint() {
}

GenericLogOperation::GenericLogOperation(const utime_t dispatch_time) : m_dispatch_time(dispatch_time) {
}

SyncPointLogOperation::SyncPointLogOperation(shared_ptr<SyncPoint> sync_point,
					     sync_point_appending_callback_t sync_point_appending_callback,
					     sync_complete_callback_t sync_complete_callback,
					     const utime_t dispatch_time)
  : GenericLogOperation(dispatch_time), sync_point(sync_point),
    sync_point_appending_callback(sync_point_appending_callback),
    sync_complete_callback(sync_complete_callback) {
}

SyncPointLogOperation::~SyncPointLogOperation() { }

void SyncPointLogOperation::appending() {
  sync_point_appending_callback(0);
}

void SyncPointLogOperation::complete(int result) {
  sync_complete_callback(result);
}

WriteLogOperation::WriteLogOperation(WriteLogOperationSet &set, uint64_t image_offset_bytes, uint64_t write_bytes)
  : GenericLogOperation(set.m_dispatch_time), m_lock("librbd::cache::rwl::WriteLogOperation::m_lock"),
    log_entry(make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = set.m_extent_ops_appending->new_sub();
  on_write_persist = set.m_extent_ops_persist->new_sub();
  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

WriteLogOperation::~WriteLogOperation() { }

/* Called when the write log operation is appending and its log position is guaranteed */
void WriteLogOperation::appending() {
  Context *on_append = nullptr;
  {
    Mutex::Locker locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }
  if (on_append) {
    on_append->complete(0);
  }
}

/* Called when the write log operation is completed in all log replicas */
void WriteLogOperation::complete(int result) {
  appending();
  Context *on_persist = nullptr;
  {
    Mutex::Locker locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }
  if (on_persist) {
    on_persist->complete(result);
  }
}

WriteLogOperationSet::WriteLogOperationSet(CephContext *cct, utime_t dispatched, shared_ptr<SyncPoint> sync_point,
					   bool persist_on_flush, BlockExtent extent, Context *on_finish)
  : m_cct(cct), m_extent(extent), m_on_finish(on_finish),
    m_persist_on_flush(persist_on_flush), m_dispatch_time(dispatched), sync_point(sync_point) {
  m_on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
  m_on_ops_persist = nullptr;
  m_extent_ops_persist =
    new C_Gather(cct,
		 new FunctionContext( [this](int r) {
		     //ldout(m_cct, 6) << "m_extent_ops_persist completed" << dendl;
		     if (m_on_ops_persist) {
		       m_on_ops_persist->complete(r);
		     }
		     m_on_finish->complete(r);
		   }));
  auto appending_persist_sub = m_extent_ops_persist->new_sub();
  m_extent_ops_appending =
    new C_Gather(cct,
		 new FunctionContext( [this, appending_persist_sub](int r) {
		     //ldout(m_cct, 6) << "m_extent_ops_appending completed" << dendl;
		     m_on_ops_appending->complete(r);
		     appending_persist_sub->complete(r);
		   }));
}

WriteLogOperationSet::~WriteLogOperationSet() { }

GuardedRequestFunctionContext::GuardedRequestFunctionContext(boost::function<void(BlockGuardCell*,bool)> &&callback)
  : m_callback(std::move(callback)){ }

GuardedRequestFunctionContext::~GuardedRequestFunctionContext(void) { }

void GuardedRequestFunctionContext::finish(int r) {
  assert(true == m_callback_invoked);
}

void GuardedRequestFunctionContext::acquired(BlockGuardCell *cell, bool detained) {
  bool initial = false;
  if (m_callback_invoked.compare_exchange_strong(initial, true)) {
    m_callback(cell, detained);
  }
  complete(0);
}

WriteLogMapEntry::WriteLogMapEntry(const BlockExtent block_extent,
				   shared_ptr<WriteLogEntry> log_entry)
  : block_extent(block_extent) , log_entry(log_entry) {
}

WriteLogMapEntry::WriteLogMapEntry(shared_ptr<WriteLogEntry> log_entry)
  : block_extent(log_entry->block_extent()) , log_entry(log_entry) {
}

WriteLogMap::WriteLogMap(CephContext *cct)
  : m_cct(cct), m_lock("librbd::cache::rwl::WriteLogMap::m_lock") {
}

/**
 * Add a write log entry to the map. Subsequent queries for blocks
 * within this log entry's extent will find this log entry. Portions
 * of prior write log entries overlapping with this log entry will
 * be replaced in the map by this log entry.
 *
 * The map_entries field of the log entry object will be updated to
 * contain this map entry.
 *
 * The map_entries fields of all log entries overlapping with this
 * entry will be updated to remove the regions that overlap with
 * this.
 */
void WriteLogMap::add_log_entry(shared_ptr<WriteLogEntry> log_entry) {
  assert(log_entry->ram_entry.is_write());
  Mutex::Locker locker(m_lock);
  add_log_entry_locked(log_entry);
}

void WriteLogMap::add_log_entries(WriteLogEntries &log_entries) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    add_log_entry_locked(log_entry);
  }
}

/**
 * Remove any map entries that refer to the supplied write log
 * entry.
 */
void WriteLogMap::remove_log_entry(shared_ptr<WriteLogEntry> log_entry) {
  if (!log_entry->ram_entry.is_write()) { return; }
  Mutex::Locker locker(m_lock);
  remove_log_entry_locked(log_entry);
}

void WriteLogMap::remove_log_entries(WriteLogEntries &log_entries) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    remove_log_entry_locked(log_entry);
  }
}

/**
 * Returns the list of all write log entries that overlap the specified block
 * extent. This doesn't tell you which portions of these entries overlap the
 * extent, or each other. For that, use find_map_entries(). A log entry may
 * appear in the list more than once, if multiple map entries refer to it
 * (e.g. the middle of that write log entry has been overwritten).
 */
WriteLogEntries WriteLogMap::find_log_entries(BlockExtent block_extent) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_log_entries_locked(block_extent);
}

/**
 * Returns the list of all write log map entries that overlap the
 * specified block extent.
 */
WriteLogMapEntries WriteLogMap::find_map_entries(BlockExtent block_extent) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_map_entries_locked(block_extent);
}

void WriteLogMap::add_log_entry_locked(shared_ptr<WriteLogEntry> log_entry) {
  WriteLogMapEntry map_entry(log_entry);
  ldout(m_cct, 20) << "block_extent=" << map_entry.block_extent
		   << dendl;
  assert(m_lock.is_locked_by_me());
  assert(log_entry->ram_entry.is_write());
  WriteLogMapEntries overlap_entries = find_map_entries_locked(map_entry.block_extent);
  if (overlap_entries.size()) {
    for (auto &entry : overlap_entries) {
      ldout(m_cct, 20) << entry << dendl;
      if (map_entry.block_extent.block_start <= entry.block_extent.block_start) {
	if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
	  ldout(m_cct, 20) << "map entry completely occluded by new log entry" << dendl;
	  remove_map_entry_locked(entry);
	} else {
	  assert(map_entry.block_extent.block_end < entry.block_extent.block_end);
	  /* The new entry occludes the beginning of the old entry */
	  BlockExtent adjusted_extent(map_entry.block_extent.block_end+1,
				      entry.block_extent.block_end);
	  adjust_map_entry_locked(entry, adjusted_extent);
	}
      } else {
	assert(map_entry.block_extent.block_start > entry.block_extent.block_start);
	if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
	  /* The new entry occludes the end of the old entry */
	  BlockExtent adjusted_extent(entry.block_extent.block_start,
				      map_entry.block_extent.block_start-1);
	  adjust_map_entry_locked(entry, adjusted_extent);
	} else {
	  /* The new entry splits the old entry */
	  split_map_entry_locked(entry, map_entry.block_extent);
	}
      }
    }
  }
  add_map_entry_locked(map_entry);
}

void WriteLogMap::remove_log_entry_locked(shared_ptr<WriteLogEntry> log_entry) {
  ldout(m_cct, 20) << "*log_entry=" << *log_entry << dendl;
  assert(m_lock.is_locked_by_me());

  if (!log_entry->ram_entry.is_write()) { return; }
  BlockExtent log_entry_extent(log_entry->block_extent());
  WriteLogMapEntries possible_hits = find_map_entries_locked(log_entry_extent);
  for (auto &possible_hit : possible_hits) {
    if (possible_hit.log_entry == log_entry) {
      /* This map entry refers to the specified log entry */
      remove_map_entry_locked(possible_hit);
    }
  }
}

void WriteLogMap::add_map_entry_locked(WriteLogMapEntry &map_entry)
{
  assert(map_entry.log_entry);
  m_block_to_log_entry_map.insert(map_entry);
  map_entry.log_entry->referring_map_entries++;
}

void WriteLogMap::remove_map_entry_locked(WriteLogMapEntry &map_entry)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  WriteLogMapEntry erased = *it;
  m_block_to_log_entry_map.erase(it);
  erased.log_entry->referring_map_entries--;
  if (0 == erased.log_entry->referring_map_entries) {
    ldout(m_cct, 20) << "log entry has zero map entries: " << erased.log_entry << dendl;
  }
}

void WriteLogMap::adjust_map_entry_locked(WriteLogMapEntry &map_entry, BlockExtent &new_extent)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  WriteLogMapEntry adjusted = *it;
  m_block_to_log_entry_map.erase(it);

  m_block_to_log_entry_map.insert(WriteLogMapEntry(new_extent, adjusted.log_entry));
}

void WriteLogMap::split_map_entry_locked(WriteLogMapEntry &map_entry, BlockExtent &removed_extent)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  WriteLogMapEntry split = *it;
  m_block_to_log_entry_map.erase(it);

  BlockExtent left_extent(split.block_extent.block_start,
			  removed_extent.block_start-1);
  m_block_to_log_entry_map.insert(WriteLogMapEntry(left_extent, split.log_entry));

  BlockExtent right_extent(removed_extent.block_end+1,
			   split.block_extent.block_end);
  m_block_to_log_entry_map.insert(WriteLogMapEntry(right_extent, split.log_entry));

  split.log_entry->referring_map_entries++;
}

WriteLogEntries WriteLogMap::find_log_entries_locked(BlockExtent &block_extent) {
  WriteLogEntries overlaps;
  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;

  assert(m_lock.is_locked_by_me());
  WriteLogMapEntries map_entries = find_map_entries_locked(block_extent);
  for (auto &map_entry : map_entries) {
    overlaps.emplace_back(map_entry.log_entry);
  }
  return overlaps;
}

/**
 * TODO: Generalize this to do some arbitrary thing to each map
 * extent, instead of returning a list.
 */
WriteLogMapEntries WriteLogMap::find_map_entries_locked(BlockExtent &block_extent) {
  WriteLogMapEntries overlaps;

  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  assert(m_lock.is_locked_by_me());
  auto p = m_block_to_log_entry_map.equal_range(WriteLogMapEntry(block_extent));
  ldout(m_cct, 20) << "count=" << std::distance(p.first, p.second) << dendl;
  for ( auto i = p.first; i != p.second; ++i ) {
    WriteLogMapEntry entry = *i;
    overlaps.emplace_back(entry);
    ldout(m_cct, 20) << entry << dendl;
  }
  return overlaps;
}

/* We map block extents to write log entries, or portions of write log
 * entries. These are both represented by a WriteLogMapEntry. When a
 * WriteLogEntry is added to this map, a WriteLogMapEntry is created to
 * represent the entire block extent of the WriteLogEntry, and the
 * WriteLogMapEntry is added to the set.
 *
 * The set must not contain overlapping WriteLogMapEntrys. WriteLogMapEntrys
 * in the set that overlap with one being added are adjusted (shrunk, split,
 * or removed) before the new entry is added.
 *
 * This comparison works despite the ambiguity because we ensure the set
 * contains no overlapping entries. This comparison works to find entries
 * that overlap with a given block extent because equal_range() returns the
 * first entry in which the extent doesn't end before the given extent
 * starts, and the last entry for which the extent starts before the given
 * extent ends (the first entry that the key is less than, and the last entry
 * that is less than the key).
 */
bool WriteLogMap::WriteLogMapEntryCompare::operator()(const WriteLogMapEntry &lhs,
						      const WriteLogMapEntry &rhs) const {
  if (lhs.block_extent.block_end < rhs.block_extent.block_start) {
    return true;
  }
  return false;
}

WriteLogMapEntry WriteLogMap::block_extent_to_map_key(BlockExtent &block_extent) {
  return WriteLogMapEntry(block_extent);
}

bool is_block_aligned(const Extent &extent) {
  if (extent.first % MIN_WRITE_SIZE != 0 || extent.second % MIN_WRITE_SIZE != 0) {
    return false;
  }
  return true;
}

bool is_block_aligned(const Extents &image_extents) {
  for (auto &extent : image_extents) {
    if (!is_block_aligned(extent)) {
      return false;
    }
  }
  return true;
}

/**
 * A request that can be deferred in a BlockGuard to sequence
 * overlapping operations.
 */
struct C_GuardedBlockIORequest : public Context {
protected:
  CephContext *m_cct;
private:
  BlockGuardCell* m_cell = nullptr;
public:
  C_GuardedBlockIORequest(CephContext *cct)
    : m_cct(cct) {
    ldout(m_cct, 99) << this << dendl;
  }
  ~C_GuardedBlockIORequest() {
    ldout(m_cct, 99) << this << dendl;
  }
  C_GuardedBlockIORequest(const C_GuardedBlockIORequest&) = delete;
  C_GuardedBlockIORequest &operator=(const C_GuardedBlockIORequest&) = delete;

  virtual void send() = 0;
  virtual const char *get_name() const = 0;
  void set_cell(BlockGuardCell *cell) {
    ldout(m_cct, 20) << this << dendl;
    assert(cell);
    m_cell = cell;
  }
  BlockGuardCell *get_cell(void) {
    ldout(m_cct, 20) << this << dendl;
    return m_cell;
  }
};

} // namespace rwl

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(ImageCtx &image_ctx, ImageCache<I> *lower)
  : m_image_ctx(image_ctx),
    m_log_pool_size(DEFAULT_POOL_SIZE),
    m_image_writeback(lower), m_write_log_guard(image_ctx.cct),
    m_entry_reader_lock("librbd::cache::ReplicatedWriteLog::m_entry_reader_lock"),
    m_deferred_dispatch_lock("librbd::cache::ReplicatedWriteLog::m_deferred_dispatch_lock",
			     false, true, true, image_ctx.cct),
    m_log_append_lock("librbd::cache::ReplicatedWriteLog::m_log_append_lock",
		      false, true, true, image_ctx.cct),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock",
	   false, true, true, image_ctx.cct),
    m_blockguard_lock("librbd::cache::ReplicatedWriteLog::m_blockguard_lock",
	   false, true, true, image_ctx.cct),
    m_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_persist_finisher", "pfin_rwl"),
    m_log_append_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_log_append_finisher", "afin_rwl"),
    m_on_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_on_persist_finisher", "opfin_rwl"),
    m_blocks_to_log_entries(image_ctx.cct),
    m_timer_lock("librbd::cache::ReplicatedWriteLog::m_timer_lock",
	   false, true, true, image_ctx.cct),
    m_timer(image_ctx.cct, m_timer_lock, false) {
  assert(lower);
  if (use_finishers) {
    m_persist_finisher.start();
    m_log_append_finisher.start();
    m_on_persist_finisher.start();
  }
  m_timer.init();
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  ldout(m_image_ctx.cct, 6) << "enter" << dendl;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.shutdown();
    ldout(m_image_ctx.cct, 15) << "acquiring locks that shouldn't still be held" << dendl;
    RWLock::WLocker reader_locker(m_entry_reader_lock);
    Mutex::Locker dispatch_locker(m_deferred_dispatch_lock);
    Mutex::Locker append_locker(m_log_append_lock);
    Mutex::Locker locker(m_lock);
    ldout(m_image_ctx.cct, 15) << "gratuitous locking complete" << dendl;
    delete m_image_writeback;
    m_image_writeback = nullptr;
    assert(m_deferred_ios.size() == 0);
    assert(m_ops_to_flush.size() == 0);
    assert(m_ops_to_append.size() == 0);
    assert(m_flush_ops_in_flight == 0);
    assert(m_unpublished_reserves == 0);
  }
  ldout(m_image_ctx.cct, 6) << "exit" << dendl;
}

template <typename ExtentsType>
class ExtentsSummary {
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;
  uint64_t first_block;
  uint64_t last_block;
  friend std::ostream &operator<<(std::ostream &os,
				  const ExtentsSummary &s) {
    os << "total_bytes=" << s.total_bytes << ", "
       << "first_image_byte=" << s.first_image_byte << ", "
       << "last_image_byte=" << s.last_image_byte << ", "
       << "first_block=" << s.first_block << ", "
       << "last_block=" << s.last_block<< "";
    return os;
  };
  ExtentsSummary(const ExtentsType &extents) {
    total_bytes = 0;
    first_image_byte = 0;
    last_image_byte = 0;
    first_block = 0;
    last_block = 0;
    if (extents.empty()) return;
    first_image_byte = extents.front().first;
    last_image_byte = first_image_byte + extents.front().second;
    for (auto &extent : extents) {
      total_bytes += extent.second;
      if (extent.first < first_image_byte) {
	first_image_byte = extent.first;
      }
      if ((extent.first + extent.second) > last_image_byte) {
	last_image_byte = extent.first + extent.second;
      }
    }
    first_block = first_image_byte / MIN_WRITE_SIZE;
    last_block = last_image_byte / MIN_WRITE_SIZE;
  }
};

struct ImageExtentBuf : public Extent {
public:
  buffer::raw *m_buf;
  ImageExtentBuf(Extent extent, buffer::raw *buf = nullptr)
    : Extent(extent), m_buf(buf) {}
};
typedef std::vector<ImageExtentBuf> ImageExtentBufs;

struct C_ReadRequest : public Context {
  CephContext *m_cct;
  Context *m_on_finish;
  Extents m_miss_extents; // move back to caller
  ImageExtentBufs m_read_extents;
  bufferlist m_miss_bl;
  bufferlist *m_out_bl;
  utime_t m_arrived_time;
  PerfCounters *m_perfcounter;

  C_ReadRequest(CephContext *cct, utime_t arrived, PerfCounters *perfcounter, bufferlist *out_bl, Context *on_finish)
    : m_cct(cct), m_on_finish(on_finish), m_out_bl(out_bl),
      m_arrived_time(arrived), m_perfcounter(perfcounter) {
    ldout(m_cct, 99) << this << dendl;
  }
  ~C_ReadRequest() {
    ldout(m_cct, 99) << this << dendl;
  }

  virtual void finish(int r) override {
    ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
    int hits = 0;
    int misses = 0;
    int hit_bytes = 0;
    int miss_bytes = 0;
    if (r >= 0) {
      /*
       * At this point the miss read has completed. We'll iterate through
       * m_read_extents and produce *m_out_bl by assembling pieces of m_miss_bl
       * and the individual hit extent bufs in the read extents that represent
       * hits.
       */
      uint64_t miss_bl_offset = 0;
      for (auto &extent : m_read_extents) {
	if (extent.m_buf) {
	  /* This was a hit */
	  ++hits;
	  hit_bytes += extent.second;
	  bufferlist hit_extent_bl;
	  hit_extent_bl.append(extent.m_buf);
	  m_out_bl->claim_append(hit_extent_bl);
	} else {
	  /* This was a miss. */
	  ++misses;
	  miss_bytes += extent.second;
	  bufferlist miss_extent_bl;
	  miss_extent_bl.substr_of(m_miss_bl, miss_bl_offset, extent.second);
	  /* Add this read miss bullerlist to the output bufferlist */
	  m_out_bl->claim_append(miss_extent_bl);
	  /* Consume these bytes in the read miss bufferlist */
	  miss_bl_offset += extent.second;
	}
      }
    }
    ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << " bl=" << *m_out_bl << dendl;
    utime_t now = ceph_clock_now();
    m_on_finish->complete(r);
    m_perfcounter->inc(l_librbd_rwl_rd_bytes, hit_bytes + miss_bytes);
    m_perfcounter->inc(l_librbd_rwl_rd_hit_bytes, hit_bytes);
    m_perfcounter->tinc(l_librbd_rwl_rd_latency, now - m_arrived_time);
    if (!misses) {
      m_perfcounter->inc(l_librbd_rwl_rd_hit_req, 1);
      m_perfcounter->tinc(l_librbd_rwl_rd_hit_latency, now - m_arrived_time);
    } else {
      if (hits) {
	m_perfcounter->inc(l_librbd_rwl_rd_part_hit_req, 1);
      }
    }
  }

  virtual const char *get_name() const {
    return "C_ReadRequest";
  }
};

template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents &&image_extents, bufferlist *bl,
				 int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  utime_t now = ceph_clock_now();
  C_ReadRequest *read_ctx = new C_ReadRequest(cct, now, m_perfcounter, bl, on_finish);
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
		<< "bl=" << bl << ", "
		<< "on_finish=" << on_finish << dendl;

  bl->clear();
  m_perfcounter->inc(l_librbd_rwl_rd_req, 1);

  // TODO: Handle unaligned IO:
  // - Inflate read extents to block bounds, and submit onward
  // - On read completion, discard leading/trailing buffer to match unaligned request
  if (!is_block_aligned(image_extents)) {
    lderr(cct) << "unaligned read fails" << dendl;
    for (auto &extent : image_extents) {
      lderr(cct) << "start: " << extent.first << " length: " << extent.second << dendl;
    }
    on_finish->complete(-EINVAL);
    return;
  }

  // TODO handle fadvise flags

  /*
   * The strategy here is to look up all the WriteLogMapEntries that overlap
   * this read, and iterate through those to separate this read into hits and
   * misses. A new Extents object is produced here with Extents for each miss
   * region. The miss Extents is then passed on to the read cache below RWL. We
   * also produce an ImageExtentBufs for all the extents (hit or miss) in this
   * read. When the read from the lower cache layer completes, we iterate
   * through the ImageExtentBufs and insert buffers for each cache hit at the
   * appropriate spot in the bufferlist returned from below for the miss
   * read. The buffers we insert here refer directly to regions of various
   * write log entry data buffers.
   *
   * TBD: Locking. These buffer objects hold a reference on those
   * write log entries to prevent them from being retired from the log
   * while the read is completing. The WriteLogEntry references are
   * released by the buffer destructor.
   */
  for (auto &extent : image_extents) {
    uint64_t extent_offset = 0;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));
    for (auto &entry : map_entries) {
      Extent entry_image_extent(image_extent(entry.block_extent));
      /* If this map entry starts after the current image extent offset ... */
      if (entry_image_extent.first > extent.first + extent_offset) {
	/* ... add range before map_entry to miss extents */
	uint64_t miss_extent_start = extent.first + extent_offset;
	uint64_t miss_extent_length = entry_image_extent.first - miss_extent_start;
	Extent miss_extent(miss_extent_start, miss_extent_length);
	read_ctx->m_miss_extents.push_back(miss_extent);
	/* Add miss range to read extents */
	ImageExtentBuf miss_extent_buf(miss_extent);
	read_ctx->m_read_extents.push_back(miss_extent_buf);
	extent_offset += miss_extent_length;
      }
      assert(entry_image_extent.first <= extent.first + extent_offset);
      uint64_t entry_offset = 0;
      /* If this map entry starts before the current image extent offset ... */
      if (entry_image_extent.first < extent.first + extent_offset) {
	/* ... compute offset into log entry for this read extent */
	entry_offset = (extent.first + extent_offset) - entry_image_extent.first;
      }
      /* This read hit ends at the end of the extent or the end of the log
	 entry, whichever is less. */
      uint64_t entry_hit_length = min(entry_image_extent.second - entry_offset,
				      extent.second - extent_offset);
      Extent hit_extent(entry_image_extent.first, entry_hit_length);
      /* Offset of the map entry into the log entry's buffer */
      uint64_t map_entry_buffer_offset = entry_image_extent.first - entry.log_entry->ram_entry.image_offset_bytes;
      /* Offset into the log entry buffer of this read hit */
      uint64_t read_buffer_offset = map_entry_buffer_offset + entry_offset;
      /* Create buffer object referring to pmem pool for this read hit */
      shared_ptr<WriteLogEntry> log_entry = entry.log_entry;
      ldout(cct, 20) << "adding reader: log_entry=" << *log_entry << dendl;
      log_entry->add_reader();
      m_async_op_tracker.start_op();
      buffer::raw *hit_buf =
	buffer::claim_buffer(entry_hit_length,
			     (char*)(log_entry->pmem_buffer + read_buffer_offset),
			     make_deleter([this, log_entry]
					  {
					    CephContext *cct = m_image_ctx.cct;
					    ldout(cct, 20) << "removing reader: log_entry="
							  << *log_entry << dendl;
					    log_entry->remove_reader();
					    m_async_op_tracker.finish_op();
					  }));
      /* Add hit extent to read extents */
      ImageExtentBuf hit_extent_buf(hit_extent, hit_buf);
      read_ctx->m_read_extents.push_back(hit_extent_buf);
      /* Exclude RWL hit range from buffer and extent */
      extent_offset += entry_hit_length;
      ldout(cct, 20) << entry << dendl;
    }
    /* If the last map entry didn't consume the entire image extent ... */
    if (extent.second > extent_offset) {
      /* ... add the rest of this extent to miss extents */
      uint64_t miss_extent_start = extent.first + extent_offset;
      uint64_t miss_extent_length = extent.second - extent_offset;
      Extent miss_extent(miss_extent_start, miss_extent_length);
      read_ctx->m_miss_extents.push_back(miss_extent);
      /* Add miss range to read extents */
      ImageExtentBuf miss_extent_buf(miss_extent);
      read_ctx->m_read_extents.push_back(miss_extent_buf);
      extent_offset += miss_extent_length;
    }
  }

  ldout(cct, 20) << "miss_extents=" << read_ctx->m_miss_extents << ", "
		<< "miss_bl=" << read_ctx->m_miss_bl << dendl;

  if (read_ctx->m_miss_extents.empty()) {
    /* All of this read comes from RWL */
    read_ctx->complete(0);
  } else {
    /* Pass the read misses on to the layer below RWL */
    m_image_writeback->aio_read(std::move(read_ctx->m_miss_extents), &read_ctx->m_miss_bl, fadvise_flags, read_ctx);
  }
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_helper(GuardedRequest &req)
{
  CephContext *cct = m_image_ctx.cct;
  BlockGuardCell *cell;

  assert(m_blockguard_lock.is_locked_by_me());
  ldout(cct, 20) << dendl;

  int r = m_write_log_guard.detain({req.first_block_num, req.last_block_num},
				   &req, &cell);
  assert(r>=0);
  if (r > 0) {
    ldout(cct, 20) << "detaining guarded request due to in-flight requests: "
		   << "start=" << req.first_block_num << ", "
		   << "end=" << req.last_block_num << dendl;
    return nullptr;
  }

  ldout(cct, 20) << "in-flight request cell: " << cell << dendl;
  return cell;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_barrier_helper(GuardedRequest &req)
{
  BlockGuardCell *cell = nullptr;

  assert(m_blockguard_lock.is_locked_by_me());
  ldout(m_image_ctx.cct, 20) << dendl;

  if (m_barrier_in_progress) {
    req.detained = true;
    m_awaiting_barrier.push_back(req);
  } else {
    bool barrier = req.barrier;
    if (barrier) {
      m_barrier_in_progress = true;
      req.current_barrier = true;
    }
    cell = detain_guarded_request_helper(req);
    if (barrier) {
      /* Only non-null if the barrier acquires the guard now */
      m_barrier_cell = cell;
    }
  }

  return cell;
}

template <typename I>
void ReplicatedWriteLog<I>::detain_guarded_request(GuardedRequest &&req)
{
  BlockGuardCell *cell = nullptr;

  ldout(m_image_ctx.cct, 20) << dendl;
  {
    Mutex::Locker locker(m_blockguard_lock);
    cell = detain_guarded_request_barrier_helper(req);
  }
  if (cell) {
    req.on_guard_acquire->acquired(cell, req.detained);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *released_cell)
{
  CephContext *cct = m_image_ctx.cct;
  WriteLogGuard::BlockOperations block_reqs;
  WriteLogGuard::BlockOperations unblocked_reqs;
  struct acquired_req {
    BlockGuardCell *cell;
    GuardedRequest& req;

    acquired_req(BlockGuardCell *cell, GuardedRequest& req)
      : cell(cell), req(req) { };
  };
  std::list<acquired_req> block_reqs_acquired;
  ldout(cct, 20) << "released_cell=" << released_cell << dendl;

  {
    Mutex::Locker locker(m_blockguard_lock);
    m_write_log_guard.release(released_cell, &block_reqs);

    for (auto &req : block_reqs) {
      req.detained = true;
      BlockGuardCell *detained_cell = detain_guarded_request_helper(req);
      if (detained_cell) {
	if (req.current_barrier) {
	  /* The current barrier is acquiring the block guard, so now we know its cell */
	  m_barrier_cell = detained_cell;
	  assert(detained_cell != released_cell);
	  ldout(cct, 20) << "current barrier cell=" << detained_cell << " req=" << req << dendl;
	}
	ldout(cct, 20) << "acquire deferred for cell=" << detained_cell << " req=" << req << dendl;
	block_reqs_acquired.push_back(acquired_req(detained_cell, req));
      }
    }

    if (m_barrier_in_progress && (released_cell == m_barrier_cell)) {
      ldout(cct, 20) << "current barrier released cell=" << released_cell << dendl;
      /* The released cell is the current barrier request */
      m_barrier_in_progress = false;
      m_barrier_cell = nullptr;
      /* Move waiting requests into the blockguard. Stop if there's another barrier */
      while (!m_barrier_in_progress && !m_awaiting_barrier.empty()) {
	unblocked_reqs.splice(unblocked_reqs.end(),
			      m_awaiting_barrier,
			      m_awaiting_barrier.begin());
	auto &req = unblocked_reqs.back();
	ldout(cct, 20) << "submitting queued request to blockguard: " << req << dendl;
	BlockGuardCell *detained_cell = detain_guarded_request_barrier_helper(req);
	if (detained_cell) {
	  ldout(cct, 20) << "acquire deferred for cell=" << detained_cell << " req=" << req << dendl;
	  block_reqs_acquired.push_back(acquired_req(detained_cell, req));
	}
      }
    }
  }

  /* Do acquire work outside blockguard lock */
  for (auto &acquired : block_reqs_acquired) {
    acquired.req.on_guard_acquire->acquired(acquired.cell, acquired.req.detained);
  }

  ldout(cct, 20) << "exit" << dendl;
}

struct WriteBufferAllocation {
  unsigned int allocation_size = 0;
  pobj_action buffer_alloc_action;
  TOID(uint8_t) buffer_oid = OID_NULL;
};

struct WriteRequestResources {
  bool allocated = false;
  std::vector<WriteBufferAllocation> buffers;
};

/**
 * This is the custodian of the BlockGuard cell for this IO, and the
 * state information about the progress of this IO. This object lives
 * until the IO is persisted in all (live) log replicas.  User request
 * may be completed from here before the IO persists.
 */
typedef boost::function<bool(C_BlockIORequest*)> io_alloc_resources_callback_t;
typedef boost::function<void(C_BlockIORequest*)> io_deferred_callback_t;
typedef boost::function<void(C_BlockIORequest*)> io_dispatch_callback_t;
struct C_BlockIORequest : public C_GuardedBlockIORequest {
  Extents m_image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  Context *_on_finish = nullptr; /* Block guard release */
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_on_finish_completed = {false};
  ExtentsSummary<Extents> m_image_extents_summary;
  utime_t m_arrived_time;
  utime_t m_allocated_time; // When allocation began
  utime_t m_dispatched_time; // When dispatch began
  utime_t m_user_req_completed_time;
  bool m_detained = false;
  bool m_deferred = false;
  io_alloc_resources_callback_t m_io_alloc_resources_callback;
  io_deferred_callback_t m_io_deferred_callback;
  io_dispatch_callback_t m_io_dispatch_callback;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_BlockIORequest &req) {
    os << "m_image_extents=[" << req.m_image_extents << "], "
       << "m_image_extents_summary=[" << req.m_image_extents_summary << "], "
       << "bl=" << req.bl << ", "
       << "user_req=" << req.user_req << ", "
       << "m_user_req_completed=" << req.m_user_req_completed << ", "
       << "deferred=" << req.m_deferred << ", "
       << "detained=" << req.m_detained << "";
    return os;
  };
  C_BlockIORequest(CephContext *cct, const utime_t arrived, Extents &&image_extents,
		   bufferlist&& bl, const int fadvise_flags, Context *user_req,
		   io_alloc_resources_callback_t io_alloc_resources_callback,
		   io_deferred_callback_t io_deferred_callback,
		   io_dispatch_callback_t io_dispatch_callback)
    : C_GuardedBlockIORequest(cct), m_image_extents(std::move(image_extents)),
      bl(std::move(bl)), fadvise_flags(fadvise_flags),
      user_req(user_req), m_image_extents_summary(m_image_extents), m_arrived_time(arrived),
      m_io_alloc_resources_callback(io_alloc_resources_callback),
      m_io_deferred_callback(io_deferred_callback),
      m_io_dispatch_callback(io_dispatch_callback) {
    ldout(m_cct, 99) << this << dendl;
  }

  virtual ~C_BlockIORequest() {
    ldout(m_cct, 99) << this << dendl;
  }

  void complete_user_request(int r) {
    bool initial = false;
    if (m_user_req_completed.compare_exchange_strong(initial, true)) {
      ldout(m_cct, 15) << this << " completing user req" << dendl;
      m_user_req_completed_time = ceph_clock_now();
      user_req->complete(r);
    } else {
      ldout(m_cct, 20) << this << " user req already completed" << dendl;
    }
  }

  virtual void send() override {
    /* Should never be called */
    ldout(m_cct, 2) << this << " unexpected" << dendl;
  }

  virtual void finish(int r) {
    ldout(m_cct, 20) << this << dendl;

    complete_user_request(r);
    bool initial = false;
    if (m_on_finish_completed.compare_exchange_strong(initial, true)) {
      ldout(m_cct, 15) << this << " completing _on_finish" << dendl;
      _on_finish->complete(0);
    } else {
      ldout(m_cct, 20) << this << " _on_finish already completed" << dendl;
      assert(0);
    }
  }

  bool alloc_resources() {
    return m_io_alloc_resources_callback(this);
  }

  void deferred() {
    m_deferred = true;
    m_io_deferred_callback(this);
  }

  void dispatch() {
    m_io_dispatch_callback(this);
  }

  virtual const char *get_name() const override {
    return "C_BlockIORequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this write. Block
 * guard is not released until the write persists everywhere (this is
 * how we guarantee to each log replica that they will never see
 * overlapping writes).
 */
struct C_WriteRequest : public C_BlockIORequest {
  WriteRequestResources m_resources;
  unique_ptr<WriteLogOperationSet> m_op_set = nullptr;
  bool m_do_early_flush = false;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_WriteRequest &req) {
    os << (C_BlockIORequest&)req
       << " m_resources.allocated=" << req.m_resources.allocated;
    if (req.m_op_set) {
       os << "m_op_set=" << *req.m_op_set;
    }
    return os;
  };

  C_WriteRequest(CephContext *cct, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req,
		 io_alloc_resources_callback_t io_alloc_resources_callback,
		 io_deferred_callback_t io_deferred_callback,
		 io_dispatch_callback_t io_dispatch_callback)
    : C_BlockIORequest(cct, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req,
		       io_alloc_resources_callback, io_deferred_callback, io_dispatch_callback) {
    ldout(m_cct, 99) << this << dendl;
  }

  ~C_WriteRequest() {
    ldout(m_cct, 99) << this << dendl;
  }

  const char *get_name() const override {
    return "C_WriteRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this
 * aio_flush. Block guard is released as soon as the new
 * sync point (if required) is created. Subsequent IOs can
 * proceed while this flush waits for prio IOs to complete
 * and any required sync points to be persisted.
 */
struct C_FlushRequest : public C_BlockIORequest {
  std::atomic<bool> m_log_entry_allocated = {false};
  bool m_internal = false;
  shared_ptr<SyncPoint> to_append;
  shared_ptr<SyncPointLogOperation> op;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_FlushRequest &req) {
    os << (C_BlockIORequest&)req
       << " m_log_entry_allocated=" << req.m_log_entry_allocated;
    return os;
  };

  C_FlushRequest(CephContext *cct, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req,
		 io_alloc_resources_callback_t io_alloc_resources_callback,
		 io_deferred_callback_t io_deferred_callback,
		 io_dispatch_callback_t io_dispatch_callback)
    : C_BlockIORequest(cct, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req,
		       io_alloc_resources_callback, io_deferred_callback, io_dispatch_callback) {
    ldout(m_cct, 99) << this << dendl;
  }

  ~C_FlushRequest() {
  }

  const char *get_name() const override {
    return "C_FlushRequest";
  }
};

const unsigned long int ops_appended_together = MAX_ALLOC_PER_TRANSACTION;
/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void)
{
  GenericLogOperations ops;
  int append_result = 0;
  bool ops_remain = false;
  do {
    {
      ops.clear();

      {
	Mutex::Locker locker(m_lock);
	if (m_ops_to_append.size()) {
	  auto last_in_batch = m_ops_to_append.begin();
	  unsigned int ops_to_append = m_ops_to_append.size();
	  if (ops_to_append > ops_appended_together) {
	    ops_to_append = ops_appended_together;
	  }
	  std::advance(last_in_batch, ops_to_append);
	  ops.splice(ops.begin(), m_ops_to_append, m_ops_to_append.begin(), last_in_batch);
	  ops_remain = !m_ops_to_append.empty();
	  //ldout(m_image_ctx.cct, 20) << "appending " << ops.size() << ", " << m_ops_to_append.size() << " remain" << dendl;
	} else {
	  ops_remain = false;
	}
      }

      if (ops.size()) {
	Mutex::Locker locker(m_log_append_lock);
	alloc_op_log_entries(ops);
	append_result = append_op_log_entries(ops);
      }
    }

    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      complete_op_log_entries(std::move(ops), append_result);
    }
  } while (ops_remain);
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperations &ops)
{
  std::vector<shared_ptr<GenericLogOperation>> appending;
  bool need_finisher;

  /* Prepare copy of ops list to mark appending after the input list is moved
   * to m_ops_to_append */
  appending.reserve(ops.size());
  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));

  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_append.empty();
    m_ops_to_append.splice(m_ops_to_append.end(), std::move(ops));
  }

  if (need_finisher) {
    m_async_append_ops++;
    m_async_op_tracker.start_op();
    Context *append_ctx = new FunctionContext([this](int r) {
	append_scheduled_ops();
	m_async_append_ops--;
	m_async_op_tracker.finish_op();
      });
    if (use_finishers) {
      m_log_append_finisher.queue(append_ctx);
    } else {
      m_image_ctx.op_work_queue->queue(append_ctx);
    }
  }

  for (auto op : appending) {
    op->appending();
  }
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperations ops;
  bool ops_remain = false;
  do {
    {
      ops.clear();
      Mutex::Locker locker(m_lock);
      if (m_ops_to_flush.size()) {
	auto last_in_batch = m_ops_to_flush.begin();
	unsigned int ops_to_flush = m_ops_to_flush.size();
	if (ops_to_flush > ops_flushed_together) {
	  ops_to_flush = ops_flushed_together;
	}
	//ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
	std::advance(last_in_batch, ops_to_flush);
	ops.splice(ops.begin(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
	ops_remain = !m_ops_to_flush.empty();
	//ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", " << m_ops_to_flush.size() << " remain" << dendl;
      }
    }

    /* Ops subsequently scheduled for flush may finish before these,
     * which is fine. We're unconcerned with completion order until we
     * get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      schedule_append(ops);
    }
  } while (ops_remain);
  append_scheduled_ops();
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperations &ops)
{
  bool need_finisher;
  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), ops);
  }

  if (need_finisher) {
    m_async_flush_ops++;
    m_async_op_tracker.start_op();
    Context *flush_ctx = new FunctionContext([this](int r) {
	flush_then_append_scheduled_ops();
	m_async_flush_ops--;
	m_async_op_tracker.finish_op();
      });
    if (use_finishers) {
      m_persist_finisher.queue(flush_ctx);
    } else {
      m_image_ctx.op_work_queue->queue(flush_ctx);
    }
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_pmem_buffer(GenericLogOperations &ops)
{
  for (auto &operation : ops) {
    if (operation->is_write()) {
      operation->m_buf_persist_time = ceph_clock_now();
      auto write_entry = operation->get_write_log_entry();

      pmemobj_flush(m_log_pool, write_entry->pmem_buffer, write_entry->ram_entry.write_bytes);
    }
  }

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->is_write()) {
      operation->m_buf_persist_comp_time = now;
    } else {
      //ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
    }
  }
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  /* Allocate the (already reserved) log entries */
  Mutex::Locker locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = m_first_free_entry;
    m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;
    operation->get_log_entry()->log_entry_index = entry_index;
    auto &log_entry = operation->get_log_entry();
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    //ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  }
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperations &ops)
{
  if (ops.empty()) return;

  if (ops.size() > 1) {
    assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  /*
  ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
			     << "start address=" << ops.front()->get_log_entry()->pmem_entry << " "
			     << "bytes=" << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
			     << dendl;
  */
  pmemobj_flush(m_log_pool,
		ops.front()->get_log_entry()->pmem_entry,
		ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry)));
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperations &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperations entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;
  utime_t now = ceph_clock_now();

  assert(m_log_append_lock.is_locked_by_me());

  if (ops.empty()) return 0;

  /* Write log entries to ring and persist */
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
	  operation->get_log_entry()->log_entry_index) {
	/*
	ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
				   << "operation=[" << *operation << "]" << dendl;
	*/
	flush_op_log_entries(entries_to_flush);
	entries_to_flush.clear();
	now = ceph_clock_now();
      }
    }
    /*
    ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
			       << operation->get_log_entry()->log_entry_index << " "
			       << "from " << &operation->get_log_entry()->ram_entry << " "
			       << "to " << operation->get_log_entry()->pmem_entry << " "
			       << "operation=[" << *operation << "]" << dendl;
    */
    /*
    ldout(m_image_ctx.cct, 05) << "APPENDING: index="
			       << operation->get_log_entry()->log_entry_index << " "
			       << "operation=[" << *operation << "]" << dendl;
    */
    operation->m_log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    entries_to_flush.push_back(operation);
  }
  flush_op_log_entries(entries_to_flush);

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  TX_BEGIN(m_log_pool) {
    D_RW(pool_root)->first_free_entry = m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->is_write()) {
	auto write_op = (shared_ptr<WriteLogOperation>&) operation;
	pmemobj_tx_publish(write_op->buffer_alloc_action, 1);
      } else {
	//ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size() << " log entries (" << m_log_pool_name << ")" << dendl;
    assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  now = ceph_clock_now();
  for (auto &operation : ops) {
    operation->m_log_append_comp_time = now;
  }

  return ret;
}

/*
 * Complete a set of write ops with the result of append_op_entries.
 */
template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(GenericLogOperations&& ops, int result)
{
  m_async_complete_ops++;
  m_async_op_tracker.start_op();
  Context *complete_ctx = new FunctionContext([this, ops, result](int r) {
      GenericLogEntries dirty_entries;
      int published_reserves = 0;
      for (auto &op : ops) {
	utime_t now = ceph_clock_now();
	bool is_write = op->is_write();
	auto log_entry = op->get_log_entry();
	log_entry->completed = true;
	if (is_write) {
	  op->get_write_log_entry()->sync_point_entry->m_writes_completed++;
	  published_reserves++;
	  dirty_entries.push_back(log_entry);
	}
	op->complete(result);
	if (is_write) {
	  m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_buf_t, op->m_buf_persist_time - op->m_dispatch_time);
	}
	m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_app_t, op->m_log_append_time - op->m_dispatch_time);
	m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_cmp_t, now - op->m_dispatch_time);
	if (is_write) {
	  utime_t buf_lat = op->m_buf_persist_comp_time - op->m_buf_persist_time;
	  m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_bufc_t, buf_lat);
	  m_perfcounter->hinc(l_librbd_rwl_log_op_buf_to_bufc_t_hist, buf_lat.to_nsec(),
			      log_entry->ram_entry.write_bytes);
	  m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_app_t, op->m_log_append_time - op->m_buf_persist_time);
	}
	utime_t app_lat = op->m_log_append_comp_time - op->m_log_append_time;
	m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_appc_t, app_lat);
	m_perfcounter->hinc(l_librbd_rwl_log_op_app_to_appc_t_hist, app_lat.to_nsec(),
			    log_entry->ram_entry.write_bytes);
	m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_cmp_t, now - op->m_log_append_time);
      }

      {
	Mutex::Locker locker(m_lock);
	m_unpublished_reserves -= published_reserves;
	m_dirty_log_entries.splice(m_dirty_log_entries.end(), dirty_entries);

	/* New entries may be flushable */
	wake_up();
      }

      m_async_complete_ops--;
      m_async_op_tracker.finish_op();
    });
  if (use_finishers) {
    m_on_persist_finisher.queue(complete_ctx);
  } else {
    m_image_ctx.op_work_queue->queue(complete_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::complete_write_req(C_WriteRequest *write_req, int result)
{
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 15) << "write_req=" << write_req << " cell=" << write_req->get_cell() << dendl;
  assert(write_req->get_cell());
  if (!write_req->m_op_set->m_persist_on_flush) {
    write_req->complete_user_request(result);
  }
  /* Completed to caller by here */
  utime_t now = ceph_clock_now();
  release_write_lanes(write_req);
  release_guarded_request(write_req->get_cell());
  m_perfcounter->tinc(l_librbd_rwl_req_arr_to_all_t, write_req->m_allocated_time - write_req->m_arrived_time);
  m_perfcounter->tinc(l_librbd_rwl_req_all_to_dis_t, write_req->m_dispatched_time - write_req->m_allocated_time);
  m_perfcounter->tinc(l_librbd_rwl_req_arr_to_dis_t, write_req->m_dispatched_time - write_req->m_arrived_time);
  utime_t comp_latency = now - write_req->m_arrived_time;
  m_perfcounter->tinc(l_librbd_rwl_wr_latency, comp_latency);
  m_perfcounter->hinc(l_librbd_rwl_wr_latency_hist, comp_latency.to_nsec(),
		      write_req->m_image_extents_summary.total_bytes);
  m_perfcounter->tinc(l_librbd_rwl_wr_caller_latency, write_req->m_user_req_completed_time - write_req->m_arrived_time);
}

/**
 * Attempts to allocate log resources for a write. Returns true if successful.
 *
 * Resources include 1 lane per extent, 1 log entry per extent, and the payload
 * data space for each extent.
 *
 * Lanes are released after the write persists via release_write_lanes()
 */
template <typename I>
bool ReplicatedWriteLog<I>::alloc_write_resources(C_WriteRequest *write_req)
{
  bool alloc_succeeds = true;
  utime_t now = ceph_clock_now();

  assert(!m_lock.is_locked_by_me());
  assert(!write_req->m_resources.allocated);
  write_req->m_resources.buffers.reserve(write_req->m_image_extents.size());
  {
    Mutex::Locker locker(m_lock);
    if (m_free_lanes < write_req->m_image_extents.size()) {
      ldout(m_image_ctx.cct, 20) << "not enough free lanes (need "
				 <<  write_req->m_image_extents.size()
				 << ", have " << m_free_lanes << ") "
				 << *write_req << dendl;
      return false;
    }
    if (m_free_log_entries < write_req->m_image_extents.size()) {
      ldout(m_image_ctx.cct, 20) << "not enough free entries (need "
				 <<  write_req->m_image_extents.size()
				 << ", have " << m_free_log_entries << ") "
				 << *write_req << dendl;
      return false;
    }
  }
  for (auto &extent : write_req->m_image_extents) {
    write_req->m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = write_req->m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    if (extent.second > buffer.allocation_size) {
      buffer.allocation_size = extent.second;
    }
    buffer.buffer_oid = pmemobj_reserve(m_log_pool,
					&buffer.buffer_alloc_action,
					buffer.allocation_size,
					0 /* Object type */);
    if (TOID_IS_NULL(buffer.buffer_oid)) {
      ldout(m_image_ctx.cct, 5) << "can't allocate all data buffers: "
				<< pmemobj_errormsg() << ". "
				<< *write_req << dendl;
      alloc_succeeds = false;
      write_req->m_resources.buffers.pop_back();
      break;
    }
    ldout(m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo <<
      "." << buffer.buffer_oid.oid.off << ", size=" << buffer.allocation_size << dendl;
  }

  if (alloc_succeeds) {
    unsigned int num_extents = write_req->m_image_extents.size();
    Mutex::Locker locker(m_lock);
    /* We need one free log entry per extent (each is a separate entry), and
     * one free "lane" for remote replication. */
    if ((m_free_lanes >= num_extents) &&
	(m_free_log_entries >= num_extents)) {
      m_free_lanes -= num_extents;
      m_free_log_entries -= num_extents;
      m_unpublished_reserves += num_extents;
      write_req->m_resources.allocated = true;
    } else {
      alloc_succeeds = false;
    }
  }

  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : write_req->m_resources.buffers) {
      pmemobj_cancel(m_log_pool, &buffer.buffer_alloc_action, 1);
    }
    write_req->m_resources.buffers.clear();
  }

  write_req->m_allocated_time = now;
  return alloc_succeeds;
}

/**
 * Dispatch as many deferred writes as possible
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_deferred_writes(void)
{
  C_BlockIORequest *req = nullptr;
  bool allocated = false;

  do {
    {
      Mutex::Locker deferred_dispatch(m_deferred_dispatch_lock);
      {
	Mutex::Locker locker(m_lock);
	if (m_deferred_ios.size()) {
	  req = m_deferred_ios.front();
	  m_deferred_ios.pop_front();
	  /* If there are more, wake up the work function to help process these */
	  if (m_deferred_ios.size()) {
	    ldout(m_image_ctx.cct, 6) << "deferred IOs: " << m_deferred_ios.size() << dendl;
	    wake_up();
	  }
	} else {
	  req = nullptr;
	}
      }
      if (req) {
	allocated = req->alloc_resources();
	if (!allocated) {
	  Mutex::Locker locker(m_lock);
	  m_deferred_ios.push_front(req);
	  req = nullptr;
	}
      } else {
	allocated = false;
      }
    }
    if (allocated) {
      // TODO: punt to a workq if we've already dispatched one here?
      req->dispatch();
    }
  } while (allocated);
}

/**
 * Returns the lanes used by this write, and attempts to dispatch the next
 * deferred write
 */
template <typename I>
void ReplicatedWriteLog<I>::release_write_lanes(C_WriteRequest *write_req)
{
  {
    Mutex::Locker locker(m_lock);
    assert(write_req->m_resources.allocated);
    m_free_lanes += write_req->m_image_extents.size();
    write_req->m_resources.allocated = false;
  }
  dispatch_deferred_writes();
}

/**
 * Attempts to allocate log resources for a write. Write is dispatched if
 * resources are available, or queued if they aren't.
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_and_dispatch_io_req(C_BlockIORequest *req)
{
  bool dispatch_here = false;

  {
    /* If there are already deferred writes, queue behind them for resources */
    {
      Mutex::Locker locker(m_lock);
      dispatch_here = m_deferred_ios.empty();
    }
    if (dispatch_here) {
      dispatch_here = req->alloc_resources();
    }
    if (dispatch_here) {
      req->dispatch();
    } else {
      req->deferred();
      {
	Mutex::Locker locker(m_lock);
	m_deferred_ios.push_back(req);
      }
      ldout(m_image_ctx.cct, 6) << "deferred IOs: " << m_deferred_ios.size() << dendl;
      dispatch_deferred_writes();
    }
  }
}

/**
 * Takes custody of write_req. Resources must already be allocated.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_aio_write(C_WriteRequest *write_req)
{
  CephContext *cct = m_image_ctx.cct;
  WriteLogEntries log_entries;
  DeferredContexts on_exit;
  utime_t now = ceph_clock_now();
  write_req->m_dispatched_time = now;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

  ldout(cct, 15) << "write_req=" << write_req << " cell=" << write_req->get_cell() << dendl;

  {
    uint64_t buffer_offset = 0;
    Mutex::Locker locker(m_lock);
    Context *set_complete = write_req;
    if (use_finishers) {
      set_complete = new C_OnFinisher(write_req, &m_on_persist_finisher);
    }
    if ((!m_persist_on_flush && m_current_sync_point->log_entry->m_writes_completed) ||
	(m_current_sync_point->log_entry->m_writes > MAX_WRITES_PER_SYNC_POINT) ||
	(m_current_sync_point->log_entry->m_bytes > MAX_BYTES_PER_SYNC_POINT)) {
      /* Create new sync point and persist the previous one. This sequenced
       * write will bear a sync gen number shared with no already completed
       * writes. A group of sequenced writes may be safely flushed concurrently
       * if they all arrived before any of them completed.
       */
      flush_new_sync_point(nullptr, on_exit.contexts);
    }
    write_req->m_op_set =
      make_unique<WriteLogOperationSet>(cct, now, m_current_sync_point, m_persist_on_flush,
					BlockExtent(write_req->m_image_extents_summary.first_block,
						    write_req->m_image_extents_summary.last_block),
					set_complete);
    assert(write_req->m_resources.allocated);
    auto allocation = write_req->m_resources.buffers.begin();
    for (auto &extent : write_req->m_image_extents) {
      /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
      shared_ptr<WriteLogOperation> operation =
	make_shared<WriteLogOperation>(*write_req->m_op_set, extent.first, extent.second);
      write_req->m_op_set->operations.emplace_back(operation);
      log_entries.emplace_back(operation->log_entry);
      m_perfcounter->inc(l_librbd_rwl_log_ops, 1);

      operation->log_entry->ram_entry.has_data = 1;
      operation->log_entry->ram_entry.write_data = allocation->buffer_oid;
      // TODO: make shared_ptr
      operation->buffer_alloc_action = &allocation->buffer_alloc_action;
      assert(!TOID_IS_NULL(operation->log_entry->ram_entry.write_data));
      operation->log_entry->pmem_buffer = D_RW(operation->log_entry->ram_entry.write_data);
      operation->log_entry->ram_entry.sync_gen_number = m_current_sync_gen;
      if (write_req->m_op_set->m_persist_on_flush) {
	/* Persist on flush. Sequence #0 is never used. */
	operation->log_entry->ram_entry.write_sequence_number = 0;
      } else {
	/* Persist on write */
	operation->log_entry->ram_entry.write_sequence_number = ++m_last_op_sequence_num;
	operation->log_entry->ram_entry.sequenced = 1;
      }
      operation->log_entry->ram_entry.sync_point = 0;
      operation->log_entry->ram_entry.unmap = 0;
      operation->bl.substr_of(write_req->bl, buffer_offset,
			      operation->log_entry->ram_entry.write_bytes);
      buffer_offset += operation->log_entry->ram_entry.write_bytes;
      ldout(cct, 20) << "operation=[" << *operation << "]" << dendl;
      allocation++;
    }
  }

  m_async_write_req_finish++;
  m_async_op_tracker.start_op();
  write_req->_on_finish =
    new FunctionContext([this, write_req](int r) {
	complete_write_req(write_req, r);
	m_async_write_req_finish--;
	m_async_op_tracker.finish_op();
      });

  /* All extent ops subs created */
  write_req->m_op_set->m_extent_ops_appending->activate();
  write_req->m_op_set->m_extent_ops_persist->activate();

  /* Write data */
  for (auto &operation : write_req->m_op_set->operations) {
    auto write_op = (shared_ptr<WriteLogOperation>&) operation;
    bufferlist::iterator i(&write_op->bl);
    m_perfcounter->inc(l_librbd_rwl_log_op_bytes, write_op->log_entry->ram_entry.write_bytes);
    //ldout(cct, 20) << write_op->bl << dendl;
    i.copy((unsigned)write_op->log_entry->ram_entry.write_bytes, (char*)write_op->log_entry->pmem_buffer);
  }

  m_blocks_to_log_entries.add_log_entries(log_entries);

  /*
   * Entries are added to m_log_entries in alloc_op_log_entries() when their
   * order is established. They're added to m_dirty_log_entries when the write
   * completes to all replicas (they must not be flushed before then, and
   * shouldn't be read until then either).
   */

  if (write_req->m_op_set->m_persist_on_flush) {
    /*
     * We're done with the caller's buffer, and not guaranteeing
     * persistence until the next flush. The block guard for this
     * write_req will not be released until the write is persisted
     * everywhere, but the caller's request can complete now.
     */
    write_req->complete_user_request(0);
  }

  /* We may schedule append here, or when the prior sync point persists. */
  Context *schedule_append_ctx = new FunctionContext(
     [this, write_req](int r) {
       if (write_req->m_do_early_flush) {
	 /* This caller is waiting for persist, so we'll use their thread to
	  * expedite it */
	 flush_pmem_buffer(write_req->m_op_set->operations);
	 schedule_append(write_req->m_op_set->operations);
       } else {
	 /* This is probably not still the caller's thread, so do the
	  * payload flushing/replicating later. */
	 schedule_flush_and_append(write_req->m_op_set->operations);
       }
     });
  Mutex::Locker locker(m_lock);
  if (!write_req->m_op_set->m_persist_on_flush &&
      write_req->m_op_set->sync_point->earlier_sync_point) {
    write_req->m_do_early_flush = false;
    write_req->m_op_set->sync_point->earlier_sync_point->m_on_sync_point_appending.push_back(schedule_append_ctx);
  } else {
    /* The prior sync point is done, so we'll schedule append here */
    write_req->m_do_early_flush =
      !(write_req->m_detained || write_req->m_deferred || write_req->m_op_set->m_persist_on_flush);
    on_exit.contexts.push_back(schedule_append_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
				      bufferlist&& bl,
				      int fadvise_flags,
				      Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_wr_req, 1);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  // TODO: Handle unaligned IO.
  //
  // - Inflate write extent to block bounds
  // - Submit inflated write to blockguard
  // - After blockguard dispatch reads through RWL for each unaligned extent edge
  // - Reads complete to a gather
  // - On read completion, write request is adjusted to block bounds using read data
  // - Dispatch write request normally, appending completion context to discard read buffers
  if (!is_block_aligned(image_extents)) {
    lderr(cct) << "unaligned write fails" << dendl;
    for (auto &extent : image_extents) {
      lderr(cct) << "start: " << extent.first << " length: " << extent.second << dendl;
    }
    on_finish->complete(-EINVAL);
    return;
  }

  C_WriteRequest *write_req =
    new C_WriteRequest(cct, now, std::move(image_extents), std::move(bl), fadvise_flags, on_finish,
		       [this](C_BlockIORequest* req)->bool {
			 C_WriteRequest *write_req = (C_WriteRequest*)req;
			 ldout(m_image_ctx.cct, 20) << "req type=" << write_req->get_name()
						   << "req=[" << *write_req << "]" << dendl;
			 return alloc_write_resources(write_req);
		       },
		       [this](C_BlockIORequest* req) {
			 m_perfcounter->inc(l_librbd_rwl_wr_req_def, 1);
		       },
		       [this](C_BlockIORequest* req) {
			 C_WriteRequest *write_req = (C_WriteRequest*)req;
			 dispatch_aio_write(write_req);
		       });
  m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req](BlockGuardCell *cell, bool detained) {
      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 20) << "write_req=" << write_req << " cell=" << cell << dendl;

      assert(cell);
      write_req->m_detained = detained;
      write_req->set_cell(cell);
      if (detained) {
	m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
      }
      alloc_and_dispatch_io_req(write_req);
    });

  detain_guarded_request(GuardedRequest(write_req->m_image_extents_summary.first_block,
					write_req->m_image_extents_summary.last_block,
					guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
					bool skip_partial_discard, Context *on_finish) {
  Extent discard_extent = {offset, length};
  Extent adjusted_discard_extent;
  m_perfcounter->inc(l_librbd_rwl_discard, 1);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
		 << "length=" << length << ", "
		 << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  /* We get adjusted_discard_extent adjusted to the enclosing block bounds (by
   * converting from image extent to block extant and back). We'll use
   * adjusted_discard_extent for the block guard, and the invalidate from RWL.
   * We'll pass the original discard_extent down to the image. */
  if (!is_block_aligned(discard_extent)) {
    ldout(cct, 20) << "aligning discard to block size" << dendl;
    adjusted_discard_extent = image_extent(block_extent(discard_extent));
  } else {
    adjusted_discard_extent = discard_extent;
  }


  // TBD: Discard without flushing. Append a discard entry to the log, and
  // put the entry in the map. On read, extents that match discard entries are
  // zero filled with bufferlist::append_zero(). Don't send discard onward
  // until that entry flushes.
  //
  // TBD: When we do flush the discard entry, do we really want to preserve the
  // skip_partial_discard flag supplied here? If that flag is set, do we know
  // here what the effect of the discard wil be (what all reads to these
  // extents will return)? If we don't know that, and we complete reads to
  // these extents before the discard flushes, we'll need to ensure that all
  // reads to these extents from the image return zeros. That may mean
  // skip_partial has to be false. It might mean we have to preceed the discard
  // with writes of zeros so the regions not actually discarded will return
  // zero if read.

  // Temporary strategy: flush RWL, invalidate discarded region, then send
  // discard down to the next layer (another cache or the image). We will not
  // append a discard entry to the log (which would produce zeros for all reads
  // to that extent). The invalidate will append an invalidate entry to the
  // log, which will cause erads to that extent to be treated as misses. This
  // guarantees all reads of the discarded region will always return the same
  // (possibly unpredictable) content.
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext(
      [this, skip_partial_discard, on_finish, discard_extent, adjusted_discard_extent](BlockGuardCell *cell, bool detained) {
	CephContext *cct = m_image_ctx.cct;
	ldout(cct, 6) << "discard_extent=" << discard_extent << " "
		      << "adjusted_discard_extent=" << adjusted_discard_extent << " "
		      << "cell=" << cell << dendl;

	assert(cell);

	Context *ctx = new FunctionContext(
	  [this, cell, on_finish](int r) {
	    on_finish->complete(r);
	    release_guarded_request(cell);
	  });
	ctx = new FunctionContext(
	  [this, skip_partial_discard, on_finish, discard_extent, adjusted_discard_extent, ctx](int r) {
	    Context *next_ctx = ctx;
	    if (r < 0) {
	      /* Override on_finish status with this error */
	      next_ctx = new FunctionContext([r, ctx](int _r) {
		  ctx->complete(r);
		});
	    }
	    /* Invalidate from caches below */
	    m_image_writeback->aio_discard(discard_extent.first, discard_extent.second,
					   skip_partial_discard, next_ctx);
	  });
	ctx = new FunctionContext(
	  [this, skip_partial_discard, on_finish, discard_extent, adjusted_discard_extent, ctx](int r) {
	    Context *next_ctx = ctx;
	    if (r < 0) {
	      /* Override on_finish status with this error */
	      next_ctx = new FunctionContext([r, ctx](int _r) {
		  ctx->complete(r);
		});
	    }
	    /* Invalidate from RWL */
	    invalidate({adjusted_discard_extent}, next_ctx);
	  });
	flush(ctx);
    });

  ldout(cct, 6) << "discard_extent=" << discard_extent << " "
		<< "adjusted_discard_extent=" << adjusted_discard_extent << dendl;
  BlockExtent discard_block_extent(block_extent(adjusted_discard_extent));
  detain_guarded_request(GuardedRequest(discard_block_extent.block_start,
					discard_block_extent.block_end,
					guarded_ctx));
}

template <typename I>
C_FlushRequest* ReplicatedWriteLog<I>::make_flush_req(Context *on_finish) {
  utime_t flush_begins = ceph_clock_now();
  bufferlist bl;

  C_FlushRequest *flush_req =
    new C_FlushRequest(m_image_ctx.cct, flush_begins, Extents({whole_volume_extent()}),
		       std::move(bl), 0, on_finish,
		       [this](C_BlockIORequest* req)->bool {
			 C_FlushRequest *flush_req = (C_FlushRequest*)req;
			 ldout(m_image_ctx.cct, 20) << "req type=" << flush_req->get_name() << " "
						    << "req=[" << *flush_req << "]" << dendl;
			 assert(!flush_req->m_log_entry_allocated);
			 bool allocated_here = false;
			 Mutex::Locker locker(m_lock);
			 if (m_free_log_entries) {
			   m_free_log_entries--;
			   flush_req->m_log_entry_allocated = true;
			   allocated_here = true;
			 }
			 return allocated_here;
		       },
		       [this](C_BlockIORequest* req) {
			 m_perfcounter->inc(l_librbd_rwl_aio_flush_def, 1);
		       },
		       [this](C_BlockIORequest* req) {
			 utime_t now = ceph_clock_now();
			 C_FlushRequest *flush_req = (C_FlushRequest*)req;
			 ldout(m_image_ctx.cct, 20) << "req type=" << flush_req->get_name() << " "
			 << "req=[" << *flush_req << "]" << dendl;
			 assert(flush_req->m_log_entry_allocated);
			 flush_req->m_dispatched_time = now;

			 /* Handler for sync point appending */
			 sync_complete_callback_t appending_cb =
			 [this, flush_req](int result) {
			   std::vector<Context*> on_append;
			   ldout(m_image_ctx.cct, 20) << "Sync point op appending for "
			   << "flush req=[" << *flush_req << "]" << dendl;
			   {
			     Mutex::Locker locker(m_lock);
			     flush_req->to_append->m_appending = true;
			     on_append.swap(flush_req->to_append->m_on_sync_point_appending);
			   }
			   finish_contexts(m_image_ctx.cct, on_append, result);
			 };

			 /* Handler for sync point persist complete */
			 sync_complete_callback_t comp_cb =
			 [this, flush_req](int result) {
			   ldout(m_image_ctx.cct, 20) << "Sync point op completed for "
			   << "flush req=[" << *flush_req << "]" << dendl;
			   {
			     Mutex::Locker locker(m_lock);
			     /* Remove link from next sync point */
			     assert(flush_req->to_append->later_sync_point);
			     assert(flush_req->to_append->later_sync_point->earlier_sync_point ==
				    flush_req->to_append);
			     flush_req->to_append->later_sync_point->earlier_sync_point = nullptr;
			     // TODO: only on debug
			     // int found_self = 0;
			     // for (auto ctx : flush_req->to_append->m_on_sync_point_persisted) {
			     //   if (ctx == flush_req) {
			     //		 found_self++;
			     //   }
			     // }
			     // assert(found_self == 1);
			   }
			   /* Catch late-arriving on-appending work */
			   flush_req->op->appending();
			   /* This flush request will be one of these contexts */
			   finish_contexts(m_image_ctx.cct,
					   flush_req->to_append->m_on_sync_point_persisted, result);
			 };

			 flush_req->op =
			 make_shared<SyncPointLogOperation>(flush_req->to_append, appending_cb, comp_cb, now);

			 m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
			 GenericLogOperations ops;
			 ops.push_back(flush_req->op);
			 schedule_append(ops);
		       });

  flush_req->_on_finish = new FunctionContext(
    [this, flush_req](int r) {
      ldout(m_image_ctx.cct, 6) << "flush_req=" << flush_req
				<< " cell=" << flush_req->get_cell() << dendl;
      assert(!flush_req->get_cell());
      flush_req->complete_user_request(r);

      /* Completed to caller by here */
      utime_t now = ceph_clock_now();
      m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - flush_req->m_arrived_time);

      /* Block guard already released */
    });

  return flush_req;
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point(C_FlushRequest *flush_req, Contexts &later) {
  assert(m_lock.is_locked_by_me());

  if (!flush_req) {
    m_async_null_flush_finish++;
    m_async_op_tracker.start_op();
    Context *flush_ctx = new FunctionContext([this](int r) {
	m_async_null_flush_finish--;
	m_async_op_tracker.finish_op();
      });
    flush_req = make_flush_req(flush_ctx);
    flush_req->m_internal = true;
  }

  /* Add a new sync point. */
  new_sync_point(later);
  assert(m_current_sync_point->earlier_sync_point);

  /* This flush request will append/persist the (now) previous sync point */
  flush_req->to_append = m_current_sync_point->earlier_sync_point;
  flush_req->to_append->m_append_scheduled = true;

  /* All prior sync points that are still in this list must already be scheduled for append */
  shared_ptr<SyncPoint> previous = flush_req->to_append->earlier_sync_point;
  while (previous) {
    assert(previous->m_append_scheduled);
    previous = previous->earlier_sync_point;
  }

  /* When the m_sync_point_persist Gather completes this sync point can be
   * appended.  The only sub for this Gather is the finisher Context for
   * m_prior_log_entries_persisted, which records the result of the Gather in
   * the sync point, and completes. TODO: Do we still need both of these
   * Gathers?*/
  flush_req->to_append->m_sync_point_persist->
    set_finisher(new FunctionContext([this, flush_req](int r) {
	  ldout(m_image_ctx.cct, 20) << "Flush req=" << flush_req
				     << " sync point =" << flush_req->to_append
				     << ". Ready to persist." << dendl;
	  alloc_and_dispatch_io_req(flush_req);
	}));

  /* The m_sync_point_persist Gather has all the subs it will ever have, and
   * now has its finisher. If the sub is already complete, activation will
   * complete the Gather. The finisher will acquire m_lock, so we'll activate
   * this when we release m_lock.*/
  later.push_back(new FunctionContext([this, flush_req](int r) {
	flush_req->to_append->m_sync_point_persist->activate();
      }));
  flush_req->to_append->m_on_sync_point_persisted.push_back(flush_req);
}

/**
 * Aio_flush completes when all previously completed writes are
 * flushed to persistent cache. We make a best-effort attempt to also
 * defer until all in-progress writes complete, but we may not know
 * about all of the writes the application considers in-progress yet,
 * due to uncertainty in the IO submission workq (multiple WQ threads
 * may allow out-of-order submission).
 *
 * This flush operation will not wait for writes deferred for overlap
 * in the block guard.
 */
template <typename I>
void ReplicatedWriteLog<I>::aio_flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << dendl;
  m_perfcounter->inc(l_librbd_rwl_aio_flush, 1);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  auto flush_req = make_flush_req(on_finish);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, flush_req](BlockGuardCell *cell, bool detained) {
      ldout(m_image_ctx.cct, 20) << "flush_req=" << flush_req << " cell=" << cell << dendl;
      assert(cell);
      flush_req->m_detained = detained;
      /* We don't call flush_req->set_cell(), because the block guard will be released here */
      if (detained) {
	//m_perfcounter->inc(l_librbd_rwl_aio_flush_overlap, 1);
      }
      {
	DeferredContexts post_unlock; /* Do these when the lock below is released */
	Mutex::Locker locker(m_lock);

	if (!m_flush_seen) {
	  ldout(m_image_ctx.cct, 5) << "flush seen" << dendl;
	  m_flush_seen = true;
	  if (!m_persist_on_flush && m_persist_on_write_until_flush) {
	    m_persist_on_flush = true;
	    ldout(m_image_ctx.cct, 5) << "now persisting on flush" << dendl;
	  }
	}

	/*
	 * Create a new sync point if there have been writes since the last
	 * one.
	 *
	 * We do not flush the caches below the RWL here.
	 */
	/* If there have been writes since the last sync point ... */
	if (m_current_sync_point->log_entry->m_writes) {
	  flush_new_sync_point(flush_req, post_unlock.contexts);
	} else {
	  /* There have been no writes to the current sync point. */
	  if (m_current_sync_point->earlier_sync_point) {
	    /* If previous sync point hasn't completed, complete this flush
	     * with the earlier sync point.  No alloc or dispatch needed. */
	    m_current_sync_point->earlier_sync_point->m_on_sync_point_persisted.push_back(flush_req);
	    assert(m_current_sync_point->earlier_sync_point->m_append_scheduled);
	  } else {
	    /* The previous sync point has already completed and been
	     * appended. This flush completes now. */
	    post_unlock.add(flush_req);
	  }
	}
      }

      release_guarded_request(cell);
    });

  detain_guarded_request(GuardedRequest(flush_req->m_image_extents_summary.first_block,
					flush_req->m_image_extents_summary.last_block,
					guarded_ctx, true));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
					  bufferlist&& bl, int fadvise_flags,
					  Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  m_perfcounter->inc(l_librbd_rwl_ws, 1);
  ldout(cct, 20) << "offset=" << offset << ", "
		 << "length=" << length << ", "
		 << "data_len=" << bl.length() << ", "
		 << "on_finish=" << on_finish << dendl;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  // TBD: Must pass through block guard.

  m_image_writeback->aio_writesame(offset, length, std::move(bl), fadvise_flags, on_finish);

  /*
  bufferlist total_bl;

  uint64_t left = length;
  while(left) {
    total_bl.append(bl);
    left -= bl.length();
  }
  assert(length == total_bl.length());
  aio_write({{offset, length}}, std::move(total_bl), fadvise_flags, on_finish);
  */
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
						  bufferlist&& cmp_bl,
						  bufferlist&& bl,
						  uint64_t *mismatch_offset,
						  int fadvise_flags,
						  Context *on_finish) {

  m_perfcounter->inc(l_librbd_rwl_cmp, 1);

  // TBD: Must pass through block guard. Dispatch read through RWL. In completion
  // compare to cmp_bl. On match dispatch write.

  // TODO:
  // Compare source may be RWL, image cache, or image.
  // Write will be to RWL

  //CephContext *cct = m_image_ctx.cct;

  //ldout(cct, 20) << "image_extents=" << image_extents << ", "
  //               << "on_finish=" << on_finish << dendl;

  m_image_writeback->aio_compare_and_write(
    std::move(image_extents), std::move(cmp_bl), std::move(bl), mismatch_offset,
    fadvise_flags, on_finish);
}

/**
 * Begin a new sync point
 */
template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(Contexts &later) {
  CephContext *cct = m_image_ctx.cct;
  shared_ptr<SyncPoint> old_sync_point = m_current_sync_point;
  shared_ptr<SyncPoint> new_sync_point;
  ldout(cct, 20) << dendl;

  assert(m_lock.is_locked_by_me());

  if (old_sync_point) {
    /* Advance the sync gen num unless this is the first sync point */
    ++m_current_sync_gen;
  }
  new_sync_point = make_shared<SyncPoint>(cct, m_current_sync_gen);
  m_current_sync_point = new_sync_point;

  if (old_sync_point) {
    new_sync_point->earlier_sync_point = old_sync_point;
    old_sync_point->later_sync_point = new_sync_point;
    old_sync_point->m_final_op_sequence_num = m_last_op_sequence_num;
    if (!old_sync_point->m_appending) {
      /* Append of new sync point deferred until old sync point is appending */
      old_sync_point->m_on_sync_point_appending.push_back(new_sync_point->m_prior_log_entries_persisted->new_sub());
    }
    /* This sync point will acquire no more sub-ops. Activation needs
     * to acquire m_lock, so defer to later*/
    later.push_back(new FunctionContext(
      [this, old_sync_point](int r) {
	old_sync_point->m_prior_log_entries_persisted->activate();
      }));
  }

  Context *sync_point_persist_ready = new_sync_point->m_sync_point_persist->new_sub();
  new_sync_point->m_prior_log_entries_persisted->
    set_finisher(new FunctionContext([this, new_sync_point, sync_point_persist_ready](int r) {
	  ldout(m_image_ctx.cct, 20) << "Prior log entries persisted for sync point =[" << new_sync_point << "]" << dendl;
	  new_sync_point->m_prior_log_entries_persisted_result = r;
	  new_sync_point->m_prior_log_entries_persisted_complete = true;
	  sync_point_persist_ready->complete(r);
	}));

  if (old_sync_point) {
    ldout(cct,6) << "new sync point = [" << *m_current_sync_point
		 << "], prior = [" << *old_sync_point << "]" << dendl;
  } else {
    ldout(cct,6) << "first sync point = [" << *m_current_sync_point
		 << "]" << dendl;
  }
}

template <typename I>
const typename ImageCache<I>::Extent ReplicatedWriteLog<I>::whole_volume_extent(void) {
  return typename ImageCache<I>::Extent({0, ~0});
}

template <typename I>
void ReplicatedWriteLog<I>::perf_start(std::string name) {
  PerfCountersBuilder plb(m_image_ctx.cct, name, l_librbd_rwl_first, l_librbd_rwl_last);

  // Latency axis configuration for op histograms, values are in nanoseconds
  PerfHistogramCommon::axis_config_d op_hist_x_axis_config{
    "Latency (usec)",
    PerfHistogramCommon::SCALE_LOG2, ///< Latency in logarithmic scale
    0,                               ///< Start at 0
    5000,                            ///< Quantization unit is 5usec
    16,                              ///< Ranes into the mS
  };

  // Op size axis configuration for op histograms, values are in bytes
  PerfHistogramCommon::axis_config_d op_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    512,                             ///< Quantization unit is 512 bytes
    8,                               ///< Writes up to 32k
  };

  plb.add_u64_counter(l_librbd_rwl_rd_req, "rd", "Reads");
  plb.add_u64_counter(l_librbd_rwl_rd_bytes, "rd_bytes", "Data size in reads");
  plb.add_time_avg(l_librbd_rwl_rd_latency, "rd_latency", "Latency of reads");

  plb.add_u64_counter(l_librbd_rwl_rd_hit_req, "hit_rd", "Reads completely hitting RWL");
  plb.add_u64_counter(l_librbd_rwl_rd_hit_bytes, "rd_hit_bytes", "Bytes read from RWL");
  plb.add_time_avg(l_librbd_rwl_rd_hit_latency, "hit_rd_latency", "Latency of read hits");

  plb.add_u64_counter(l_librbd_rwl_rd_part_hit_req, "part_hit_rd", "reads partially hitting RWL");

  plb.add_u64_counter(l_librbd_rwl_wr_req, "wr", "Writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def, "wr_def", "Writes deferred for resources");
  plb.add_u64_counter(l_librbd_rwl_wr_req_overlap, "wr_overlap", "Writes overlapping with prior in-progress writes");
  plb.add_u64_counter(l_librbd_rwl_wr_bytes, "wr_bytes", "Data size in writes");

  plb.add_u64_counter(l_librbd_rwl_log_ops, "log_ops", "Log appends");
  plb.add_u64_avg(l_librbd_rwl_log_op_bytes, "log_op_bytes", "Average log append bytes");

  plb.add_time_avg(l_librbd_rwl_req_arr_to_all_t, "req_arr_to_all_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_req_arr_to_dis_t, "req_arr_to_dis_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_req_all_to_dis_t, "req_all_to_dis_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_wr_latency, "wr_latency", "Latency of writes (persistent completion)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_wr_latency_hist, "wr_latency_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_wr_caller_latency, "caller_wr_latency", "Latency of write completion to caller");

  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_buf_t, "op_dis_to_buf_t", "Average dispatch to buffer persist time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_app_t, "op_dis_to_app_t", "Average dispatch to log append time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_cmp_t, "op_dis_to_cmp_t", "Average dispatch to persist completion time");

  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_app_t, "op_buf_to_app_t", "Average buffer persist to log append time (write data persist/replicate + wait for append time)");
  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_bufc_t, "op_buf_to_bufc_t", "Average buffer persist time (write data persist/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_buf_to_bufc_t_hist, "op_buf_to_bufc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write buffer persist time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_cmp_t, "op_app_to_cmp_t", "Average log append to persist complete time (log entry append/replicate + wait for complete time)");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_appc_t, "op_app_to_appc_t", "Average log append to persist complete time (log entry append/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_app_to_appc_t_hist, "op_app_to_appc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of log append persist time (nanoseconds) (vs. op bytes)");

  plb.add_u64_counter(l_librbd_rwl_discard, "discard", "Discards");
  plb.add_u64_counter(l_librbd_rwl_discard_bytes, "discard_bytes", "Bytes discarded");
  plb.add_time_avg(l_librbd_rwl_discard_latency, "discard_lat", "Discard latency");

  plb.add_u64_counter(l_librbd_rwl_aio_flush, "aio_flush", "AIO flush (flush to RWL)");
  plb.add_u64_counter(l_librbd_rwl_aio_flush_def, "aio_flush_def", "AIO flushes deferred for resources");
  plb.add_time_avg(l_librbd_rwl_aio_flush_latency, "aio_flush_lat", "AIO flush latency");

  plb.add_u64_counter(l_librbd_rwl_ws,"ws", "Write Sames");
  plb.add_u64_counter(l_librbd_rwl_ws_bytes, "ws_bytes", "Write Same bytes to image");
  plb.add_time_avg(l_librbd_rwl_ws_latency, "ws_lat", "Write Same latency");

  plb.add_u64_counter(l_librbd_rwl_cmp, "cmp", "Compare and Write");
  plb.add_u64_counter(l_librbd_rwl_cmp_bytes, "cmp_bytes", "Compare and Write bytes written");
  plb.add_time_avg(l_librbd_rwl_cmp_latency, "cmp_lat", "Compare and Write latecy");

  plb.add_u64_counter(l_librbd_rwl_flush, "flush", "Flush (flush RWL)");
  plb.add_u64_counter(l_librbd_rwl_invalidate_cache, "invalidate", "Invalidate RWL");

  m_perfcounter = plb.create_perf_counters();
  m_image_ctx.cct->get_perfcounters_collection()->add(m_perfcounter);
}

template <typename I>
void ReplicatedWriteLog<I>::perf_stop() {
  assert(m_perfcounter);
  m_image_ctx.cct->get_perfcounters_collection()->remove(m_perfcounter);
  delete m_perfcounter;
}

template <typename I>
void ReplicatedWriteLog<I>::log_perf() {
  bufferlist bl;
  Formatter *f = Formatter::create("json-pretty");
  bl.append("Perf dump follows\n--- Begin perf dump ---\n");
  bl.append("rwl_stats: {\n");
  bl.append("\"stats\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted(f, 0);
  f->flush(bl);
  bl.append(",\n\"histograms\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted_histograms(f, 0);
  f->flush(bl);
  delete f;
  bl.append("}\n--- End perf dump ---\n");
  bl.append('\0');
  ldout(m_image_ctx.cct, 1) << bl.c_str() << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::arm_periodic_stats() {
  if (m_periodic_stats_enabled) {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.add_event_after(LOG_STATS_INTERVAL_SECONDS, new FunctionContext(
      [this](int r) {
	{
	  Mutex::Locker locker(m_lock);
	  ldout(m_image_ctx.cct, 1) << "STATS: "
				    << "m_free_log_entries=" << m_free_log_entries << ", "
				    << "m_ops_to_flush=" << m_ops_to_flush.size() << ", "
				    << "m_ops_to_append=" << m_ops_to_append.size() << ", "
				    << "m_deferred_ios=" << m_deferred_ios.size() << ", "
				    << "m_log_entries=" << m_log_entries.size() << ", "
				    << "m_dirty_log_entries=" << m_dirty_log_entries.size() << ", "
				    << "m_flush_ops_in_flight=" << m_flush_ops_in_flight << ", "
				    << "m_flush_bytes_in_flight=" << m_flush_bytes_in_flight << ", "
				    << "m_async_flush_ops=" << m_async_flush_ops << ", "
				    << "m_async_append_ops=" << m_async_append_ops << ", "
				    << "m_async_complete_ops=" << m_async_complete_ops << ", "
				    << "m_async_write_req_finish=" << m_async_write_req_finish << ", "
				    << "m_async_null_flush_finish=" << m_async_null_flush_finish << ", "
				    << "m_async_process_work=" << m_async_process_work << ", "
				    << "m_async_op_tracker=[" << m_async_op_tracker << "]"
				    << dendl;
	}
	arm_periodic_stats();
      }));
  }
}

template <typename I>
void ReplicatedWriteLog<I>::rwl_init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  DeferredContexts later;
  ldout(cct, 20) << dendl;
  TOID(struct WriteLogPoolRoot) pool_root;

  Mutex::Locker locker(m_lock);
  ldout(cct,5) << "rwl_enabled:" << m_image_ctx.rwl_enabled << dendl;
  ldout(cct,5) << "rwl_size:" << m_image_ctx.rwl_size << dendl;
  std::string rwl_path = m_image_ctx.rwl_path;
  ldout(cct,5) << "rwl_path:" << m_image_ctx.rwl_path << dendl;

  std::string log_pool_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".poolset";
  m_log_pool_size = ceph::max(cct->_conf->get_val<uint64_t>("rbd_rwl_size"), MIN_POOL_SIZE);

  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
  } else {
    m_log_pool_name = log_pool_name;
    lderr(cct) << "failed to open poolset" << log_poolset_name
	       << ":" << pmemobj_errormsg()
	       << ". Opening/creating simple/unreplicated pool" << dendl;
  }

  if (access(m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_log_pool =
	 pmemobj_create(m_log_pool_name.c_str(),
			rwl_pool_layout_name,
			m_log_pool_size,
			(S_IWUSR | S_IRUSR))) == NULL) {
      lderr(cct) << "failed to create pool (" << m_log_pool_name << ")"
		 << pmemobj_errormsg() << dendl;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    }
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(m_log_pool_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    TX_BEGIN(m_log_pool) {
      TX_ADD(pool_root);
      D_RW(pool_root)->header.layout_version = RWL_POOL_VERSION;
      D_RW(pool_root)->log_entries =
	TX_ZALLOC(struct WriteLogPmemEntry,
		  sizeof(struct WriteLogPmemEntry) * num_small_writes);
      D_RW(pool_root)->block_size = MIN_WRITE_ALLOC_SIZE;
      D_RW(pool_root)->num_log_entries = num_small_writes-1; // leave one free
      D_RW(pool_root)->first_free_entry = m_first_free_entry;
      D_RW(pool_root)->first_valid_entry = m_first_valid_entry;
    } TX_ONCOMMIT {
      m_total_log_entries = D_RO(pool_root)->num_log_entries;
      m_free_log_entries = D_RO(pool_root)->num_log_entries;
    } TX_ONABORT {
      m_total_log_entries = 0;
      m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << m_log_pool_name << ")" << dendl;
      on_finish->complete(-pmemobj_tx_errno());
      return;
    } TX_FINALLY {
    } TX_END;
  } else {
    /* Open existing pool */
    if ((m_log_pool =
	 pmemobj_open(m_log_pool_name.c_str(),
		      rwl_pool_layout_name)) == NULL) {
      lderr(cct) << "failed to open pool (" << m_log_pool_name << "): "
		 << pmemobj_errormsg() << dendl;
      on_finish->complete(-errno);
      return;
    }
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
    if (D_RO(pool_root)->header.layout_version != RWL_POOL_VERSION) {
      lderr(cct) << "Pool layout version is " << D_RO(pool_root)->header.layout_version
		 << " expected " << RWL_POOL_VERSION << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    if (D_RO(pool_root)->block_size != MIN_WRITE_ALLOC_SIZE) {
      lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    m_total_log_entries = D_RO(pool_root)->num_log_entries;
    m_free_log_entries = D_RO(pool_root)->num_log_entries;
    m_first_free_entry = D_RO(pool_root)->first_free_entry;
    m_first_valid_entry = D_RO(pool_root)->first_valid_entry;
    /* TODO: Actually load all the log entries already persisted */
    /* TODO: Set m_current_sync_gen to the successor of the last one seen in the log */
    ldout(cct,5) << "pool " << m_log_pool_name << "has " << D_RO(pool_root)->num_log_entries <<
      " log entries" << dendl;
    if (m_first_free_entry == m_first_valid_entry) {
      ldout(cct,5) << "write log is empty" << dendl;
    }
  }

  perf_start(m_image_ctx.id);

  /* Start the sync point following the last one seen in the log */
  new_sync_point(later.contexts);
  ldout(cct,20) << "new sync point = [" << m_current_sync_point << "]" << dendl;

  on_finish->complete(0);

  arm_periodic_stats();
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r >= 0) {
	rwl_init(on_finish);
      } else {
	/* Don't init RWL if layer below failed to init */
	on_finish->complete(r);
      }
    });
  /* Initialize the cache layer below first */
  m_image_writeback->init(ctx);
}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      {
	Mutex::Locker timer_locker(m_timer_lock);
	m_timer.cancel_all_events();
      }
      ldout(m_image_ctx.cct, 6) << "shutdown complete" << dendl;
      on_finish->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override on_finish status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      /* Shut down the cache layer below */
      ldout(m_image_ctx.cct, 6) << "shutting down lower cache" << dendl;
      m_image_writeback->shut_down(next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override next_ctx status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      m_periodic_stats_enabled = false;
      {
	Mutex::Locker timer_locker(m_timer_lock);
	m_timer.cancel_all_events();
      }
      if (m_perfcounter) {
	log_perf();
      }
      if (use_finishers) {
	ldout(m_image_ctx.cct, 6) << "stopping finishers" << dendl;
	m_persist_finisher.wait_for_empty();
	m_persist_finisher.stop();
	m_log_append_finisher.wait_for_empty();
	m_log_append_finisher.stop();
	m_on_persist_finisher.wait_for_empty();
	m_on_persist_finisher.stop();
      }
      {
	Mutex::Locker locker(m_lock);
	assert(m_dirty_log_entries.size() == 0);
	for (auto entry : m_log_entries) {
	  if (entry->ram_entry.is_write()) {
	    auto write_entry = dynamic_pointer_cast<WriteLogEntry>(entry);
	    m_blocks_to_log_entries.remove_log_entry(write_entry);
	    assert(write_entry->referring_map_entries == 0);
	    assert(write_entry->reader_count == 0);
	    assert(!write_entry->flushing);
	  }
	}
	m_log_entries.clear();
      }
      if (m_log_pool) {
	ldout(m_image_ctx.cct, 6) << "closing pmem pool" << dendl;
	pmemobj_close(m_log_pool);
	r = -errno;
      }
      if (m_perfcounter) {
	perf_stop();
      }
      next_ctx->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override next_ctx status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      ldout(m_image_ctx.cct, 6) << "waiting for internal async operations" << dendl;
      // Second op tracker wait after flush completion for process_work()
      Mutex::Locker locker(m_lock);
      m_wake_up_enabled = false;
      m_async_op_tracker.wait(m_image_ctx, next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override next_ctx status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      // flush all writes to OSDs
      ldout(m_image_ctx.cct, 6) << "flushing" << dendl;
      flush(next_ctx);
    });
  {
    ldout(m_image_ctx.cct, 6) << "waiting for in flight operations" << dendl;
    // Wait for in progress IOs to complete
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.wait(m_image_ctx, ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::wake_up() {
  CephContext *cct = m_image_ctx.cct;
  assert(m_lock.is_locked());

  if (!m_wake_up_enabled) {
    // wake_up is disabled during shutdown after flushing completes
    ldout(m_image_ctx.cct, 6) << "deferred processing disabled" << dendl;
    return;
  }

  if (m_wake_up_requested && m_wake_up_scheduled) {
    return;
  }

  ldout(cct, 20) << dendl;

  /* Wake-up can be requested while it's already scheduled */
  m_wake_up_requested = true;

  /* Wake-up cannot be scheduled if it's already scheduled */
  if (m_wake_up_scheduled) {
    return;
  }
  m_wake_up_scheduled = true;
  m_async_process_work++;
  m_async_op_tracker.start_op();
  m_image_ctx.op_work_queue->queue(new FunctionContext(
    [this](int r) {
      process_work();
      m_async_process_work--;
      m_async_op_tracker.finish_op();
    }), 0);
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  ldout(cct, 20) << dendl;

  do {
    {
      Mutex::Locker locker(m_lock);
      m_wake_up_requested = false;
    }
    dispatch_deferred_writes();
    process_writeback_dirty_entries();
    while (retire_entries()) { }

    {
      Mutex::Locker locker(m_lock);
      wake_up_requested = m_wake_up_requested;
    }
  } while (wake_up_requested && --max_iterations > 0);

  {
    Mutex::Locker locker(m_lock);
    m_wake_up_scheduled = false;
    /* Reschedule if it's still requested */
    if (m_wake_up_requested) {
      wake_up();
    }
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::can_flush_entry(shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->ram_entry.is_write());
  assert(m_lock.is_locked_by_me());

  /* For OWB we can flush entries with the same sync gen number (write between
   * aio_flush() calls) concurrently. Here we'll consider an entry flushable if
   * its sync gen number is <= the lowest sync gen number carried by all the
   * entries currently flushing.
   *
   * If the entry considered here bears a sync gen number lower than a
   * previously flushed entry, the application had to have submitted the write
   * bearing the higher gen number before the write with the lower gen number
   * completed. So, flushing these concurrently is OK.
   *
   * If the entry considered here bears a sync gen number higher than a
   * currently flushing entry, the write with the lower gen number may have
   * completed to the application before the write with the higher sync gen
   * number was submitted, and the application may rely on that completion
   * order for volume consistency. In this case the entry will not be
   * considered flushable until all the entries bearing lower sync gen numbers
   * finish flushing.
   */

  if (m_flush_ops_in_flight &&
      (log_entry->ram_entry.sync_gen_number > m_lowest_flushing_sync_gen)) {
    return false;
  }

  auto write_entry = dynamic_pointer_cast<WriteLogEntry>(log_entry);
  return (write_entry->completed &&
	  (m_flush_ops_in_flight <= IN_FLIGHT_FLUSH_WRITE_LIMIT) &&
	  (m_flush_bytes_in_flight <= IN_FLIGHT_FLUSH_BYTES_LIMIT));
}

template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->ram_entry.is_write());
  assert(m_entry_reader_lock.is_locked());
  assert(m_lock.is_locked_by_me());
  if (!m_flush_ops_in_flight ||
      (log_entry->ram_entry.sync_gen_number < m_lowest_flushing_sync_gen)) {
    m_lowest_flushing_sync_gen = log_entry->ram_entry.sync_gen_number;
  }
  auto write_entry = dynamic_pointer_cast<WriteLogEntry>(log_entry);
  m_flush_ops_in_flight += 1;
  m_flush_bytes_in_flight += write_entry->ram_entry.write_bytes;

  /* Construct bl for pmem buffer */
  write_entry->add_reader();
  write_entry->flushing = true;
  m_async_op_tracker.start_op();
  buffer::raw *entry_buf =
    buffer::claim_buffer(write_entry->ram_entry.write_bytes,
			 (char*)write_entry->pmem_buffer,
			 make_deleter([this, write_entry]
				      {
					CephContext *cct = m_image_ctx.cct;
					ldout(cct, 20) << "removing (flush) reader: log_entry="
						       << *write_entry << dendl;
					write_entry->remove_reader();
					m_async_op_tracker.finish_op();
				      }));

  /* The caller will send the flush write later when we're not holding m_lock */
  return new FunctionContext(
    [this, cct, log_entry, write_entry, entry_buf](int r) {
      /* Flush write completion action */
      Context *ctx = new FunctionContext(
	[this, cct, log_entry, write_entry](int r) {
	  {
	    Mutex::Locker locker(m_lock);
	    m_flush_ops_in_flight -= 1;
	    m_flush_bytes_in_flight -= write_entry->ram_entry.write_bytes;
	    write_entry->flushing = false;
	    if (r < 0) {
	      lderr(cct) << "failed to flush write log entry"
			 << cpp_strerror(r) << dendl;
	      m_dirty_log_entries.push_front(log_entry);
	    } else {
	      write_entry->flushed = true;
	      ldout(cct, 20) << "flushed: " << write_entry << dendl;
	    }
	    wake_up();
	  }
	});

      bufferlist entry_bl;
      entry_bl.push_back(entry_buf);
      ldout(cct, 15) << "flushing:" << write_entry
		     << " " << *write_entry << dendl;
      m_image_writeback->aio_write({{write_entry->ram_entry.image_offset_bytes,
				     write_entry->ram_entry.write_bytes}},
				   std::move(entry_bl), 0, ctx);
    });
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_entries() {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;

  ldout(cct, 20) << "Look for dirty entries" << dendl;
  {
    DeferredContexts post_unlock;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    while (true) {
      Mutex::Locker locker(m_lock);
      if (m_dirty_log_entries.empty()) {
	ldout(cct, 20) << "Nothing new to flush" << dendl;
	break;
      }
      auto candidate = m_dirty_log_entries.front();
      bool flushable = can_flush_entry(candidate);
      if (flushable) {
	post_unlock.contexts.push_back(construct_flush_entry_ctx(candidate));
      }
      if (flushable || !candidate->ram_entry.is_write()) {
	/* Remove if we're flushing it, or it's not a write */
	m_dirty_log_entries.pop_front();
      } else {
	ldout(cct, 20) << "Next dirty entry isn't flushable yet" << dendl;
	break;
      }
    }

    /* Check for and take flush complete actions */
    all_clean = (0 == m_flush_ops_in_flight &&
		 m_dirty_log_entries.empty());
  }

  if (all_clean) {
    /* All flushing complete, drain outside lock */
    Contexts flush_contexts;
    {
      Mutex::Locker locker(m_lock);
      flush_contexts.swap(m_flush_complete_contexts);
    }
    finish_contexts(m_image_ctx.cct, flush_contexts, 0);
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::can_retire_entry(shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(m_lock.is_locked_by_me());
  if (!log_entry->completed) {
    return false;
  }
  if (log_entry->ram_entry.is_write()) {
    auto write_entry = dynamic_pointer_cast<WriteLogEntry>(log_entry);
    return (write_entry->flushed &&
	    0 == write_entry->reader_count);
  } else {
    return true;
  }
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries() {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntries retiring_entries;
  uint32_t first_valid_entry;

  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(m_entry_reader_lock);
    Mutex::Locker locker(m_lock);
    first_valid_entry = m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
	   retiring_entries.size() < MAX_ALLOC_PER_TRANSACTION &&
	   can_retire_entry(entry)) {
      assert(entry->completed);
      assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if (entry->ram_entry.is_write()) {
	auto write_entry = dynamic_pointer_cast<WriteLogEntry>(entry);
	m_blocks_to_log_entries.remove_log_entry(write_entry);
	assert(!write_entry->flushing);
	assert(write_entry->flushed);
	assert(!write_entry->reader_count);
	assert(!write_entry->referring_map_entries);
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    /* Advance first valid entry and release buffers */
    {
      Mutex::Locker append_locker(m_log_append_lock);

      TX_BEGIN(m_log_pool) {
	D_RW(pool_root)->first_valid_entry = first_valid_entry;
	for (auto &entry: retiring_entries) {
	  if (entry->ram_entry.is_write()) {
	    ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo <<
	      "." << entry->ram_entry.write_data.oid.off << dendl;
	    TX_FREE(entry->ram_entry.write_data);
	  } else {
	    //ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
	  }
	}
      } TX_ONCOMMIT {
      } TX_ONABORT {
	lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << m_log_pool_name << ")" << dendl;
	assert(false);
      } TX_FINALLY {
      } TX_END;
    }

    /* Update runtime copy of first_valid, and free entries counts */
    {
      Mutex::Locker locker(m_lock);

      m_first_valid_entry = first_valid_entry;
      m_free_log_entries += retiring_entries.size();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  Extent invalidate_extent = whole_volume_extent();
  m_perfcounter->inc(l_librbd_rwl_invalidate_cache, 1);
  ldout(cct, 20) << dendl;

  assert(is_block_aligned(invalidate_extent));

  /* Invalidate must pass through block guard to ensure all layers of cache are
   * consistently invalidated. This ensures no in-flight write leaves some
   * layers with valid regions, which may later produce inconsistent read
   * results. */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext(
      [this, on_finish, invalidate_extent](BlockGuardCell *cell, bool detained) {
	CephContext *cct = m_image_ctx.cct;
	ldout(cct, 6) << "invalidate_extent=" << invalidate_extent << " "
		      << "cell=" << cell << dendl;

	assert(cell);

	Context *ctx = new FunctionContext(
	  [this, cell, on_finish](int r) {
	    on_finish->complete(r);
	    release_guarded_request(cell);
	  });
	ctx = new FunctionContext(
	  [this, ctx](int r) {
	    Context *next_ctx = ctx;
	    if (r < 0) {
	      /* Override on_finish status with this error */
	      next_ctx = new FunctionContext([r, ctx](int _r) {
		  ctx->complete(r);
		});
	    }
	    /* Invalidate from caches below */
	    m_image_writeback->invalidate(next_ctx);
	  });
	invalidate({invalidate_extent}, ctx);
      });
  BlockExtent invalidate_block_extent(block_extent(invalidate_extent));
  detain_guarded_request(GuardedRequest(invalidate_block_extent.block_start,
					invalidate_block_extent.block_end,
					guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Extents&& image_extents,
				       Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  // TODO - Selective invalidate does not pass through block guard, but
  // whatever calls it must. Appends invalidate entry. Affected region is
  // treated as a RWL miss on reads, and are not flushable (each affected entry
  // will be updated to indicate what portion was invalidated). Even in OWB
  // flushing, portions of writes occluded by invalidates must not be
  // flushed. Selective invalidate is *not* passed on to cache below.
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint32_t block_start_offset = image_offset % MIN_WRITE_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
				      MIN_WRITE_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  on_finish->complete(0);
}

/*
 * Internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;
  m_perfcounter->inc(l_librbd_rwl_flush, 1);

  {
    Mutex::Locker locker(m_lock);
    all_clean = (0 == m_flush_ops_in_flight &&
		 m_dirty_log_entries.empty());
  }

  if (all_clean) {
    /* Complete without holding m_lock */
    ldout(cct, 20) << "no dirty entries" << dendl;
    on_finish->complete(0);
  } else {
    ldout(cct, 20) << "dirty entries remain" << dendl;
    Mutex::Locker locker(m_lock);
    /* on_finish can't be completed yet */
    m_flush_complete_contexts.push_back(new FunctionContext(
      [this, on_finish](int r) {
	flush(on_finish);
      }));
    wake_up();
  }
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
