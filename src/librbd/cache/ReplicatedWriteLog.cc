// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <libpmemobj.h>
#include "ReplicatedWriteLog.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "rwl/SharedPtrContext.h"
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

/* Defer a set of Contexts until destruct/exit. Used for deferring
 * work on a given thread until a required lock is dropped. */
class DeferredContexts {
private:
  std::vector<Context*> contexts;
public:
  ~DeferredContexts() {
    finish_contexts(nullptr, contexts, 0);
  }
  void add(Context* ctx) {
    contexts.push_back(ctx);
  }
};

/* Pmem structures */
POBJ_LAYOUT_BEGIN(rbd_rwl);
POBJ_LAYOUT_ROOT(rbd_rwl, struct WriteLogPoolRoot);
POBJ_LAYOUT_TOID(rbd_rwl, uint8_t);
POBJ_LAYOUT_TOID(rbd_rwl, struct WriteLogPmemEntry);
POBJ_LAYOUT_END(rbd_rwl);

struct WriteLogPmemEntry {
  uint64_t sync_gen_number = 0;
  uint64_t write_sequence_number = 0;
  uint64_t image_offset_bytes;
  uint64_t write_bytes;
  TOID(uint8_t) write_data;
  struct {
    uint8_t entry_valid :1; /* if 0, this entry is free */
    uint8_t sync_point :1;  /* No data. No write sequence number. Marks sync
			       point for this sync gen number */
    uint8_t sequenced :1;   /* write sequence number is valid */
    uint8_t has_data :1;    /* write_data field is valid (else ignore) */
    uint8_t discard :1;     /* has_data will be 0 if this is a discard */
    uint8_t writesame :1;   /* ws_datalen indicates length of data at write_bytes */
  };
  uint32_t ws_datalen = 0;  /* Length of data buffer (writesame only) */
  uint32_t entry_index = 0; /* For debug consistency check. Can be removed if
			     * we need the space */
  WriteLogPmemEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : image_offset_bytes(image_offset_bytes), write_bytes(write_bytes),
      entry_valid(0), sync_point(0), sequenced(0), has_data(0), discard(0), writesame(0) {
  }
  const BlockExtent block_extent();
  bool is_sync_point() {
    return sync_point;
  }
  bool is_discard() {
    return discard;
  }
  bool is_writesame() {
    return writesame;
  }
  bool is_write() {
    /* Log entry is a basic write */
    return !is_sync_point() && !is_discard() && !is_writesame();
  }
  bool is_writer() {
    /* Log entry is any type that writes data */
    return is_write() || is_discard() || is_writesame();
  }
  const uint64_t get_offset_bytes() { return image_offset_bytes; }
  const uint64_t get_write_bytes() { return write_bytes; }
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogPmemEntry &entry) {
    os << "entry_valid=" << (bool)entry.entry_valid << ", "
       << "sync_point=" << (bool)entry.sync_point << ", "
       << "sequenced=" << (bool)entry.sequenced << ", "
       << "has_data=" << (bool)entry.has_data << ", "
       << "discard=" << (bool)entry.discard << ", "
       << "writesame=" << (bool)entry.writesame << ", "
       << "sync_gen_number=" << entry.sync_gen_number << ", "
       << "write_sequence_number=" << entry.write_sequence_number << ", "
       << "image_offset_bytes=" << entry.image_offset_bytes << ", "
       << "write_bytes=" << entry.write_bytes << ", "
       << "ws_datalen=" << entry.ws_datalen << ", "
       << "entry_index=" << entry.entry_index;
    return os;
  };
};

static_assert(sizeof(WriteLogPmemEntry) == 64);

struct WriteLogPoolRoot {
  union {
    struct {
      uint8_t layout_version;    /* Version of this structure (RWL_POOL_VERSION) */
    };
    uint64_t _u64;
  } header;
  TOID(struct WriteLogPmemEntry) log_entries;   /* contiguous array of log entries */
  uint64_t pool_size;
  uint64_t flushed_sync_gen;     /* All writing entries with this or a lower
				  * sync gen number are flushed. */
  uint32_t block_size;		 /* block size */
  uint32_t num_log_entries;
  uint32_t first_free_entry;     /* Entry following the newest valid entry */
  uint32_t first_valid_entry;    /* Index of the oldest valid entry in the log */
};

static const bool RWL_VERBOSE_LOGGING = true;

typedef ReplicatedWriteLog<ImageCtx>::Extent Extent;
typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

/*
 * A BlockExtent identifies a range by first and last.
 *
 * An Extent ("image extent") identifies a range by start and length.
 *
 * The ImageCache interface is defined in terms of image extents, and
 * requires no alignment of the beginning or end of the extent. We
 * convert between image and block extents here using a "block size"
 * of 1.
 */
const BlockExtent block_extent(const uint64_t offset_bytes, const uint64_t length_bytes)
{
  return BlockExtent(offset_bytes,
		     offset_bytes + length_bytes - 1);
}

const BlockExtent block_extent(const Extent& image_extent)
{
  return block_extent(image_extent.first, image_extent.second);
}

const Extent image_extent(const BlockExtent& block_extent)
{
  return Extent(block_extent.block_start,
		block_extent.block_end - block_extent.block_start + 1);
}

const BlockExtent WriteLogPmemEntry::block_extent() {
  return BlockExtent(librbd::cache::rwl::block_extent(image_offset_bytes, write_bytes));
}

class GenericLogEntry {
public:
  WriteLogPmemEntry ram_entry;
  WriteLogPmemEntry *pmem_entry = nullptr;
  uint32_t log_entry_index = 0;
  bool completed = false;
  GenericLogEntry(const uint64_t image_offset_bytes = 0, const uint64_t write_bytes = 0)
    : ram_entry(image_offset_bytes, write_bytes) {
  };
  virtual ~GenericLogEntry() { };
  GenericLogEntry(const GenericLogEntry&) = delete;
  GenericLogEntry &operator=(const GenericLogEntry&) = delete;
  virtual unsigned int write_bytes() = 0;
  bool is_sync_point() { return ram_entry.is_sync_point(); }
  bool is_discard() { return ram_entry.is_discard(); }
  bool is_writesame() { return ram_entry.is_writesame(); }
  bool is_write() { return ram_entry.is_write(); }
  bool is_writer() { return ram_entry.is_writer(); }
  virtual const GenericLogEntry* get_log_entry() = 0;
  virtual const SyncPointLogEntry* get_sync_point_log_entry() { return nullptr; }
  virtual const GeneralWriteLogEntry* get_gen_write_log_entry() { return nullptr; }
  virtual const WriteLogEntry* get_write_log_entry() { return nullptr; }
  virtual const WriteSameLogEntry* get_write_same_log_entry() { return nullptr; }
  virtual const DiscardLogEntry* get_discard_log_entry() { return nullptr; }
  virtual std::ostream &format(std::ostream &os) const {
    os << "ram_entry=[" << ram_entry << "], "
       << "pmem_entry=" << (void*)pmem_entry << ", "
       << "log_entry_index=" << log_entry_index << ", "
       << "completed=" << completed;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GenericLogEntry &entry) {
    return entry.format(os);
  }
};

class SyncPointLogEntry : public GenericLogEntry {
public:
  /* Writing entries using this sync gen number */
  std::atomic<unsigned int> m_writes = {0};
  /* Total bytes for all writing entries using this sync gen number */
  std::atomic<uint64_t> m_bytes = {0};
  /* Writing entries using this sync gen number that have completed */
  std::atomic<unsigned int> m_writes_completed = {0};
  /* Writing entries using this sync gen number that have completed flushing to the writeback interface */
  std::atomic<unsigned int> m_writes_flushed = {0};
  /* All writing entries using all prior sync gen numbers have been flushed */
  std::atomic<bool> m_prior_sync_point_flushed = {true};
  std::shared_ptr<SyncPointLogEntry> m_next_sync_point_entry = nullptr;
  SyncPointLogEntry(const uint64_t sync_gen_number) {
    ram_entry.sync_gen_number = sync_gen_number;
    ram_entry.sync_point = 1;
  };
  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;
  virtual inline unsigned int write_bytes() { return 0; }
  const GenericLogEntry* get_log_entry() override { return get_sync_point_log_entry(); }
  const SyncPointLogEntry* get_sync_point_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Sync Point) ";
    GenericLogEntry::format(os);
    os << ", "
       << "m_writes=" << m_writes << ", "
       << "m_bytes=" << m_bytes << ", "
       << "m_writes_completed=" << m_writes_completed << ", "
       << "m_writes_flushed=" << m_writes_flushed << ", "
       << "m_prior_sync_point_flushed=" << m_prior_sync_point_flushed << ", "
       << "m_next_sync_point_entry=" << m_next_sync_point_entry;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const SyncPointLogEntry &entry) {
    return entry.format(os);
  }
};

class GeneralWriteLogEntry : public GenericLogEntry {
private:
  friend class WriteLogEntry;
  friend class DiscardLogEntry;
public:
  uint32_t referring_map_entries = 0;
  bool flushing = false;
  bool flushed = false; /* or invalidated */
  std::shared_ptr<SyncPointLogEntry> sync_point_entry;
  GeneralWriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		       const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) { }
  GeneralWriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(nullptr) { }
  GeneralWriteLogEntry(const GeneralWriteLogEntry&) = delete;
  GeneralWriteLogEntry &operator=(const GeneralWriteLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. Discard and WS override. */
    return ram_entry.write_bytes;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. Discard and WS override. */
    return write_bytes();
  };
  const BlockExtent block_extent() { return ram_entry.block_extent(); }
  const GenericLogEntry* get_log_entry() override { return get_gen_write_log_entry(); }
  const GeneralWriteLogEntry* get_gen_write_log_entry() override { return this; }
  uint32_t get_map_ref() { return(referring_map_entries); }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }
  std::ostream &format(std::ostream &os) const {
    GenericLogEntry::format(os);
    os << ", "
       << "sync_point_entry=[";
    if (sync_point_entry) {
      os << *sync_point_entry;
    } else {
      os << "nullptr";
    }
    os << "], "
       << "referring_map_entries=" << referring_map_entries << ", "
       << "flushing=" << flushing << ", "
       << "flushed=" << flushed;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GeneralWriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteLogEntry : public GeneralWriteLogEntry {
protected:
  buffer::ptr pmem_bp;
  buffer::list pmem_bl;
  std::atomic<int> bl_refs = {0}; /* The refs held on pmem_bp by pmem_bl */

  void init_pmem_bp() {
    assert(!pmem_bp.get_raw());
    pmem_bp = buffer::ptr(buffer::create_static(this->write_bytes(), (char*)pmem_buffer));
  }

  /* Write same will override */
  virtual void init_bl(buffer::ptr &bp, buffer::list &bl) {
    bl.append(bp);
  }

  void init_pmem_bl() {
    pmem_bl.clear();
    init_pmem_bp();
    assert(pmem_bp.get_raw());
    int before_bl = pmem_bp.raw_nref();
    this->init_bl(pmem_bp, pmem_bl);
    int after_bl = pmem_bp.raw_nref();
    bl_refs = after_bl - before_bl;
  }

public:
  uint8_t *pmem_buffer = nullptr;
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  const BlockExtent block_extent();

  unsigned int reader_count() {
    if (pmem_bp.get_raw()) {
      return (pmem_bp.raw_nref() - bl_refs - 1);
    } else {
      return 0;
    }
  }

  /* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
  buffer::list &get_pmem_bl(Mutex &entry_bl_lock) {
    if (0 == bl_refs) {
      Mutex::Locker locker(entry_bl_lock);
      if (0 == bl_refs) {
	init_pmem_bl();
      }
      assert(0 != bl_refs);
    }
    return pmem_bl;
  };

  /* Constructs a new bl containing copies of pmem_bp */
  void copy_pmem_bl(Mutex &entry_bl_lock, bufferlist *out_bl) {
    this->get_pmem_bl(entry_bl_lock);
    /* pmem_bp is now initialized */
    buffer::ptr cloned_bp(pmem_bp.clone());
    out_bl->clear();
    this->init_bl(cloned_bp, *out_bl);
  }

  virtual const GenericLogEntry* get_log_entry() override { return get_write_log_entry(); }
  const WriteLogEntry* get_write_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Write) ";
    GeneralWriteLogEntry::format(os);
    os << ", "
       << "pmem_buffer=" << (void*)pmem_buffer << ", ";
    os << "pmem_bp=" << pmem_bp << ", ";
    os << "pmem_bl=" << pmem_bl << ", ";
    os << "bl_refs=" << bl_refs;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteSameLogEntry : public WriteLogEntry {
protected:
  void init_bl(buffer::ptr &bp, buffer::list &bl) override {
    for (uint64_t i = 0; i < ram_entry.write_bytes / ram_entry.ws_datalen; i++) {
      bl.append(bp);
    }
    int trailing_partial = ram_entry.write_bytes % ram_entry.ws_datalen;
    if (trailing_partial) {
      bl.append(bp, 0, trailing_partial);
    }
  };

public:
  WriteSameLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		    const uint64_t image_offset_bytes, const uint64_t write_bytes,
		    const uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes,
		    const uint32_t data_length)
    : WriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;
  virtual inline unsigned int write_bytes() override {
    /* The valid bytes in this ops data buffer. */
    return ram_entry.ws_datalen;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  const BlockExtent block_extent();
  const GenericLogEntry* get_log_entry() override { return get_write_same_log_entry(); }
  const WriteSameLogEntry* get_write_same_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(WriteSame) ";
    WriteLogEntry::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteSameLogEntry &entry) {
    return entry.format(os);
  }
};

class DiscardLogEntry : public GeneralWriteLogEntry {
public:
  DiscardLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		  const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };
  DiscardLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };
  DiscardLogEntry(const DiscardLogEntry&) = delete;
  DiscardLogEntry &operator=(const DiscardLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. */
    return 0;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  const BlockExtent block_extent();
  const GenericLogEntry* get_log_entry() { return get_discard_log_entry(); }
  const DiscardLogEntry* get_discard_log_entry() { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Discard) ";
    GeneralWriteLogEntry::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const DiscardLogEntry &entry) {
    return entry.format(os);
  }
};

template <typename T>
SyncPoint<T>::SyncPoint(T &rwl, uint64_t sync_gen_num)
  : rwl(rwl), log_entry(std::make_shared<SyncPointLogEntry>(sync_gen_num)) {
  m_prior_log_entries_persisted = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  m_sync_point_persist = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  m_on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  m_on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "sync point " << sync_gen_num << dendl;
  }
}

template <typename T>
SyncPoint<T>::~SyncPoint() {
  assert(m_on_sync_point_appending.empty());
  assert(m_on_sync_point_persisted.empty());
  assert(!earlier_sync_point);
}

template <typename T>
std::ostream &SyncPoint<T>::format(std::ostream &os) const {
  os << "log_entry=[" << *log_entry << "], "
     << "earlier_sync_point=" << earlier_sync_point << ", "
     << "later_sync_point=" << later_sync_point << ", "
     << "m_final_op_sequence_num=" << m_final_op_sequence_num << ", "
     << "m_prior_log_entries_persisted=" << m_prior_log_entries_persisted << ", "
     << "m_prior_log_entries_persisted_complete=" << m_prior_log_entries_persisted_complete << ", "
     << "m_append_scheduled=" << m_append_scheduled << ", "
     << "m_appending=" << m_appending << ", "
     << "m_on_sync_point_appending=" << m_on_sync_point_appending.size() << ", "
     << "m_on_sync_point_persisted=" << m_on_sync_point_persisted.size() << "";
  return os;
};

template <typename T>
GenericLogOperation<T>::GenericLogOperation(T &rwl, const utime_t dispatch_time)
  : rwl(rwl), m_dispatch_time(dispatch_time) {
}

template <typename T>
SyncPointLogOperation<T>::SyncPointLogOperation(T &rwl,
						std::shared_ptr<SyncPoint<T>> sync_point,
						const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time), sync_point(sync_point) {
}

template <typename T>
SyncPointLogOperation<T>::~SyncPointLogOperation() { }

template <typename T>
void SyncPointLogOperation<T>::appending() {
  std::vector<Context*> appending_contexts;

  assert(sync_point);
  {
    Mutex::Locker locker(rwl.m_lock);
    if (!sync_point->m_appending) {
      ldout(rwl.m_image_ctx.cct, 20) << "Sync point op=[" << *this
				     << "] appending" << dendl;
      sync_point->m_appending = true;
    }
    appending_contexts.swap(sync_point->m_on_sync_point_appending);
  }
  for (auto &ctx : appending_contexts) {
    //rwl.m_work_queue.queue(ctx);
    ctx->complete(0);
  }
}

template <typename T>
void SyncPointLogOperation<T>::complete(int result) {
  std::vector<Context*> persisted_contexts;

  assert(sync_point);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "Sync point op =[" << *this
				   << "] completed" << dendl;
  }
  {
    Mutex::Locker locker(rwl.m_lock);
    /* Remove link from next sync point */
    assert(sync_point->later_sync_point);
    assert(sync_point->later_sync_point->earlier_sync_point ==
	   sync_point);
    sync_point->later_sync_point->earlier_sync_point = nullptr;
  }

  /* Do append now in case completion occurred before the
   * normal append callback executed, and to handle
   * on_append work that was queued after the sync point
   * entered the appending state. */
  appending();

  {
    Mutex::Locker locker(rwl.m_lock);
    /* The flush request that scheduled this op will be one of these
     * contexts */
    persisted_contexts.swap(sync_point->m_on_sync_point_persisted);
    rwl.handle_flushed_sync_point(sync_point->log_entry);
  }
  for (auto &ctx : persisted_contexts) {
    //rwl.m_work_queue.queue(ctx, result);
    ctx->complete(result);
  }
}

template <typename T>
GeneralWriteLogOperation<T>::GeneralWriteLogOperation(T &rwl,
						      std::shared_ptr<SyncPoint<T>> sync_point,
						      const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time),
  m_lock("librbd::cache::rwl::GeneralWriteLogOperation::m_lock"), sync_point(sync_point) {
}

template <typename T>
GeneralWriteLogOperation<T>::~GeneralWriteLogOperation() { }

template <typename T>
DiscardLogOperation<T>::DiscardLogOperation(T &rwl,
					    std::shared_ptr<SyncPoint<T>> sync_point,
					    const uint64_t image_offset_bytes,
					    const uint64_t write_bytes,
					    const utime_t dispatch_time)
  : GeneralWriteLogOperation<T>(rwl, sync_point, dispatch_time),
    log_entry(std::make_shared<DiscardLogEntry>(sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = sync_point->m_prior_log_entries_persisted->new_sub();
  on_write_persist = nullptr;
  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
DiscardLogOperation<T>::~DiscardLogOperation() { }

template <typename T>
WriteLogOperation<T>::WriteLogOperation(WriteLogOperationSet<T> &set,
					uint64_t image_offset_bytes, uint64_t write_bytes)
  : GeneralWriteLogOperation<T>(set.rwl, set.sync_point, set.m_dispatch_time),
    log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = set.m_extent_ops_appending->new_sub();
  on_write_persist = set.m_extent_ops_persist->new_sub();
  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
WriteLogOperation<T>::~WriteLogOperation() { }

template <typename T>
std::ostream &WriteLogOperation<T>::format(std::ostream &os) const {
  os << "(Write) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  os << "bl=[" << bl << "],"
     << "buffer_alloc=" << buffer_alloc;
  return os;
};

template <typename T>
WriteSameLogOperation<T>::WriteSameLogOperation(WriteLogOperationSet<T> &set, uint64_t image_offset_bytes,
						uint64_t write_bytes, uint32_t data_len)
  : WriteLogOperation<T>(set, image_offset_bytes, write_bytes) {
  auto ws_entry =
    std::make_shared<WriteSameLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes, data_len);
  log_entry = static_pointer_cast<WriteLogEntry>(ws_entry);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
}

template <typename T>
WriteSameLogOperation<T>::~WriteSameLogOperation() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
}

/* Called when the write log operation is appending and its log position is guaranteed */
template <typename T>
void GeneralWriteLogOperation<T>::appending() {
  Context *on_append = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    Mutex::Locker locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }
  if (on_append) {
    //rwl.m_work_queue.queue(on_append);
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_append=" << on_append << dendl;
    }
    on_append->complete(0);
  }
}

/* Called when the write log operation is completed in all log replicas */
template <typename T>
void GeneralWriteLogOperation<T>::complete(int result) {
  appending();
  Context *on_persist = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    Mutex::Locker locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }
  if (on_persist) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_persist=" << on_persist << dendl;
    }
    on_persist->complete(result);
  }
}

template <typename T>
std::ostream &DiscardLogOperation<T>::format(std::ostream &os) const {
  os << "(Discard) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  return os;
};

template <typename T>
WriteLogOperationSet<T>::WriteLogOperationSet(T &rwl, utime_t dispatched, std::shared_ptr<SyncPoint<T>> sync_point,
					      bool persist_on_flush, BlockExtent extent, Context *on_finish)
  : rwl(rwl), m_extent(extent), m_on_finish(on_finish),
    m_persist_on_flush(persist_on_flush), m_dispatch_time(dispatched), sync_point(sync_point) {
  m_on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
  m_on_ops_persist = nullptr;
  m_extent_ops_persist =
    new C_Gather(rwl.m_image_ctx.cct,
		 new FunctionContext( [this](int r) {
		     if (RWL_VERBOSE_LOGGING) {
		       ldout(this->rwl.m_image_ctx.cct,20) << __func__ << " " << this << " m_extent_ops_persist completed" << dendl;
		     }
		     if (m_on_ops_persist) {
		       m_on_ops_persist->complete(r);
		     }
		     m_on_finish->complete(r);
		   }));
  auto appending_persist_sub = m_extent_ops_persist->new_sub();
  m_extent_ops_appending =
    new C_Gather(rwl.m_image_ctx.cct,
		 new FunctionContext( [this, appending_persist_sub](int r) {
		     if (RWL_VERBOSE_LOGGING) {
		       ldout(this->rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " m_extent_ops_appending completed" << dendl;
		     }
		     m_on_ops_appending->complete(r);
		     appending_persist_sub->complete(r);
		   }));
}

template <typename T>
WriteLogOperationSet<T>::~WriteLogOperationSet() { }

GuardedRequestFunctionContext::GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback)
  : m_callback(std::move(callback)){ }

GuardedRequestFunctionContext::~GuardedRequestFunctionContext(void) { }

void GuardedRequestFunctionContext::finish(int r) {
  assert(m_cell);
  m_callback(*this);
}

/**
 * A request that can be deferred in a BlockGuard to sequence
 * overlapping operations.
 */
template <typename T>
struct C_GuardedBlockIORequest : public SharedPtrContext {
private:
  std::atomic<bool> m_cell_released = {false};
  BlockGuardCell* m_cell = nullptr;
public:
  T &rwl;
  C_GuardedBlockIORequest(T &rwl)
    : rwl(rwl) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }
  ~C_GuardedBlockIORequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
    assert(m_cell_released || !m_cell);
  }
  C_GuardedBlockIORequest(const C_GuardedBlockIORequest&) = delete;
  C_GuardedBlockIORequest &operator=(const C_GuardedBlockIORequest&) = delete;
  auto shared_from_this() {
    return shared_from(this);
  }

  virtual const char *get_name() const = 0;
  void set_cell(BlockGuardCell *cell) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << cell << dendl;
    }
    assert(cell);
    assert(!m_cell);
    m_cell = cell;
  }
  BlockGuardCell *get_cell(void) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
    }
    return m_cell;
  }

  void release_cell() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
    }
    assert(m_cell);
    bool initial = false;
    if (m_cell_released.compare_exchange_strong(initial, true)) {
      rwl.release_guarded_request(m_cell);
    } else {
      ldout(rwl.m_image_ctx.cct, 5) << "cell " << m_cell << " already released for " << this << dendl;
    }
  }
};

} // namespace rwl

class ReplicatedWriteLogInternal {
public:
  PMEMobjpool *m_log_pool = nullptr;
};

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(ImageCtx &image_ctx, ImageCache<I> *lower)
  : m_internal(new ReplicatedWriteLogInternal),
    rwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_rwl)),
    m_image_ctx(image_ctx),
    m_log_pool_config_size(DEFAULT_POOL_SIZE),
    m_image_writeback(lower), m_write_log_guard(image_ctx.cct),
    m_log_retire_lock("librbd::cache::ReplicatedWriteLog::m_log_retire_lock",
		      false, true, true),
    m_entry_reader_lock("librbd::cache::ReplicatedWriteLog::m_entry_reader_lock"),
    m_deferred_dispatch_lock("librbd::cache::ReplicatedWriteLog::m_deferred_dispatch_lock",
			     false, true, true),
    m_log_append_lock("librbd::cache::ReplicatedWriteLog::m_log_append_lock",
		      false, true, true),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock",
	   false, true, true),
    m_blockguard_lock("librbd::cache::ReplicatedWriteLog::m_blockguard_lock",
	   false, true, true),
    m_entry_bl_lock("librbd::cache::ReplicatedWriteLog::m_entry_bl_lock",
	   false, true, true),
    m_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_persist_finisher", "pfin_rwl"),
    m_log_append_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_log_append_finisher", "afin_rwl"),
    m_on_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_on_persist_finisher", "opfin_rwl"),
    m_blocks_to_log_entries(image_ctx.cct),
    m_timer_lock("librbd::cache::ReplicatedWriteLog::m_timer_lock",
	   false, true, true),
    m_timer(image_ctx.cct, m_timer_lock, false),
    m_thread_pool(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::thread_pool", "tp_rwl",
		  /*image_ctx.cct->_conf.get_val<int64_t>("rbd_op_threads")*/ 4, // TODO: Add config value
		  /*"rbd_op_threads"*/""), //TODO: match above
    m_work_queue("librbd::cache::ReplicatedWriteLog::work_queue",
		 60, //image_ctx.cct->_conf.get_val<int64_t>("rbd_op_thread_timeout"),
		 &m_thread_pool)
{
  assert(lower);
  m_thread_pool.start();
  if (use_finishers) {
    m_persist_finisher.start();
    m_log_append_finisher.start();
    m_on_persist_finisher.start();
  }
  m_timer.init();
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  ldout(m_image_ctx.cct, 20) << "enter" << dendl;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.shutdown();
    ldout(m_image_ctx.cct, 15) << "acquiring locks that shouldn't still be held" << dendl;
    Mutex::Locker retire_locker(m_log_retire_lock);
    RWLock::WLocker reader_locker(m_entry_reader_lock);
    Mutex::Locker dispatch_locker(m_deferred_dispatch_lock);
    Mutex::Locker append_locker(m_log_append_lock);
    Mutex::Locker locker(m_lock);
    Mutex::Locker bg_locker(m_blockguard_lock);
    Mutex::Locker bl_locker(m_entry_bl_lock);
    ldout(m_image_ctx.cct, 15) << "gratuitous locking complete" << dendl;
    delete m_image_writeback;
    m_image_writeback = nullptr;
    assert(m_deferred_ios.size() == 0);
    assert(m_ops_to_flush.size() == 0);
    assert(m_ops_to_append.size() == 0);
    assert(m_flush_ops_in_flight == 0);
    assert(m_unpublished_reserves == 0);
    assert(m_bytes_dirty == 0);
    assert(m_bytes_cached == 0);
    assert(m_bytes_allocated == 0);
    delete m_internal;
  }
  ldout(m_image_ctx.cct, 20) << "exit" << dendl;
}

template <typename ExtentsType>
class ExtentsSummary {
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;
  friend std::ostream &operator<<(std::ostream &os,
				  const ExtentsSummary &s) {
    os << "total_bytes=" << s.total_bytes << ", "
       << "first_image_byte=" << s.first_image_byte << ", "
       << "last_image_byte=" << s.last_image_byte << "";
    return os;
  };
  ExtentsSummary(const ExtentsType &extents) {
    total_bytes = 0;
    first_image_byte = 0;
    last_image_byte = 0;
    if (extents.empty()) return;
    /* These extents refer to image offsets between first_image_byte
     * and last_image_byte, inclusive, but we don't guarantee here
     * that they address all of those bytes. There may be gaps. */
    first_image_byte = extents.front().first;
    last_image_byte = first_image_byte + extents.front().second;
    for (auto &extent : extents) {
      /* Ignore zero length extents */
      if (extent.second) {
	total_bytes += extent.second;
	if (extent.first < first_image_byte) {
	  first_image_byte = extent.first;
	}
	if ((extent.first + extent.second) > last_image_byte) {
	  last_image_byte = extent.first + extent.second;
	}
      }
    }
  }
  const BlockExtent block_extent() {
    return BlockExtent(first_image_byte, last_image_byte);
  }
  const Extent image_extent() {
    return rwl::image_extent(block_extent());
  }
};

struct ImageExtentBuf : public Extent {
public:
  bufferlist m_bl;
  ImageExtentBuf(Extent extent, buffer::raw *buf = nullptr)
    : Extent(extent) {
    if (buf) {
      m_bl.append(buf);
    }
  }
  ImageExtentBuf(Extent extent, bufferlist bl)
    : Extent(extent), m_bl(bl) { }
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
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 99) << this << dendl;
    }
  }
  ~C_ReadRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 99) << this << dendl;
    }
  }

  virtual void finish(int r) override {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
    }
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
	if (extent.m_bl.length()) {
	  /* This was a hit */
	  assert(extent.second == extent.m_bl.length());
	  ++hits;
	  hit_bytes += extent.second;
	  m_out_bl->claim_append(extent.m_bl);
	} else {
	  /* This was a miss. */
	  ++misses;
	  miss_bytes += extent.second;
	  bufferlist miss_extent_bl;
	  miss_extent_bl.substr_of(m_miss_bl, miss_bl_offset, extent.second);
	  /* Add this read miss bufferlist to the output bufferlist */
	  m_out_bl->claim_append(miss_extent_bl);
	  /* Consume these bytes in the read miss bufferlist */
	  miss_bl_offset += extent.second;
	}
      }
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << " bl=" << *m_out_bl << dendl;
    }
    utime_t now = ceph_clock_now();
    assert((int)m_out_bl->length() == hit_bytes + miss_bytes);
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

static const bool COPY_PMEM_FOR_READ = true;
template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents &&image_extents, bufferlist *bl,
				     int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  utime_t now = ceph_clock_now();
  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }
  C_ReadRequest *read_ctx = new C_ReadRequest(cct, now, m_perfcounter, bl, on_finish);
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
		   << "image_extents=" << image_extents << ", "
		   << "bl=" << bl << ", "
		   << "on_finish=" << on_finish << dendl;
  }

  assert(m_initialized);
  bl->clear();
  m_perfcounter->inc(l_librbd_rwl_rd_req, 1);

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
   * Locking: These buffer objects hold a reference on the write log entries
   * they refer to. Log entries can't be retired until there are no references.
   * The GeneralWriteLogEntry references are released by the buffer destructor.
   */
  for (auto &extent : image_extents) {
    uint64_t extent_offset = 0;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));
    for (auto &map_entry : map_entries) {
      Extent entry_image_extent(image_extent(map_entry.block_extent));
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
      assert(map_entry.log_entry->is_writer());
      if (map_entry.log_entry->is_write() || map_entry.log_entry->is_writesame()) {
	/* Offset of the map entry into the log entry's buffer */
	uint64_t map_entry_buffer_offset = entry_image_extent.first - map_entry.log_entry->ram_entry.image_offset_bytes;
	/* Offset into the log entry buffer of this read hit */
	uint64_t read_buffer_offset = map_entry_buffer_offset + entry_offset;
	/* Create buffer object referring to pmem pool for this read hit */
	auto write_entry = static_pointer_cast<WriteLogEntry>(map_entry.log_entry);

	/* Make a bl for this hit extent. This will add references to the write_entry->pmem_bp */
	buffer::list hit_bl;
	if (COPY_PMEM_FOR_READ) {
	  buffer::list entry_bl_copy;
	  write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
	  entry_bl_copy.copy(read_buffer_offset, entry_hit_length, hit_bl);
	} else {
	  hit_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), read_buffer_offset, entry_hit_length);
	}
	assert(hit_bl.length() == entry_hit_length);

	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, hit_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);
      } else if (map_entry.log_entry->is_discard()) {
	auto discard_entry = static_pointer_cast<DiscardLogEntry>(map_entry.log_entry);
	if (RWL_VERBOSE_LOGGING) {
	  ldout(cct, 20) << "read hit on discard entry: log_entry=" << *discard_entry << dendl;
	}
	/* Discards read as zero, so we'll construct a bufferlist of zeros */
	bufferlist zero_bl;
	zero_bl.append_zero(entry_hit_length);
	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, zero_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);
      } else {
	ldout(cct, 02) << "Reading from log entry=" << *map_entry.log_entry
		       << " unimplemented" << dendl;
	assert(false);
      }

      /* Exclude RWL hit range from buffer and extent */
      extent_offset += entry_hit_length;
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << map_entry << dendl;
      }
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

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "miss_extents=" << read_ctx->m_miss_extents << ", "
		   << "miss_bl=" << read_ctx->m_miss_bl << dendl;
  }

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
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  int r = m_write_log_guard.detain(req.block_extent, &req, &cell);
  assert(r>=0);
  if (r > 0) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << "detaining guarded request due to in-flight requests: "
		     << "req=" << req << dendl;
    }
    return nullptr;
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "in-flight request cell: " << cell << dendl;
  }
  return cell;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_barrier_helper(GuardedRequest &req)
{
  BlockGuardCell *cell = nullptr;

  assert(m_blockguard_lock.is_locked_by_me());
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }

  if (m_barrier_in_progress) {
    req.guard_ctx->m_state.queued = true;
    m_awaiting_barrier.push_back(req);
  } else {
    bool barrier = req.guard_ctx->m_state.barrier;
    if (barrier) {
      m_barrier_in_progress = true;
      req.guard_ctx->m_state.current_barrier = true;
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

  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  {
    Mutex::Locker locker(m_blockguard_lock);
    cell = detain_guarded_request_barrier_helper(req);
  }
  if (cell) {
    req.guard_ctx->m_cell = cell;
    req.guard_ctx->complete(0);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *released_cell)
{
  CephContext *cct = m_image_ctx.cct;
  WriteLogGuard::BlockOperations block_reqs;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "released_cell=" << released_cell << dendl;
  }

  {
    Mutex::Locker locker(m_blockguard_lock);
    m_write_log_guard.release(released_cell, &block_reqs);

    for (auto &req : block_reqs) {
      req.guard_ctx->m_state.detained = true;
      BlockGuardCell *detained_cell = detain_guarded_request_helper(req);
      if (detained_cell) {
	if (req.guard_ctx->m_state.current_barrier) {
	  /* The current barrier is acquiring the block guard, so now we know its cell */
	  m_barrier_cell = detained_cell;
	  /* detained_cell could be == released_cell here */
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(cct, 20) << "current barrier cell=" << detained_cell << " req=" << req << dendl;
	  }
	}
	req.guard_ctx->m_cell = detained_cell;
	m_work_queue.queue(req.guard_ctx);
      }
    }

    if (m_barrier_in_progress && (released_cell == m_barrier_cell)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << "current barrier released cell=" << released_cell << dendl;
      }
      /* The released cell is the current barrier request */
      m_barrier_in_progress = false;
      m_barrier_cell = nullptr;
      /* Move waiting requests into the blockguard. Stop if there's another barrier */
      while (!m_barrier_in_progress && !m_awaiting_barrier.empty()) {
	auto &req = m_awaiting_barrier.front();
	if (RWL_VERBOSE_LOGGING) {
	  ldout(cct, 20) << "submitting queued request to blockguard: " << req << dendl;
	}
	BlockGuardCell *detained_cell = detain_guarded_request_barrier_helper(req);
	if (detained_cell) {
	  req.guard_ctx->m_cell = detained_cell;
	  m_work_queue.queue(req.guard_ctx);
	}
	m_awaiting_barrier.pop_front();
      }
    }
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "exit" << dendl;
  }
}

struct WriteBufferAllocation {
  unsigned int allocation_size = 0;
  pobj_action buffer_alloc_action;
  TOID(uint8_t) buffer_oid = OID_NULL;
  bool allocated = false;
  utime_t allocation_lat;
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
template <typename T>
struct C_BlockIORequest : public C_GuardedBlockIORequest<T> {
  using C_GuardedBlockIORequest<T>::rwl;
  Extents m_image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_finish_called = {false};
  ExtentsSummary<Extents> m_image_extents_summary;
  utime_t m_arrived_time;
  utime_t m_allocated_time;               /* When allocation began */
  utime_t m_dispatched_time;              /* When dispatch began */
  utime_t m_user_req_completed_time;
  bool m_detained = false;                /* Detained in blockguard (overlapped with a prior IO) */
  std::atomic<bool> m_deferred = {false}; /* Deferred because this or a prior IO had to wait for write resources */
  bool m_waited_lanes = false;            /* This IO waited for free persist/replicate lanes */
  bool m_waited_entries = false;          /* This IO waited for free log entries */
  bool m_waited_buffers = false;          /* This IO waited for data buffers (pmemobj_reserve() failed) */
  friend std::ostream &operator<<(std::ostream &os,
				  const C_BlockIORequest<T> &req) {
    os << "m_image_extents=[" << req.m_image_extents << "], "
       << "m_image_extents_summary=[" << req.m_image_extents_summary << "], "
       << "bl=" << req.bl << ", "
       << "user_req=" << req.user_req << ", "
       << "m_user_req_completed=" << req.m_user_req_completed << ", "
       << "deferred=" << req.m_deferred << ", "
       << "detained=" << req.m_detained << ", "
       << "m_waited_lanes=" << req.m_waited_lanes << ", "
       << "m_waited_entries=" << req.m_waited_entries << ", "
       << "m_waited_buffers=" << req.m_waited_buffers << "";
    return os;
  };
  C_BlockIORequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		   bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_GuardedBlockIORequest<T>(rwl), m_image_extents(std::move(image_extents)),
      bl(std::move(bl)), fadvise_flags(fadvise_flags),
      user_req(user_req), m_image_extents_summary(m_image_extents), m_arrived_time(arrived) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
    /* Remove zero length image extents from input */
    for (auto it = m_image_extents.begin(); it != m_image_extents.end(); ) {
      if (0 == it->second) {
	it = m_image_extents.erase(it);
	continue;
      }
      ++it;
    }
  }

  virtual ~C_BlockIORequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void complete_user_request(int r) {
    bool initial = false;
    if (m_user_req_completed.compare_exchange_strong(initial, true)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 15) << this << " completing user req" << dendl;
      }
      m_user_req_completed_time = ceph_clock_now();
      user_req->complete(r);
    } else {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << this << " user req already completed" << dendl;
      }
    }
  }

  void finish(int r) {
    ldout(rwl.m_image_ctx.cct, 20) << this << dendl;

    complete_user_request(r);
    bool initial = false;
    if (m_finish_called.compare_exchange_strong(initial, true)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 15) << this << " finishing" << dendl;
      }
      finish_req(0);
    } else {
      ldout(rwl.m_image_ctx.cct, 20) << this << " already finished" << dendl;
      assert(0);
    }
  }

  virtual void finish_req(int r) = 0;

  virtual bool alloc_resources() = 0;

  void deferred() {
    bool initial = false;
    if (m_deferred.compare_exchange_strong(initial, true)) {
      deferred_handler();
    }
  }

  virtual void deferred_handler() = 0;

  virtual void dispatch()  = 0;

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
template <typename T>
struct C_WriteRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  WriteRequestResources m_resources;
  unique_ptr<WriteLogOperationSet<T>> m_op_set = nullptr;
  bool m_do_early_flush = false;
  std::atomic<int> m_appended = {0};
  bool m_queued = false;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_WriteRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << " m_resources.allocated=" << req.m_resources.allocated;
    if (req.m_op_set) {
       os << "m_op_set=" << *req.m_op_set;
    }
    return os;
  };

  C_WriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_WriteRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_WriteRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_WriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " write_req=" << this << " cell=" << guard_ctx.m_cell << dendl;
    }

    assert(guard_ctx.m_cell);
    this->m_detained = guard_ctx.m_state.detained; /* overlapped */
    this->m_queued = guard_ctx.m_state.queued; /* queued behind at least one barrier */
    this->set_cell(guard_ctx.m_cell);
  }

  /* Common finish to plain write and compare-and-write (if it writes) */
  virtual void finish_req(int r) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;
    }

    /* Completed to caller by here (in finish(), which calls this) */
    utime_t now = ceph_clock_now();
    rwl.release_write_lanes(this);
    this->release_cell(); /* TODO: Consider doing this in appending state */
    update_req_stats(now);
  }

  /* Compare and write will override this */
  virtual void update_req_stats(utime_t &now) {
    for (auto &allocation : this->m_resources.buffers) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_log_op_alloc_t, allocation.allocation_lat);
      rwl.m_perfcounter->hinc(l_librbd_rwl_log_op_alloc_t_hist,
			      allocation.allocation_lat.to_nsec(), allocation.allocation_size);
    }
    if (this->m_detained) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
    }
    if (this->m_queued) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_queued, 1);
    }
    if (this->m_deferred) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def, 1);
    }
    if (this->m_waited_lanes) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_lanes, 1);
    }
    if (this->m_waited_entries) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_log, 1);
    }
    if (this->m_waited_buffers) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_buf, 1);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
    utime_t comp_latency = now - this->m_arrived_time;
    if (!(this->m_waited_entries || this->m_waited_buffers || this->m_deferred)) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_latency, comp_latency);
      rwl.m_perfcounter->hinc(l_librbd_rwl_nowait_wr_latency_hist, comp_latency.to_nsec(),
			      this->m_image_extents_summary.total_bytes);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_caller_latency,
			      this->m_user_req_completed_time - this->m_arrived_time);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_latency, comp_latency);
    rwl.m_perfcounter->hinc(l_librbd_rwl_wr_latency_hist, comp_latency.to_nsec(),
			    this->m_image_extents_summary.total_bytes);
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_caller_latency, this->m_user_req_completed_time - this->m_arrived_time);
  }

  virtual bool alloc_resources() override;

  /* Plain writes will allocate one buffer per request extent */
  virtual void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) {
    for (auto &extent : this->m_image_extents) {
      m_resources.buffers.emplace_back();
      struct WriteBufferAllocation &buffer = m_resources.buffers.back();
      buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
      buffer.allocated = false;
      bytes_cached += extent.second;
      if (extent.second > buffer.allocation_size) {
	buffer.allocation_size = extent.second;
      }
    }
    bytes_dirtied = bytes_cached;
  }

  void deferred_handler() override { }

  void dispatch() override;

  virtual void setup_log_operations() {
    for (auto &extent : this->m_image_extents) {
      /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
      auto operation =
	std::make_shared<WriteLogOperation<T>>(*m_op_set, extent.first, extent.second);
      m_op_set->operations.emplace_back(operation);
    }
  }

  virtual void schedule_append() {
    assert(++m_appended == 1);
    if (m_do_early_flush) {
      /* This caller is waiting for persist, so we'll use their thread to
       * expedite it */
      rwl.flush_pmem_buffer(this->m_op_set->operations);
      rwl.schedule_append(this->m_op_set->operations);
    } else {
      /* This is probably not still the caller's thread, so do the payload
       * flushing/replicating later. */
      rwl.schedule_flush_and_append(this->m_op_set->operations);
    }
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
template <typename T>
struct C_FlushRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  std::atomic<bool> m_log_entry_allocated = {false};
  bool m_internal = false;
  std::shared_ptr<SyncPoint<T>> to_append;
  std::shared_ptr<SyncPointLogOperation<T>> op;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_FlushRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << " m_log_entry_allocated=" << req.m_log_entry_allocated;
    return os;
  };

  C_FlushRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_FlushRequest() {
  }

  template <typename... U>
  static inline std::shared_ptr<C_FlushRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_FlushRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << "flush_req=" << this
				     << " cell=" << this->get_cell() << dendl;
    }
    /* Block guard already released */
    assert(!this->get_cell());

    /* Completed to caller by here */
    utime_t now = ceph_clock_now();
    rwl.m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - this->m_arrived_time);
  }

  bool alloc_resources() override;

  void deferred_handler() override {
    rwl.m_perfcounter->inc(l_librbd_rwl_aio_flush_def, 1);
  }

  void dispatch() override;

  const char *get_name() const override {
    return "C_FlushRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this discard. As in the
 * case of write, the block guard is not released until the discard persists
 * everywhere.
 */
template <typename T>
struct C_DiscardRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  std::atomic<bool> m_log_entry_allocated = {false};
  bool m_skip_partial_discard;
  std::shared_ptr<DiscardLogOperation<T>> op;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_DiscardRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << "m_skip_partial_discard=" << req.m_skip_partial_discard;
    if (req.op) {
      os << "op=[" << *req.op << "]";
    } else {
      os << "op=nullptr";
    }
    return os;
  };

  C_DiscardRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		   const int skip_partial_discard, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), bufferlist(), 0, user_req),
    m_skip_partial_discard(skip_partial_discard) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_DiscardRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_DiscardRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_DiscardRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {}

  bool alloc_resources() override;

  void deferred_handler() override { }

  void dispatch() override;

  const char *get_name() const override {
    return "C_DiscardRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this compare and write. The
 * block guard is acquired before the read begins to guarantee atomicity of this
 * operation.  If this results in a write, the block guard will be released
 * when the write completes to all replicas.
 */
template <typename T>
struct C_CompAndWriteRequest : public C_WriteRequest<T> {
  using C_BlockIORequest<T>::rwl;
  bool m_compare_succeeded = false;
  uint64_t *m_mismatch_offset;
  bufferlist m_cmp_bl;
  bufferlist m_read_bl;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_CompAndWriteRequest<T> &req) {
    os << (C_WriteRequest<T>&)req
       << "m_cmp_bl=" << req.m_cmp_bl << ", "
       << "m_read_bl=" << req.m_read_bl << ", "
       << "m_compare_succeeded=" << req.m_compare_succeeded << ", "
       << "m_mismatch_offset=" << req.m_mismatch_offset;
    return os;
  };

  C_CompAndWriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
			bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
			int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
    m_mismatch_offset(mismatch_offset), m_cmp_bl(std::move(cmp_bl)) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_CompAndWriteRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_CompAndWriteRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_CompAndWriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) override {
    if (m_compare_succeeded) {
      C_WriteRequest<T>::finish_req(r);
    } else {
      utime_t now = ceph_clock_now();
      update_req_stats(now);
    }
  }

  void update_req_stats(utime_t &now) override {
    /* Compare-and-write stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    if (!m_compare_succeeded) {
      rwl.m_perfcounter->inc(l_librbd_rwl_cmp_fails, 1);
    }
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_cmp_latency, comp_latency);
  }

  /*
   * Compare and write doesn't implement alloc_resources(), deferred_handler(),
   * or dispatch(). We use the implementation in C_WriteRequest(), and only if the
   * compare phase succeeds and a write is actually performed.
   */

  const char *get_name() const override {
    return "C_CompAndWriteRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this write same.
 *
 * A writesame allocates and persists a data buffer like a write, but the
 * data buffer is usually much shorter than the write same.
 */
template <typename T>
struct C_WriteSameRequest : public C_WriteRequest<T> {
  using C_BlockIORequest<T>::rwl;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_WriteSameRequest<T> &req) {
    os << (C_WriteRequest<T>&)req;
    return os;
  };

  C_WriteSameRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		     bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_WriteSameRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_WriteSameRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_WriteSameRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  /* Inherit finish_req() from C_WriteRequest */

  void update_req_stats(utime_t &now) override {
    /* Write same stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_ws_latency, comp_latency);
  }

  /* Write sames will allocate one buffer, the size of the repeating pattern */
  void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) override {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    assert(this->m_image_extents.size() == 1);
    bytes_dirtied += this->m_image_extents[0].second;
    auto pattern_length = this->bl.length();
    this->m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    bytes_cached += pattern_length;
    if (pattern_length > buffer.allocation_size) {
      buffer.allocation_size = pattern_length;
    }
  }

  virtual void setup_log_operations() {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    /* Write same adds a single WS log op to the vector, corresponding to the single buffer item created above */
    assert(this->m_image_extents.size() == 1);
    auto extent = this->m_image_extents.front();
    /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
    auto operation =
      std::make_shared<WriteSameLogOperation<T>>(*this->m_op_set.get(), extent.first, extent.second, this->bl.length());
    this->m_op_set->operations.emplace_back(operation);
  }

  /*
   * Write same doesn't implement alloc_resources(), deferred_handler(), or
   * dispatch(). We use the implementation in C_WriteRequest().
   */

  void schedule_append() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this->get_name() << " " << this << dendl;
    }
    C_WriteRequest<T>::schedule_append();
  }

  const char *get_name() const override {
    return "C_WriteSameRequest";
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
  GenericLogOperationsT ops;
  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  do {
    ops.clear();

    {
      Mutex::Locker locker(m_lock);
      if (!appending && m_appending) {
	/* Another thread is appending */
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 15) << "Another thread is appending" << dendl;
	}
	return;
      }
      if (m_ops_to_append.size()) {
	appending = true;
	m_appending = true;
	auto last_in_batch = m_ops_to_append.begin();
	unsigned int ops_to_append = m_ops_to_append.size();
	if (ops_to_append > ops_appended_together) {
	  ops_to_append = ops_appended_together;
	}
	std::advance(last_in_batch, ops_to_append);
	ops.splice(ops.end(), m_ops_to_append, m_ops_to_append.begin(), last_in_batch);
	ops_remain = true; /* Always check again before leaving */
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "appending " << ops.size() << ", " << m_ops_to_append.size() << " remain" << dendl;
	}
      } else {
	ops_remain = false;
	if (appending) {
	  appending = false;
	  m_appending = false;
	}
      }
    }

    if (ops.size()) {
      Mutex::Locker locker(m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    }

    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      complete_op_log_entries(std::move(ops), append_result);
    }
  } while (ops_remain);
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_appender()
{
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
    m_work_queue.queue(append_ctx);
  }
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsT &ops)
{
  bool need_finisher;
  GenericLogOperationsVectorT appending;

  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_append.empty() && !m_appending;
    m_ops_to_append.splice(m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    enlist_op_appender();
  }

  for (auto &op : appending) {
    op->appending();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsVectorT &ops)
{
  GenericLogOperationsT to_append(ops.begin(), ops.end());

  schedule_append(to_append);
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationSharedPtrT op)
{
  GenericLogOperationsT to_append { op };

  schedule_append(to_append);
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperationsT ops;
  bool ops_remain = false;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
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
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
	}
	std::advance(last_in_batch, ops_to_flush);
	ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
	ops_remain = !m_ops_to_flush.empty();
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", " << m_ops_to_flush.size() << " remain" << dendl;
	}
      } else {
	ops_remain = false;
      }
    }

    if (ops_remain) {
      enlist_op_flusher();
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

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
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
    m_work_queue.queue(flush_ctx);
  }
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVectorT &ops)
{
  GenericLogOperationsT to_flush(ops.begin(), ops.end());
  bool need_finisher;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  {
    Mutex::Locker locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_time = ceph_clock_now();
      auto write_entry = operation->get_write_log_entry();

      pmemobj_flush(m_internal->m_log_pool, write_entry->pmem_buffer, write_entry->write_bytes());
    }
  }

  /* Drain once for all */
  pmemobj_drain(m_internal->m_log_pool);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_comp_time = now;
    } else {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
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
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperationsT &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  assert(m_log_append_lock.is_locked_by_me());

  /* Allocate the (already reserved) log entries */
  Mutex::Locker locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = m_first_free_entry;
    m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;
    //if (m_log_entries.back()) {
    //  assert((m_log_entries.back()->log_entry_index + 1) % m_total_log_entries == entry_index);
    //}
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
    }
  }
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVectorT &ops)
{
  if (ops.empty()) return;

  if (ops.size() > 1) {
    assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
			       << "start address=" << ops.front()->get_log_entry()->pmem_entry << " "
			       << "bytes=" << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
			       << dendl;
  }
  pmemobj_flush(m_internal->m_log_pool,
		ops.front()->get_log_entry()->pmem_entry,
		ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry)));
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperationsT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperationsVectorT entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;

  assert(m_log_append_lock.is_locked_by_me());

  if (ops.empty()) return 0;
  entries_to_flush.reserve(ops_appended_together);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
	  operation->get_log_entry()->log_entry_index) {
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
				     << "operation=[" << *operation << "]" << dendl;
	}
	flush_op_log_entries(entries_to_flush);
	entries_to_flush.clear();
	now = ceph_clock_now();
      }
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "from " << &operation->get_log_entry()->ram_entry << " "
				 << "to " << operation->get_log_entry()->pmem_entry << " "
				 << "operation=[" << *operation << "]" << dendl;
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 05) << "APPENDING: index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "operation=[" << *operation << "]" << dendl;
    }
    operation->m_log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "APPENDING: index="
				 << operation->get_log_entry()->log_entry_index << " "
				 << "pmem_entry=[" << *operation->get_log_entry()->pmem_entry << "]" << dendl;
    }
    entries_to_flush.push_back(operation);
  }
  flush_op_log_entries(entries_to_flush);

  /* Drain once for all */
  pmemobj_drain(m_internal->m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  TX_BEGIN(m_internal->m_log_pool) {
    D_RW(pool_root)->first_free_entry = m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->is_write() || operation->is_writesame()) {
	auto write_op = (std::shared_ptr<WriteLogOperationT>&) operation;
	pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1);
      } else {
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
	}
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size() << " log entries (" << m_log_pool_name << ")" << dendl;
    assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(l_librbd_rwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->m_log_append_comp_time = tx_end;
  }

  return ret;
}

/*
 * Complete a set of write ops with the result of append_op_entries.
 */
template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  GenericLogEntries dirty_entries;
  int published_reserves = 0;
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << __func__ << ": completing" << dendl;
  }
  for (auto &op : ops) {
    utime_t now = ceph_clock_now();
    auto log_entry = op->get_log_entry();
    log_entry->completed = true;
    if (op->is_writing_op()) {
      op->get_gen_write_op()->sync_point->log_entry->m_writes_completed++;
      dirty_entries.push_back(log_entry);
    }
    if (op->is_write() || op->is_writesame()) {
      published_reserves++;
    }
    if (op->is_discard()) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << __func__ << ": completing discard" << dendl;
      }
    }
    op->complete(result);
    if (op->is_write()) {
      m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_buf_t, op->m_buf_persist_time - op->m_dispatch_time);
    }
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_app_t, op->m_log_append_time - op->m_dispatch_time);
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_cmp_t, now - op->m_dispatch_time);
    m_perfcounter->hinc(l_librbd_rwl_log_op_dis_to_cmp_t_hist, utime_t(now - op->m_dispatch_time).to_nsec(),
			log_entry->ram_entry.write_bytes);
    if (op->is_write()) {
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
}

/*
 * Push op log entry completion to a WQ.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  m_async_complete_ops++;
  m_async_op_tracker.start_op();
  Context *complete_ctx = new FunctionContext([this, ops=move(ops), result](int r) {
      auto captured_ops = std::move(ops);
      complete_op_log_entries(std::move(captured_ops), result);
      m_async_complete_ops--;
      m_async_op_tracker.finish_op();
    });
  if (use_finishers) {
    m_on_persist_finisher.queue(complete_ctx);
  } else {
    m_work_queue.queue(complete_ctx);
  }
}

/**
 * Attempts to allocate log resources for a write. Returns true if successful.
 *
 * Resources include 1 lane per extent, 1 log entry per extent, and the payload
 * data space for each extent.
 *
 * Lanes are released after the write persists via release_write_lanes()
 */
template <typename T>
bool C_WriteRequest<T>::alloc_resources()
{
  bool alloc_succeeds = true;
  bool no_space = false;
  utime_t alloc_start = ceph_clock_now();
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;

  assert(!rwl.m_lock.is_locked_by_me());
  assert(!m_resources.allocated);
  m_resources.buffers.reserve(this->m_image_extents.size());
  {
    Mutex::Locker locker(rwl.m_lock);
    if (rwl.m_free_lanes < this->m_image_extents.size()) {
      this->m_waited_lanes = true;
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << "not enough free lanes (need "
				       <<  this->m_image_extents.size()
				       << ", have " << rwl.m_free_lanes << ") "
				       << *this << dendl;
      }
      alloc_succeeds = false;
      /* This isn't considered a "no space" alloc fail. Lanes are a throttling mechanism. */
    }
    if (rwl.m_free_log_entries < this->m_image_extents.size()) {
      this->m_waited_entries = true;
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << "not enough free entries (need "
				       <<  this->m_image_extents.size()
				       << ", have " << rwl.m_free_log_entries << ") "
				       << *this << dendl;
      }
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
    /* Don't attempt buffer allocate if we've exceeded the "full" threshold */
    if (rwl.m_bytes_allocated > rwl.m_bytes_allocated_cap) {
      if (!this->m_waited_buffers) {
	this->m_waited_buffers = true;
	if (RWL_VERBOSE_LOGGING) {
	  ldout(rwl.m_image_ctx.cct, 1) << "Waiting for allocation cap (cap=" << rwl.m_bytes_allocated_cap
					<< ", allocated=" << rwl.m_bytes_allocated
					<< ") in write [" << *this << "]" << dendl;
	}
      }
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
  }

  if (alloc_succeeds) {
    setup_buffer_resources(bytes_cached, bytes_dirtied);
  }

  if (alloc_succeeds) {
    for (auto &buffer : m_resources.buffers) {
      bytes_allocated += buffer.allocation_size;
      utime_t before_reserve = ceph_clock_now();
      buffer.buffer_oid = pmemobj_reserve(rwl.m_internal->m_log_pool,
					  &buffer.buffer_alloc_action,
					  buffer.allocation_size,
					  0 /* Object type */);
      buffer.allocation_lat = ceph_clock_now() - before_reserve;
      if (TOID_IS_NULL(buffer.buffer_oid)) {
	if (!this->m_waited_buffers) {
	  this->m_waited_buffers = true;
	}
	if (RWL_VERBOSE_LOGGING) {
	  ldout(rwl.m_image_ctx.cct, 5) << "can't allocate all data buffers: "
					<< pmemobj_errormsg() << ". "
					<< *this << dendl;
	}
	alloc_succeeds = false;
	no_space = true; /* Entries need to be retired */
	break;
      } else {
	buffer.allocated = true;
      }
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo
				       << "." << buffer.buffer_oid.oid.off
				       << ", size=" << buffer.allocation_size << dendl;
      }
    }
  }

  if (alloc_succeeds) {
    unsigned int num_extents = this->m_image_extents.size();
    Mutex::Locker locker(rwl.m_lock);
    /* We need one free log entry per extent (each is a separate entry), and
     * one free "lane" for remote replication. */
    if ((rwl.m_free_lanes >= num_extents) &&
	(rwl.m_free_log_entries >= num_extents)) {
      rwl.m_free_lanes -= num_extents;
      rwl.m_free_log_entries -= num_extents;
      rwl.m_unpublished_reserves += num_extents;
      rwl.m_bytes_allocated += bytes_allocated;
      rwl.m_bytes_cached += bytes_cached;
      rwl.m_bytes_dirty += bytes_dirtied;
      m_resources.allocated = true;
    } else {
      alloc_succeeds = false;
    }
  }

  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : m_resources.buffers) {
      if (buffer.allocated) {
	pmemobj_cancel(rwl.m_internal->m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }
    m_resources.buffers.clear();
    if (no_space) {
      /* Expedite flushing and/or retiring */
      Mutex::Locker locker(rwl.m_lock);
      rwl.m_alloc_failed_since_retire = true;
      rwl.m_last_alloc_fail = ceph_clock_now();
    }
  }

 this->m_allocated_time = alloc_start;
  return alloc_succeeds;
}

/**
 * Dispatch as many deferred writes as possible
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_deferred_writes(void)
{
  C_BlockIORequestT *front_req = nullptr;     /* req still on front of deferred list */
  C_BlockIORequestT *allocated_req = nullptr; /* req that was allocated, and is now off the list */
  bool allocated = false; /* front_req allocate succeeded */
  bool cleared_dispatching_flag = false;

  /* If we can't become the dispatcher, we'll exit */
  {
    Mutex::Locker locker(m_lock);
    if (m_dispatching_deferred_ops ||
	!m_deferred_ios.size()) {
      return;
    }
    m_dispatching_deferred_ops = true;
  }

  /* There are ops to dispatch, and this should be the only thread dispatching them */
  {
    Mutex::Locker deferred_dispatch(m_deferred_dispatch_lock);
    do {
      {
	Mutex::Locker locker(m_lock);
	assert(m_dispatching_deferred_ops);
	if (allocated) {
	  /* On the 2..n-1 th time we get m_lock, front_req->alloc_resources() will
	   * have succeeded, and we'll need to pop it off the deferred ops list
	   * here. */
	  assert(front_req);
	  assert(!allocated_req);
	  m_deferred_ios.pop_front();
	  allocated_req = front_req;
	  front_req = nullptr;
	  allocated = false;
	}
	assert(!allocated);
	if (!allocated && front_req) {
	  /* front_req->alloc_resources() failed on the last iteration. We'll stop dispatching. */
	  front_req = nullptr;
	  assert(!cleared_dispatching_flag);
	  m_dispatching_deferred_ops = false;
	  cleared_dispatching_flag = true;
	} else {
	  assert(!front_req);
	  if (m_deferred_ios.size()) {
	    /* New allocation candidate */
	    front_req = m_deferred_ios.front();
	  } else {
	    assert(!cleared_dispatching_flag);
	    m_dispatching_deferred_ops = false;
	    cleared_dispatching_flag = true;
	  }
	}
      }
      /* Try allocating for front_req before we decide what to do with allocated_req
       * (if any) */
      if (front_req) {
	assert(!cleared_dispatching_flag);
	allocated = front_req->alloc_resources();
      }
      if (allocated_req && front_req && allocated) {
	/* Push dispatch of the first allocated req to a wq */
	m_work_queue.queue(new FunctionContext(
	  [this, allocated_req](int r) {
	    allocated_req->dispatch();
	  }), 0);
	allocated_req = nullptr;
      }
      assert(!(allocated_req && front_req && allocated));

      /* Continue while we're still considering the front of the deferred ops list */
    } while (front_req);
    assert(!allocated);
  }
  ceph_assert(cleared_dispatching_flag);

  /* If any deferred requests were allocated, the last one will still be in allocated_req */
  if (allocated_req) {
    allocated_req->dispatch();
  }
}

/**
 * Returns the lanes used by this write, and attempts to dispatch the next
 * deferred write
 */
template <typename I>
void ReplicatedWriteLog<I>::release_write_lanes(C_WriteRequestT *write_req)
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
void ReplicatedWriteLog<I>::alloc_and_dispatch_io_req(C_BlockIORequestT *req)
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
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "dispatching" << dendl;
      }
      req->dispatch();
    } else {
      req->deferred();
      {
	Mutex::Locker locker(m_lock);
	m_deferred_ios.push_back(req);
      }
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "deferred IOs: " << m_deferred_ios.size() << dendl;
      }
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
template <typename T>
void C_WriteRequest<T>::dispatch()
{
  CephContext *cct = rwl.m_image_ctx.cct;
  GeneralWriteLogEntries log_entries;
  DeferredContexts on_exit;
  utime_t now = ceph_clock_now();
  auto write_req_sp = shared_from_this();
  this->m_dispatched_time = now;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(rwl.m_internal->m_log_pool, struct WriteLogPoolRoot);

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 15) << "name: " << rwl.m_image_ctx.name << " id: " << rwl.m_image_ctx.id
		   << "write_req=" << this << " cell=" << this->get_cell() << dendl;
  }

  {
    uint64_t buffer_offset = 0;
    Mutex::Locker locker(rwl.m_lock);
    Context *set_complete = this;
    if (use_finishers) {
      set_complete = new C_OnFinisher(this, &rwl.m_on_persist_finisher);
    }
    if ((!rwl.m_persist_on_flush && rwl.m_current_sync_point->log_entry->m_writes_completed) ||
	(rwl.m_current_sync_point->log_entry->m_writes > MAX_WRITES_PER_SYNC_POINT) ||
	(rwl.m_current_sync_point->log_entry->m_bytes > MAX_BYTES_PER_SYNC_POINT)) {
      /* Create new sync point and persist the previous one. This sequenced
       * write will bear a sync gen number shared with no already completed
       * writes. A group of sequenced writes may be safely flushed concurrently
       * if they all arrived before any of them completed. We'll insert one on
       * an aio_flush() from the application. Here we're inserting one to cap
       * the number of bytes and writes per sync point. When the application is
       * not issuing flushes, we insert sync points to record some observed
       * write concurrency information that enables us to safely issue >1 flush
       * write (for writes observed here to have been in flight simultaneously)
       * at a time in persist-on-write mode.
       */
      rwl.flush_new_sync_point(nullptr, on_exit);
    }
    m_op_set =
      make_unique<WriteLogOperationSet<T>>(rwl, now, rwl.m_current_sync_point, rwl.m_persist_on_flush,
					   this->m_image_extents_summary.block_extent(), set_complete);
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << "write_req=" << this << " m_op_set=" << m_op_set.get() << dendl;
    }
    assert(m_resources.allocated);
    /* m_op_set->operations initialized differently for plain write or write same */
    this->setup_log_operations();
    auto allocation = m_resources.buffers.begin();
    for (auto &gen_op : m_op_set->operations) {
      /* A WS is also a write */
      auto operation = gen_op->get_write_op();
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << "write_req=" << this << " m_op_set=" << m_op_set.get()
		       << " operation=" << operation << dendl;
      }
      log_entries.emplace_back(operation->log_entry);
      rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);

      operation->log_entry->ram_entry.has_data = 1;
      operation->log_entry->ram_entry.write_data = allocation->buffer_oid;
      // TODO: make std::shared_ptr
      operation->buffer_alloc = &(*allocation);
      assert(!TOID_IS_NULL(operation->log_entry->ram_entry.write_data));
      operation->log_entry->pmem_buffer = D_RW(operation->log_entry->ram_entry.write_data);
      operation->log_entry->ram_entry.sync_gen_number = rwl.m_current_sync_gen;
      if (m_op_set->m_persist_on_flush) {
	/* Persist on flush. Sequence #0 is never used. */
	operation->log_entry->ram_entry.write_sequence_number = 0;
      } else {
	/* Persist on write */
	operation->log_entry->ram_entry.write_sequence_number = ++rwl.m_last_op_sequence_num;
	operation->log_entry->ram_entry.sequenced = 1;
      }
      operation->log_entry->ram_entry.sync_point = 0;
      operation->log_entry->ram_entry.discard = 0;
      operation->bl.substr_of(this->bl, buffer_offset,
			      operation->log_entry->write_bytes());
      buffer_offset += operation->log_entry->write_bytes();
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << "operation=[" << *operation << "]" << dendl;
      }
      allocation++;
    }
  }

  /* All extent ops subs created */
  m_op_set->m_extent_ops_appending->activate();
  m_op_set->m_extent_ops_persist->activate();

  /* Write data */
  for (auto &operation : m_op_set->operations) {
    /* operation is a shared_ptr, so write_op is only good as long as operation is in scope */
    auto write_op = operation->get_write_op();
    assert(write_op != nullptr);
    bufferlist::iterator i(&write_op->bl);
    rwl.m_perfcounter->inc(l_librbd_rwl_log_op_bytes, write_op->log_entry->write_bytes());
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << write_op->bl << dendl;
    }
    i.copy((unsigned)write_op->log_entry->write_bytes(), (char*)write_op->log_entry->pmem_buffer);
  }

  /* Adding these entries to the map of blocks to log entries makes them
   * readable by this application. They aren't persisted yet, so they may
   * disappear after certain failures. We'll indicate our guaratee of the
   * persistence of these writes with the completion of this request, or the
   * following aio_flush(), according to the configured policy. */
  rwl.m_blocks_to_log_entries.add_log_entries(log_entries);

  /*
   * Entries are added to m_log_entries in alloc_op_log_entries() when their
   * order is established. They're added to m_dirty_log_entries when the write
   * completes to all replicas. They must not be flushed before then. We don't
   * prevent the application from reading these before they persist. If we
   * supported coherent shared access, that might be a problem (the write could
   * fail after another initiator had read it). As it is the cost of running
   * reads through the block guard (and exempting them from the barrier, which
   * doesn't need to apply to them) to prevent reading before the previous
   * write of that data persists doesn't seem justified.
   */

  if (rwl.m_persist_on_flush_early_user_comp &&
      m_op_set->m_persist_on_flush) {
    /*
     * We're done with the caller's buffer, and not guaranteeing
     * persistence until the next flush. The block guard for this
     * write_req will not be released until the write is persisted
     * everywhere, but the caller's request can complete now.
     */
    this->complete_user_request(0);
  }

  bool append_deferred = false;
  {
    Mutex::Locker locker(rwl.m_lock);
    if (!m_op_set->m_persist_on_flush &&
	m_op_set->sync_point->earlier_sync_point) {
      /* In persist-on-write mode, we defer the append of this write until the
       * previous sync point is appending (meaning all the writes before it are
       * persisted and that previous sync point can now appear in the
       * log). Since we insert sync points in persist-on-write mode when writes
       * have already completed to the current sync point, this limits us to
       * one inserted sync point in flight at a time, and gives the next
       * inserted sync point some time to accumulate a few writes if they
       * arrive soon. Without this we can insert an absurd number of sync
       * points, each with one or two writes. That uses a lot of log entries,
       * and limits flushing to very few writes at a time. */
      m_do_early_flush = false;
      Context *schedule_append_ctx = new FunctionContext([this, write_req_sp](int r) {
	  write_req_sp->schedule_append();
	});
      m_op_set->sync_point->earlier_sync_point->m_on_sync_point_appending.push_back(schedule_append_ctx);
      append_deferred = true;
    } else {
      /* The prior sync point is done, so we'll schedule append here. If this is
       * persist-on-write, and probably still the caller's thread, we'll use this
       * caller's thread to perform the persist & replication of the payload
       * buffer. */
      m_do_early_flush =
	!(this->m_detained || this->m_queued || this->m_deferred || m_op_set->m_persist_on_flush);
    }
  }
  if (!append_deferred) {
    this->schedule_append();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
				      bufferlist&& bl,
				      int fadvise_flags,
				      Context *on_finish) {
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_wr_req, 1);

  assert(m_initialized);
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  auto *write_req =
    C_WriteRequestT::create(*this, now, std::move(image_extents), std::move(bl), fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req](GuardedRequestFunctionContext &guard_ctx) {
      write_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(write_req);
    });

  detain_guarded_request(GuardedRequest(write_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename T>
bool C_DiscardRequest<T>::alloc_resources() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(!m_log_entry_allocated);
  bool allocated_here = false;
  Mutex::Locker locker(rwl.m_lock);
  if (rwl.m_free_log_entries) {
    rwl.m_free_log_entries--;
    /* No bytes are allocated for a discard, but we count the discarded bytes
     * as dirty.  This means it's possible to have more bytes dirty than
     * there are bytes cached or allocated. */
    rwl.m_bytes_dirty += op->log_entry->bytes_dirty();
    m_log_entry_allocated = true;
    allocated_here = true;
  }
  return allocated_here;
}

template <typename T>
void C_DiscardRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(m_log_entry_allocated);
  this->m_dispatched_time = now;

  rwl.m_blocks_to_log_entries.add_log_entry(op->log_entry);

  rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
					bool skip_partial_discard, Context *on_finish) {
  utime_t now = ceph_clock_now();
  Extents discard_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_discard, 1);

  CephContext *cct = m_image_ctx.cct;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
		   << "offset=" << offset << ", "
		   << "length=" << length << ", "
		   << "on_finish=" << on_finish << dendl;
  }

  assert(m_initialized);
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (length == 0) {
    on_finish->complete(0);
    return;
  }

  auto discard_req_sp =
    C_DiscardRequestT::create(*this, now, std::move(discard_extents), skip_partial_discard, on_finish);
  auto *discard_req = discard_req_sp.get();
  // TODO: Add discard stats
  //m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);
  {
    Mutex::Locker locker(m_lock);
    discard_req->op = std::make_shared<DiscardLogOperationT>(*this, m_current_sync_point,
							     offset, length, now);

    discard_req->op->log_entry->ram_entry.sync_gen_number = m_current_sync_gen;
    if (m_persist_on_flush) {
      /* Persist on flush. Sequence #0 is never used. */
      discard_req->op->log_entry->ram_entry.write_sequence_number = 0;
    } else {
      /* Persist on write */
      discard_req->op->log_entry->ram_entry.write_sequence_number = ++m_last_op_sequence_num;
      discard_req->op->log_entry->ram_entry.sequenced = 1;
    }
  }
  discard_req->op->on_write_persist = new FunctionContext(
    [this, discard_req_sp](int r) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "discard_req=" << discard_req_sp
				   << " cell=" << discard_req_sp->get_cell() << dendl;
      }
      assert(discard_req_sp->get_cell());
      discard_req_sp->complete_user_request(r);

      /* Completed to caller by here */
      // TODO: add discard timing stats
      //utime_t now = ceph_clock_now();
      //m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - flush_req->m_arrived_time);

      discard_req_sp->release_cell();
    });

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, discard_req](GuardedRequestFunctionContext &guard_ctx) {
      CephContext *cct = m_image_ctx.cct;
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << __func__ << " discard_req=" << discard_req << " cell=" << guard_ctx.m_cell << dendl;
      }

      assert(guard_ctx.m_cell);
      discard_req->m_detained = guard_ctx.m_state.detained;
      discard_req->set_cell(guard_ctx.m_cell);
      // TODO: more missing discard stats
      if (discard_req->m_detained) {
	//m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
      }
      alloc_and_dispatch_io_req(discard_req);
    });

  detain_guarded_request(GuardedRequest(discard_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename T>
bool C_FlushRequest<T>::alloc_resources() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }

  assert(!m_log_entry_allocated);
  bool allocated_here = false;
  Mutex::Locker locker(rwl.m_lock);
  if (rwl.m_free_log_entries) {
    rwl.m_free_log_entries--;
    m_log_entry_allocated = true;
    allocated_here = true;
  }
  return allocated_here;
}

template <typename T>
void C_FlushRequest<T>::dispatch() {
  utime_t now = ceph_clock_now();
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "req type=" << get_name() << " "
				   << "req=[" << *this << "]" << dendl;
  }
  assert(m_log_entry_allocated);
  this->m_dispatched_time = now;

  op = std::make_shared<SyncPointLogOperation<T>>(rwl, to_append, now);

  rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  rwl.schedule_append(op);
}

template <typename I>
C_FlushRequest<ReplicatedWriteLog<I>>* ReplicatedWriteLog<I>::make_flush_req(Context *on_finish) {
  utime_t flush_begins = ceph_clock_now();
  bufferlist bl;

  auto *flush_req =
    C_FlushRequestT::create(*this, flush_begins, Extents({whole_volume_extent()}),
			    std::move(bl), 0, on_finish).get();

  return flush_req;
}

/* Make a new sync point and flush the previous during initialization, when there may or may
 * not be a previous sync point */
template <typename I>
void ReplicatedWriteLog<I>::init_flush_new_sync_point(DeferredContexts &later) {
  assert(m_lock.is_locked_by_me());
  assert(!m_initialized); /* Don't use this after init */

  if (!m_current_sync_point) {
    /* First sync point since start */
    new_sync_point(later);
  } else {
    flush_new_sync_point(nullptr, later);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point(C_FlushRequestT *flush_req, DeferredContexts &later) {
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
  std::shared_ptr<SyncPointT> to_append = m_current_sync_point->earlier_sync_point;
  assert(to_append);

  /* This flush request will append/persist the (now) previous sync point */
  flush_req->to_append = to_append;
  to_append->m_append_scheduled = true;

  /* All prior sync points that are still in this list must already be scheduled for append */
  std::shared_ptr<SyncPointT> previous = to_append->earlier_sync_point;
  while (previous) {
    assert(previous->m_append_scheduled);
    previous = previous->earlier_sync_point;
  }

  /* When the m_sync_point_persist Gather completes this sync point can be
   * appended.  The only sub for this Gather is the finisher Context for
   * m_prior_log_entries_persisted, which records the result of the Gather in
   * the sync point, and completes. TODO: Do we still need both of these
   * Gathers?*/
  to_append->m_sync_point_persist->
    set_finisher(new FunctionContext([this, flush_req](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "Flush req=" << flush_req
				       << " sync point =" << flush_req->to_append
				       << ". Ready to persist." << dendl;
	  }
	  alloc_and_dispatch_io_req(flush_req);
	}));

  /* The m_sync_point_persist Gather has all the subs it will ever have, and
   * now has its finisher. If the sub is already complete, activation will
   * complete the Gather. The finisher will acquire m_lock, so we'll activate
   * this when we release m_lock.*/
  later.add(new FunctionContext([this, to_append](int r) {
	to_append->m_sync_point_persist->activate();
      }));

  /* The flush request completes when the sync point persists */
  to_append->m_on_sync_point_persisted.push_back(flush_req);
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, DeferredContexts &later) {
  assert(m_lock.is_locked_by_me());

  /* If there have been writes since the last sync point ... */
  if (m_current_sync_point->log_entry->m_writes) {
    flush_new_sync_point(flush_req, later);
  } else {
    /* There have been no writes to the current sync point. */
    if (m_current_sync_point->earlier_sync_point) {
      /* If previous sync point hasn't completed, complete this flush
       * with the earlier sync point. No alloc or dispatch needed. */
      m_current_sync_point->earlier_sync_point->m_on_sync_point_persisted.push_back(flush_req);
      assert(m_current_sync_point->earlier_sync_point->m_append_scheduled);
    } else {
      /* The previous sync point has already completed and been
       * appended. The current sync point has no writes, so this flush
       * has nothing to wait for. This flush completes now. */
      later.add(flush_req);
    }
  }
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
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "on_finish=" << on_finish << dendl;
  }
  m_perfcounter->inc(l_librbd_rwl_aio_flush, 1);

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish);
    return;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  auto flush_req = make_flush_req(on_finish);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, flush_req](GuardedRequestFunctionContext &guard_ctx) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(m_image_ctx.cct, 20) << "flush_req=" << flush_req << " cell=" << guard_ctx.m_cell << dendl;
      }
      assert(guard_ctx.m_cell);
      flush_req->m_detained = guard_ctx.m_state.detained;
      /* We don't call flush_req->set_cell(), because the block guard will be released here */
      if (flush_req->m_detained) {
	//m_perfcounter->inc(l_librbd_rwl_aio_flush_overlap, 1);
      }
      {
	DeferredContexts post_unlock; /* Do these when the lock below is released */
	Mutex::Locker locker(m_lock);

	if (!m_flush_seen) {
	  ldout(m_image_ctx.cct, 15) << "flush seen" << dendl;
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
	flush_new_sync_point_if_needed(flush_req, post_unlock);
      }

      release_guarded_request(guard_ctx.m_cell);
    });

  detain_guarded_request(GuardedRequest(flush_req->m_image_extents_summary.block_extent(),
					guarded_ctx, true));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
					  bufferlist&& bl, int fadvise_flags,
					  Context *on_finish) {
  utime_t now = ceph_clock_now();
  Extents ws_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_ws, 1);
  assert(m_initialized);
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if ((0 == length) || bl.length() == 0) {
    /* Zero length write or pattern */
    on_finish->complete(0);
    return;
  }

  if (length % bl.length()) {
    /* Length must be integer multiple of pattern length */
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_image_ctx.cct, 06) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
			     << "offset=" << offset << ", "
			     << "length=" << length << ", "
			     << "data_len=" << bl.length() << ", "
			     << "on_finish=" << on_finish << dendl;

  /* A write same request is also a write request. The key difference is the
   * write same data buffer is shorter than the extent of the request. The full
   * extent will be used in the block guard, and appear in
   * m_blocks_to_log_entries_map. The data buffer allocated for the WS is only
   * as long as the length of the bl here, which is the pattern that's repeated
   * in the image for the entire length of this WS. Read hits and flushing of
   * write sames are different than normal writes. */
  auto *ws_req =
    C_WriteSameRequestT::create(*this, now, std::move(ws_extents), std::move(bl), fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_ws_bytes, ws_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, ws_req](GuardedRequestFunctionContext &guard_ctx) {
      ws_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(ws_req);
    });

  detain_guarded_request(GuardedRequest(ws_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
						  bufferlist&& cmp_bl,
						  bufferlist&& bl,
						  uint64_t *mismatch_offset,
						  int fadvise_flags,
						  Context *on_finish) {
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_cmp, 1);
  assert(m_initialized);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  /* A compare and write request is also a write request. We only allocate
   * resources and dispatch this write request if the compare phase
   * succeeds. */
  auto *cw_req =
    C_CompAndWriteRequestT::create(*this, now, std::move(image_extents), std::move(cmp_bl), std::move(bl),
				   mismatch_offset, fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_cmp_bytes, cw_req->m_image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, cw_req](GuardedRequestFunctionContext &guard_ctx) {
      cw_req->blockguard_acquired(guard_ctx);

      auto read_complete_ctx = new FunctionContext(
	[this, cw_req](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
				       << "cw_req=" << cw_req << dendl;
	  }

	  /* Compare read_bl to cmp_bl to determine if this will produce a write */
	  if (cw_req->m_cmp_bl.contents_equal(cw_req->m_read_bl)) {
	    /* Compare phase succeeds. Begin write */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 05) << __func__ << " cw_req=" << cw_req << " compare matched" << dendl;
	    }
	    cw_req->m_compare_succeeded = true;
	    *cw_req->m_mismatch_offset = 0;
	    /* Continue with this request as a write. Blockguard release and
	     * user request completion handled as if this were a plain
	     * write. */
	    alloc_and_dispatch_io_req(cw_req);
	  } else {
	    /* Compare phase fails. Comp-and write ends now. */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " compare failed" << dendl;
	    }
	    /* Bufferlist doesn't tell us where they differed, so we'll have to determine that here */
	    assert(cw_req->m_read_bl.length() == cw_req->m_cmp_bl.length());
	    uint64_t bl_index = 0;
	    for (bl_index = 0; bl_index < cw_req->m_cmp_bl.length(); bl_index++) {
	      if (cw_req->m_cmp_bl[bl_index] != cw_req->m_read_bl[bl_index]) {
		if (RWL_VERBOSE_LOGGING) {
		  ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " mismatch at " << bl_index << dendl;
		}
		break;
	      }
	    }
	    cw_req->m_compare_succeeded = false;
	    *cw_req->m_mismatch_offset = bl_index;
	    cw_req->complete_user_request(-EILSEQ);
	    cw_req->release_cell();
	    cw_req->complete(0);
	  }
	});

      /* Read phase of comp-and-write must read through RWL */
      Extents image_extents_copy = cw_req->m_image_extents;
      aio_read(std::move(image_extents_copy), &cw_req->m_read_bl, cw_req->fadvise_flags, read_complete_ctx);
    });

  detain_guarded_request(GuardedRequest(cw_req->m_image_extents_summary.block_extent(),
					guarded_ctx));
}

/**
 * Begin a new sync point
 */
template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  std::shared_ptr<SyncPointT> old_sync_point = m_current_sync_point;
  std::shared_ptr<SyncPointT> new_sync_point;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  assert(m_lock.is_locked_by_me());

  /* The first time this is called, if this is a newly created log,
   * this makes the first sync gen number we'll use 1. On the first
   * call for a re-opened log m_current_sync_gen will be the highest
   * gen number from all the sync point entries found in the re-opened
   * log, and this advances to the next sync gen number. */
  ++m_current_sync_gen;

  new_sync_point = std::make_shared<SyncPointT>(*this, m_current_sync_gen);
  m_current_sync_point = new_sync_point;

  /* If this log has been re-opened, old_sync_point will initially be
   * nullptr, but m_current_sync_gen may not be zero. */
  if (old_sync_point) {
    new_sync_point->earlier_sync_point = old_sync_point;
    new_sync_point->log_entry->m_prior_sync_point_flushed = false;
    old_sync_point->log_entry->m_next_sync_point_entry = new_sync_point->log_entry;
    old_sync_point->later_sync_point = new_sync_point;
    old_sync_point->m_final_op_sequence_num = m_last_op_sequence_num;
    if (!old_sync_point->m_appending) {
      /* Append of new sync point deferred until old sync point is appending */
      old_sync_point->m_on_sync_point_appending.push_back(new_sync_point->m_prior_log_entries_persisted->new_sub());
    }
    /* This sync point will acquire no more sub-ops. Activation needs
     * to acquire m_lock, so defer to later*/
    later.add(new FunctionContext(
      [this, old_sync_point](int r) {
	old_sync_point->m_prior_log_entries_persisted->activate();
      }));
  }

  Context *sync_point_persist_ready = new_sync_point->m_sync_point_persist->new_sub();
  new_sync_point->m_prior_log_entries_persisted->
    set_finisher(new FunctionContext([this, new_sync_point, sync_point_persist_ready](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "Prior log entries persisted for sync point =["
				       << new_sync_point << "]" << dendl;
	  }
	  new_sync_point->m_prior_log_entries_persisted_result = r;
	  new_sync_point->m_prior_log_entries_persisted_complete = true;
	  sync_point_persist_ready->complete(r);
	}));

  if (old_sync_point) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct,6) << "new sync point = [" << *m_current_sync_point
		   << "], prior = [" << *old_sync_point << "]" << dendl;
    }
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
    "Latency (nsec)",
    PerfHistogramCommon::SCALE_LOG2, ///< Latency in logarithmic scale
    0,                               ///< Start at 0
    5000,                            ///< Quantization unit is 5usec
    16,                              ///< Ranges into the mS
  };

  // Op size axis configuration for op histogram y axis, values are in bytes
  PerfHistogramCommon::axis_config_d op_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    512,                             ///< Quantization unit is 512 bytes
    16,                              ///< Writes up to >32k
  };

  // Num items configuration for op histogram y axis, values are in items
  PerfHistogramCommon::axis_config_d op_hist_y_axis_count_config{
    "Number of items",
    PerfHistogramCommon::SCALE_LINEAR, ///< Request size in linear scale
    0,                                 ///< Start at 0
    1,                                 ///< Quantization unit is 512 bytes
    32,                                ///< Writes up to >32k
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
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_lanes, "wr_def_lanes", "Writes deferred for lanes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_log, "wr_def_log", "Writes deferred for log entries");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_buf, "wr_def_buf", "Writes deferred for buffers");
  plb.add_u64_counter(l_librbd_rwl_wr_req_overlap, "wr_overlap", "Writes overlapping with prior in-progress writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_queued, "wr_q_barrier", "Writes queued for prior barriers (aio_flush)");
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
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_all_t, "req_arr_to_all_nw_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_dis_t, "req_arr_to_dis_nw_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_all_to_dis_t, "req_all_to_dis_nw_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_latency, "wr_latency_nw", "Latency of writes (persistent completion) not deferred for free space");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_nowait_wr_latency_hist, "wr_latency_nw_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written for writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_caller_latency, "caller_wr_latency_nw", "Latency of write completion to callerfor writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_log_op_alloc_t, "op_alloc_t", "Average buffer pmemobj_reserve() time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_alloc_t_hist, "op_alloc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of buffer pmemobj_reserve() time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_buf_t, "op_dis_to_buf_t", "Average dispatch to buffer persist time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_app_t, "op_dis_to_app_t", "Average dispatch to log append time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_cmp_t, "op_dis_to_cmp_t", "Average dispatch to persist completion time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_dis_to_cmp_t_hist, "op_dis_to_cmp_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of op dispatch to persist complete time (nanoseconds) vs. bytes written");

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

  plb.add_u64_counter(l_librbd_rwl_cmp, "cmp", "Compare and Write requests");
  plb.add_u64_counter(l_librbd_rwl_cmp_bytes, "cmp_bytes", "Compare and Write bytes compared/written");
  plb.add_time_avg(l_librbd_rwl_cmp_latency, "cmp_lat", "Compare and Write latecy");
  plb.add_u64_counter(l_librbd_rwl_cmp_fails, "cmp_fails", "Compare and Write compare fails");

  plb.add_u64_counter(l_librbd_rwl_flush, "flush", "Flush (flush RWL)");
  plb.add_u64_counter(l_librbd_rwl_invalidate_cache, "invalidate", "Invalidate RWL");
  plb.add_u64_counter(l_librbd_rwl_invalidate_discard_cache, "discard", "Discard and invalidate RWL");

  plb.add_time_avg(l_librbd_rwl_append_tx_t, "append_tx_lat", "Log append transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_append_tx_t_hist, "append_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log append transaction time (nanoseconds) vs. entries appended");
  plb.add_time_avg(l_librbd_rwl_retire_tx_t, "retire_tx_lat", "Log retire transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_retire_tx_t_hist, "retire_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log retire transaction time (nanoseconds) vs. entries retired");

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
  bl.append("{\n");
  stringstream ss;
  utime_t now = ceph_clock_now();
  ss << "\"test_time\": \"" << now << "\",";
  ss << "\"image\": \"" << m_image_ctx.name << "\",";
  bl.append(ss);
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
void ReplicatedWriteLog<I>::periodic_stats() {
  Mutex::Locker locker(m_lock);
  ldout(m_image_ctx.cct, 1) << "STATS: "
			    << "m_free_log_entries=" << m_free_log_entries << ", "
			    << "m_ops_to_flush=" << m_ops_to_flush.size() << ", "
			    << "m_ops_to_append=" << m_ops_to_append.size() << ", "
			    << "m_deferred_ios=" << m_deferred_ios.size() << ", "
			    << "m_log_entries=" << m_log_entries.size() << ", "
			    << "m_dirty_log_entries=" << m_dirty_log_entries.size() << ", "
			    << "m_bytes_allocated=" << m_bytes_allocated << ", "
			    << "m_bytes_cached=" << m_bytes_cached << ", "
			    << "m_bytes_dirty=" << m_bytes_dirty << ", "
			    << "bytes available=" << m_bytes_allocated_cap - m_bytes_allocated << ", "
			    << "m_current_sync_gen=" << m_current_sync_gen << ", "
			    << "m_flushed_sync_gen=" << m_flushed_sync_gen << ", "
			    << "m_flush_ops_in_flight=" << m_flush_ops_in_flight << ", "
			    << "m_flush_bytes_in_flight=" << m_flush_bytes_in_flight << ", "
			    << "m_async_flush_ops=" << m_async_flush_ops << ", "
			    << "m_async_append_ops=" << m_async_append_ops << ", "
			    << "m_async_complete_ops=" << m_async_complete_ops << ", "
			    << "m_async_null_flush_finish=" << m_async_null_flush_finish << ", "
			    << "m_async_process_work=" << m_async_process_work << ", "
			    << "m_async_op_tracker=[" << m_async_op_tracker << "]"
			    << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::arm_periodic_stats() {
  if (m_periodic_stats_enabled) {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.add_event_after(LOG_STATS_INTERVAL_SECONDS, new FunctionContext(
      [this](int r) {
	periodic_stats();
	arm_periodic_stats();
      }));
  }
}

/*
 * Loads the log entries from an existing log.
 *
 * Creates the in-memory structures to represent the state of the
 * re-opened log.
 *
 * Finds the last appended sync point, and any sync points referred to
 * in log entries, but missing from the log. These missing sync points
 * are created and scheduled for append. Some rudimentary consistency
 * checking is done.
 *
 * Rebuilds the m_blocks_to_log_entries map, to make log entries
 * readable.
 *
 * Places all writes on the dirty entries list, which causes them all
 * to be flushed.
 *
 * TODO: Turn consistency check asserts into open failures.
 *
 * TODO: Writes referring to missing sync points must be discarded if
 * the replication mechanism doesn't guarantee all entries are
 * appended to all replicas in the same order, and that appends in
 * progress during a replica failure will be resolved by the
 * replication mechanism. PMDK pool replication guarantees this, so
 * discarding unsequenced writes referring to a missing sync point is
 * not yet implemented.
 *
 */
template <typename I>
void ReplicatedWriteLog<I>::load_existing_entries(DeferredContexts &later) {
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);
  uint64_t entry_index = m_first_valid_entry;
  /* The map below allows us to find sync point log entries by sync
   * gen number, which is necessary so write entries can be linked to
   * their sync points. */
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;
  std::shared_ptr<SyncPointLogEntry> highest_existing_sync_point = nullptr;
  /* The map below tracks sync points referred to in writes but not
   * appearing in the sync_point_entries map.  We'll use this to
   * determine which sync points are missing and need to be
   * created. */
  std::map<uint64_t, bool> missing_sync_points;

  /*
   * Read the existing log entries. Construct an in-memory log entry
   * object of the appropriate type for each. Add these to the global
   * log entries list.
   *
   * Write entries will not link to their sync points yet. We'll do
   * that in the next pass. Here we'll accumulate a map of sync point
   * gen numbers that are referred to in writes but do not appearing in
   * the log.
   */
  while (entry_index != m_first_free_entry) {
    WriteLogPmemEntry *pmem_entry = &pmem_log_entries[entry_index];
    std::shared_ptr<GenericLogEntry> log_entry = nullptr;
    bool writer = pmem_entry->is_writer();

    assert(pmem_entry->entry_index == entry_index);
    if (pmem_entry->is_sync_point()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a sync point. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto sync_point_entry = std::make_shared<SyncPointLogEntry>(pmem_entry->sync_gen_number);
      log_entry = sync_point_entry;
      sync_point_entries[pmem_entry->sync_gen_number] = sync_point_entry;
      missing_sync_points.erase(pmem_entry->sync_gen_number);
      if (highest_existing_sync_point) {
	/* Sync points must appear in order */
	assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }
      highest_existing_sync_point = sync_point_entry;
      m_current_sync_gen = pmem_entry->sync_gen_number;
    } else if (pmem_entry->is_write()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a write. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto write_entry =
	std::make_shared<WriteLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      write_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = write_entry;
    } else if (pmem_entry->is_writesame()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a write same. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto ws_entry =
	std::make_shared<WriteSameLogEntry>(nullptr, pmem_entry->image_offset_bytes,
					    pmem_entry->write_bytes, pmem_entry->ws_datalen);
      ws_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = ws_entry;
    } else if (pmem_entry->is_discard()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a discard. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto discard_entry =
	std::make_shared<DiscardLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      log_entry = discard_entry;
    } else {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry " << entry_index
			     << ", pmem_entry=[" << *pmem_entry << "]" << dendl;
      assert(false);
    }

    if (writer) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " writes. pmem_entry=[" << *pmem_entry << "]" << dendl;
      if (highest_existing_sync_point) {
	/* Writes must precede the sync points they bear */
	assert(highest_existing_sync_point->ram_entry.sync_gen_number ==
	       highest_existing_sync_point->pmem_entry->sync_gen_number);
	assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }
      if (!sync_point_entries[pmem_entry->sync_gen_number]) {
	missing_sync_points[pmem_entry->sync_gen_number] = true;
      }
    }

    log_entry->ram_entry = *pmem_entry;
    log_entry->pmem_entry = pmem_entry;
    log_entry->log_entry_index = entry_index;
    log_entry->completed = true;

    m_log_entries.push_back(log_entry);

    entry_index = (entry_index + 1) % m_total_log_entries;
  }

  /* Create missing sync points. These must not be appended until the
   * entry reload is complete and the write map is up to
   * date. Currently this is handled by the deferred contexts object
   * passed to new_sync_point(). These contexts won't be completed
   * until this function returns.  */
  for (auto &kv : missing_sync_points) {
    ldout(m_image_ctx.cct, 5) << "Adding sync point " << kv.first << dendl;
    if (0 == m_current_sync_gen) {
      /* The unlikely case where the log contains writing entries, but no sync
       * points (e.g. because they were all retired) */
      m_current_sync_gen = kv.first-1;
    }
    assert(kv.first == m_current_sync_gen+1);
    init_flush_new_sync_point(later);
    assert(kv.first == m_current_sync_gen);
    sync_point_entries[kv.first] = m_current_sync_point->log_entry;;
  }

  /*
   * Iterate over the log entries again (this time via the global
   * entries list), connecting write entries to their sync points and
   * updating the sync point stats.
   *
   * Add writes to the write log map.
   */
  std::shared_ptr<SyncPointLogEntry> previous_sync_point_entry = nullptr;
  for (auto &log_entry : m_log_entries)  {
    if (log_entry->ram_entry.is_writer()) {
      /* This entry is one of the types that write */
      auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);
      auto sync_point_entry = sync_point_entries[gen_write_entry->ram_entry.sync_gen_number];
      if (!sync_point_entry) {
	lderr(m_image_ctx.cct) << "Sync point missing for entry=[" << *gen_write_entry << "]" << dendl;
	assert(false);
      } else {
	/* TODO: Discard unsequenced writes for sync points that
	 * didn't appear in the log (but were added above). This is
	 * optional if the replication mechanism guarantees
	 * persistence everywhere in the same order (which PMDK pool
	 * replication does). */
	gen_write_entry->sync_point_entry = sync_point_entry;
	sync_point_entry->m_writes++;
	sync_point_entry->m_bytes += gen_write_entry->ram_entry.write_bytes;
	sync_point_entry->m_writes_completed++;
	m_blocks_to_log_entries.add_log_entry(gen_write_entry);
	/* This entry is only dirty if its sync gen number is > the flushed
	 * sync gen number from the root object. */
	if (gen_write_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) {
	  m_dirty_log_entries.push_back(log_entry);
	  m_bytes_dirty += gen_write_entry->bytes_dirty();
	} else {
	  gen_write_entry->flushed = true;
	  sync_point_entry->m_writes_flushed++;
	}
	if (log_entry->ram_entry.is_write()) {
	  /* This entry is a basic write */
	  uint64_t bytes_allocated = MIN_WRITE_ALLOC_SIZE;
	  if (gen_write_entry->ram_entry.write_bytes > bytes_allocated) {
	    bytes_allocated = gen_write_entry->ram_entry.write_bytes;
	  }
	  m_bytes_allocated += bytes_allocated;
	  m_bytes_cached += gen_write_entry->ram_entry.write_bytes;
	}
      }
    } else if (log_entry->ram_entry.is_sync_point()) {
      auto sync_point_entry = static_pointer_cast<SyncPointLogEntry>(log_entry);
      if (previous_sync_point_entry) {
	previous_sync_point_entry->m_next_sync_point_entry = sync_point_entry;
	if (previous_sync_point_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) {
	  sync_point_entry->m_prior_sync_point_flushed = false;
	  assert(!previous_sync_point_entry->m_prior_sync_point_flushed ||
		 (0 == previous_sync_point_entry->m_writes) ||
		 (previous_sync_point_entry->m_writes > previous_sync_point_entry->m_writes_flushed));
	} else {
	  sync_point_entry->m_prior_sync_point_flushed = true;
	  assert(previous_sync_point_entry->m_prior_sync_point_flushed);
	  assert(previous_sync_point_entry->m_writes == previous_sync_point_entry->m_writes_flushed);
	}
	previous_sync_point_entry = sync_point_entry;
      } else {
	/* There are no previous sync points, so we'll consider them flushed */
	sync_point_entry->m_prior_sync_point_flushed = true;
      }
      ldout(m_image_ctx.cct, 10) << "Loaded to sync point=[" << *sync_point_entry << dendl;
    } else {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry=[" << *log_entry << "]" << dendl;
      assert(false);
    }
  }
  if (0 == m_current_sync_gen) {
    /* If a re-opened log was completely flushed, we'll have found no sync point entries here,
     * and not advanced m_current_sync_gen. Here we ensure it starts past the last flushed sync
     * point recorded in the log. */
    m_current_sync_gen = m_flushed_sync_gen;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::rwl_init(Context *on_finish, DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  TOID(struct WriteLogPoolRoot) pool_root;

  Mutex::Locker locker(m_lock);
  assert(!m_initialized);
  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;
  ldout(cct,5) << "rwl_enabled: " << m_image_ctx.rwl_enabled << dendl;
  ldout(cct,5) << "rwl_size: " << m_image_ctx.rwl_size << dendl;
  std::string rwl_path = m_image_ctx.rwl_path;
  ldout(cct,5) << "rwl_path: " << m_image_ctx.rwl_path << dendl;

  std::string log_pool_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".poolset";
  m_log_pool_config_size = max(m_image_ctx.rwl_size, MIN_POOL_SIZE);

  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
    m_log_is_poolset = true;
  } else {
    m_log_pool_name = log_pool_name;
    ldout(cct, 5) << "failed to open poolset" << log_poolset_name
		  << ". Opening/creating simple/unreplicated pool" << dendl;
  }

  if (access(m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_internal->m_log_pool =
	 pmemobj_create(m_log_pool_name.c_str(),
			rwl_pool_layout_name,
			m_log_pool_config_size,
			(S_IWUSR | S_IRUSR))) == NULL) {
      lderr(cct) << "failed to create pool (" << m_log_pool_name << ")"
		 << pmemobj_errormsg() << dendl;
      m_present = false;
      m_clean = true;
      m_empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    }
    m_present = true;
    m_clean = true;
    m_empty = true;
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    assert(num_small_writes > 2);
    m_log_pool_actual_size = m_log_pool_config_size;
    m_bytes_allocated_cap = effective_pool_size;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    TX_BEGIN(m_internal->m_log_pool) {
      TX_ADD(pool_root);
      D_RW(pool_root)->header.layout_version = RWL_POOL_VERSION;
      D_RW(pool_root)->log_entries =
	TX_ZALLOC(struct WriteLogPmemEntry,
		  sizeof(struct WriteLogPmemEntry) * num_small_writes);
      D_RW(pool_root)->pool_size = m_log_pool_actual_size;
      D_RW(pool_root)->flushed_sync_gen = m_flushed_sync_gen;
      D_RW(pool_root)->block_size = MIN_WRITE_ALLOC_SIZE;
      D_RW(pool_root)->num_log_entries = num_small_writes;
      D_RW(pool_root)->first_free_entry = m_first_free_entry;
      D_RW(pool_root)->first_valid_entry = m_first_valid_entry;
    } TX_ONCOMMIT {
      m_total_log_entries = D_RO(pool_root)->num_log_entries;
      m_free_log_entries = D_RO(pool_root)->num_log_entries - 1; // leave one free
    } TX_ONABORT {
      m_total_log_entries = 0;
      m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << m_log_pool_name << ")" << dendl;
      on_finish->complete(-pmemobj_tx_errno());
      return;
    } TX_FINALLY {
    } TX_END;
  } else {
    m_present = true;
    /* Open existing pool */
    if ((m_internal->m_log_pool =
	 pmemobj_open(m_log_pool_name.c_str(),
		      rwl_pool_layout_name)) == NULL) {
      lderr(cct) << "failed to open pool (" << m_log_pool_name << "): "
		 << pmemobj_errormsg() << dendl;
      on_finish->complete(-errno);
      return;
    }
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
    if (D_RO(pool_root)->header.layout_version != RWL_POOL_VERSION) {
      lderr(cct) << "Pool layout version is " << D_RO(pool_root)->header.layout_version
		 << " expected " << RWL_POOL_VERSION << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    if (D_RO(pool_root)->block_size != MIN_WRITE_ALLOC_SIZE) {
      lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size
		 << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    m_log_pool_actual_size = D_RO(pool_root)->pool_size;
    m_flushed_sync_gen = D_RO(pool_root)->flushed_sync_gen;
    m_total_log_entries = D_RO(pool_root)->num_log_entries;
    m_first_free_entry = D_RO(pool_root)->first_free_entry;
    m_first_valid_entry = D_RO(pool_root)->first_valid_entry;
    if (m_first_free_entry < m_first_valid_entry) {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
      m_free_log_entries = m_total_log_entries - (m_first_valid_entry - m_first_free_entry) -1;
    } else {
      /* first_valid is <= first_free. If they are == we have zero valid log
       * entries, and n-1 free log entries */
      m_free_log_entries = m_total_log_entries - (m_first_free_entry - m_first_valid_entry) -1;
    }
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    m_bytes_allocated_cap = effective_pool_size;
    load_existing_entries(later);
    m_clean = m_dirty_log_entries.empty();
    m_empty = m_log_entries.empty();
  }

  ldout(cct,1) << "pool " << m_log_pool_name << " has " << m_total_log_entries
	       << " log entries, " << m_free_log_entries << " of which are free."
	       << " first_valid=" << m_first_valid_entry
	       << ", first_free=" << m_first_free_entry
	       << ", flushed_sync_gen=" << m_flushed_sync_gen
	       << ", current_sync_gen=" << m_current_sync_gen << dendl;
  if (m_first_free_entry == m_first_valid_entry) {
    ldout(cct,1) << "write log is empty" << dendl;
    m_empty = true;
  }

  /* Start the sync point following the last one seen in the
   * log. Flush the last sync point created during the loading of the
   * existing log entries. */
  init_flush_new_sync_point(later);
  ldout(cct,20) << "new sync point = [" << m_current_sync_point << "]" << dendl;

  m_initialized = true;

  m_periodic_stats_enabled = m_image_ctx.rwl_log_periodic_stats;
  arm_periodic_stats();
  m_image_ctx.op_work_queue->queue(on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  perf_start(m_image_ctx.id);

  assert(!m_initialized);
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r >= 0) {
	DeferredContexts later;
	rwl_init(on_finish, later);
	if (m_periodic_stats_enabled) {
	  periodic_stats();
	}
      } else {
	/* Don't init RWL if layer below failed to init */
	m_image_ctx.op_work_queue->queue(on_finish, r);
      }
    });
  /* Initialize the cache layer below first */
  m_image_writeback->init(ctx);
}

template <typename I>
void ReplicatedWriteLog<I>::get_state(bool &clean, bool &empty, bool &present) {
  /* State of this cache to be recorded in image metadata */
  clean = m_clean;     /* true if there's nothing to flush */
  empty = m_empty;     /* true if there's nothing to invalidate */
  present = m_present; /* true if there's no storage to release */
}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      {
	Mutex::Locker timer_locker(m_timer_lock);
	m_timer.cancel_all_events();
      }
      ldout(m_image_ctx.cct, 6) << "shutdown complete" << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, r);
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
      bool periodic_stats_enabled = m_periodic_stats_enabled;
      m_periodic_stats_enabled = false;
      {
	Mutex::Locker timer_locker(m_timer_lock);
	m_timer.cancel_all_events();
      }
      if (periodic_stats_enabled) {
	/* Log stats one last time if they were enabled */
	periodic_stats();
      }
      if (m_perfcounter && m_image_ctx.rwl_log_stats_on_close) {
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
      m_thread_pool.stop();
      {
	Mutex::Locker locker(m_lock);
	assert(m_dirty_log_entries.size() == 0);
	m_clean = true;
	bool empty = true;
	for (auto entry : m_log_entries) {
	  if (!entry->ram_entry.is_sync_point()) {
	    empty = false; /* ignore sync points for emptiness */
	  }
	  if (entry->ram_entry.is_write() || entry->ram_entry.is_writesame()) {
	    /* WS entry is also a Write entry */
	    auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
	    m_blocks_to_log_entries.remove_log_entry(write_entry);
	    assert(write_entry->referring_map_entries == 0);
	    assert(write_entry->reader_count() == 0);
	    assert(!write_entry->flushing);
	  }
	}
	m_empty = empty;
	m_log_entries.clear();
      }
      if (m_internal->m_log_pool) {
	ldout(m_image_ctx.cct, 6) << "closing pmem pool" << dendl;
	pmemobj_close(m_internal->m_log_pool);
	r = -errno;
      }
      if (m_image_ctx.rwl_remove_on_close) {
	if (m_log_is_poolset) {
	  ldout(m_image_ctx.cct, 5) << "Not removing poolset " << m_log_pool_name << dendl;
	} else {
	  ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << m_log_pool_name << dendl;
	  if (remove(m_log_pool_name.c_str()) != 0) {
	    lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << m_log_pool_name << "\": "
				   << pmemobj_errormsg() << dendl;
	  } else {
	    m_clean = true;
	    m_empty = true;
	    m_present = false;
	  }
	}
      }
      if (m_perfcounter) {
	perf_stop();
      }
      next_ctx->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      /* Get off of RWL WQ - thread pool about to be shut down */
      m_image_ctx.op_work_queue->queue(ctx);
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
      ldout(m_image_ctx.cct, 6) << "retiring entries" << dendl;
      while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }
      ldout(m_image_ctx.cct, 6) << "waiting for internal async operations" << dendl;
      // Second op tracker wait after flush completion for process_work()
      {
	Mutex::Locker locker(m_lock);
	m_wake_up_enabled = false;
      }
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
      m_shutting_down = true;
      // flush all writes to OSDs
      ldout(m_image_ctx.cct, 6) << "flushing" << dendl;
      flush_dirty_entries(next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      /* Back to RWL WQ */
      m_work_queue.queue(ctx);
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
      ldout(m_image_ctx.cct, 6) << "waiting for in flight operations" << dendl;
      // Wait for in progress IOs to complete
      Mutex::Locker locker(m_lock);
      m_async_op_tracker.wait(m_image_ctx, next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      m_work_queue.queue(ctx);
    });
  /* Complete all in-flight writes before shutting down */
  internal_flush(ctx, false, false);
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

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  /* Wake-up can be requested while it's already scheduled */
  m_wake_up_requested = true;

  /* Wake-up cannot be scheduled if it's already scheduled */
  if (m_wake_up_scheduled) {
    return;
  }
  m_wake_up_scheduled = true;
  m_async_process_work++;
  m_async_op_tracker.start_op();
  m_work_queue.queue(new FunctionContext(
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
  uint64_t aggressive_high_water_bytes = m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = m_bytes_allocated_cap * RETIRE_HIGH_WATER;
  uint64_t low_water_bytes = m_bytes_allocated_cap * RETIRE_LOW_WATER;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  do {
    {
      Mutex::Locker locker(m_lock);
      m_wake_up_requested = false;
    }
    if (m_alloc_failed_since_retire || m_shutting_down || m_invalidating ||
	m_bytes_allocated > high_water_bytes) {
      int retired = 0;
      utime_t started = ceph_clock_now();
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << m_alloc_failed_since_retire
				 << ", allocated > high_water="
				 << (m_bytes_allocated > high_water_bytes)
				 << dendl;
      while (m_alloc_failed_since_retire || m_shutting_down || m_invalidating ||
	     (m_bytes_allocated > high_water_bytes) ||
	     ((m_bytes_allocated > low_water_bytes) &&
	      (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) {
	if (!retire_entries((m_shutting_down || m_invalidating ||
			     (m_bytes_allocated > aggressive_high_water_bytes))
			    ? MAX_ALLOC_PER_TRANSACTION
			    : MAX_FREE_PER_TRANSACTION)) {
	  break;
	}
	retired++;
	dispatch_deferred_writes();
	process_writeback_dirty_entries();
      }
      ldout(m_image_ctx.cct, 10) << "Retired " << retired << " entries" << dendl;
    }
    dispatch_deferred_writes();
    process_writeback_dirty_entries();

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
bool ReplicatedWriteLog<I>::can_flush_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->ram_entry.is_writer());
  assert(m_lock.is_locked_by_me());

  if (m_invalidating) return true;

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

  auto gen_write_entry = log_entry->get_gen_write_log_entry();
  if (gen_write_entry &&
      !gen_write_entry->ram_entry.sequenced &&
      (gen_write_entry->sync_point_entry &&
       !gen_write_entry->sync_point_entry->completed)) {
    /* Sync point for this unsequenced writing entry is not persisted */
    return false;
  }

  return (log_entry->completed &&
	  (m_flush_ops_in_flight <= IN_FLIGHT_FLUSH_WRITE_LIMIT) &&
	  (m_flush_bytes_in_flight <= IN_FLIGHT_FLUSH_BYTES_LIMIT));
}

/* Update/persist the last flushed sync point in the log */
template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen(void)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  uint64_t flushed_sync_gen;

  Mutex::Locker append_locker(m_log_append_lock);
  {
    Mutex::Locker locker(m_lock);
    flushed_sync_gen = m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
				 << D_RO(pool_root)->flushed_sync_gen << " to "
				 << flushed_sync_gen << dendl;
    }
    //tx_start = ceph_clock_now();
    TX_BEGIN(m_internal->m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
    } TX_ONCOMMIT {
    } TX_ONABORT {
      lderr(m_image_ctx.cct) << "failed to commit update of flushed sync point" << dendl;
      assert(false);
    } TX_FINALLY {
    } TX_END;
    //tx_end = ceph_clock_now();
    //assert(last_retired_entry_index == (first_valid_entry - 1) % m_total_log_entries);
  }
}

/* Returns true if the specified SyncPointLogEntry is considered flushed, and
 * the log will be updated to reflect this. */
static const unsigned int HANDLE_FLUSHED_SYNC_POINT_RECURSE_LIMIT = 4;
template <typename I>
bool ReplicatedWriteLog<I>::handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  assert(m_lock.is_locked_by_me());
  assert(log_entry);

  if ((log_entry->m_writes_flushed == log_entry->m_writes) &&
      log_entry->completed && log_entry->m_prior_sync_point_flushed &&
      log_entry->m_next_sync_point_entry) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_image_ctx.cct, 20) << "All writes flushed up to sync point="
				 << *log_entry << dendl;
    }
    log_entry->m_next_sync_point_entry->m_prior_sync_point_flushed = true;
    /* Don't move the flushed sync gen num backwards. */
    if (m_flushed_sync_gen < log_entry->ram_entry.sync_gen_number) {
      m_flushed_sync_gen = log_entry->ram_entry.sync_gen_number;
    }
    m_async_op_tracker.start_op();
    m_work_queue.queue(new FunctionContext(
      [this, log_entry](int r) {
	bool handled_by_next;
	{
	  Mutex::Locker locker(m_lock);
	  handled_by_next = handle_flushed_sync_point(log_entry->m_next_sync_point_entry);
	}
	if (!handled_by_next) {
	  persist_last_flushed_sync_gen();
	}
	m_async_op_tracker.finish_op();
      }));
    return true;
  }
  return false;
}

template <typename I>
void ReplicatedWriteLog<I>::sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  assert(m_lock.is_locked_by_me());
  assert(log_entry);
  log_entry->m_writes_flushed++;

  /* If this entry might be completely flushed, look closer */
  if ((log_entry->m_writes_flushed == log_entry->m_writes) && log_entry->completed) {
    ldout(m_image_ctx.cct, 15) << "All writes flushed for sync point="
			       << *log_entry << dendl;
    handle_flushed_sync_point(log_entry);
  }
}

static const bool COPY_PMEM_FOR_FLUSH = true;
template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;
  bool invalidating = m_invalidating; // snapshot so we behave consistently
  bufferlist entry_bl;

  ldout(cct, 20) << "" << dendl;
  assert(log_entry->is_writer());
  if (!(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame())) {
    ldout(cct, 02) << "Flushing from log entry=" << *log_entry
		   << " unimplemented" << dendl;
  }
  assert(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame());
  assert(m_entry_reader_lock.is_locked());
  assert(m_lock.is_locked_by_me());
  if (!m_flush_ops_in_flight ||
      (log_entry->ram_entry.sync_gen_number < m_lowest_flushing_sync_gen)) {
    m_lowest_flushing_sync_gen = log_entry->ram_entry.sync_gen_number;
  }
  auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);
  m_flush_ops_in_flight += 1;
  /* For write same this is the bytes affected bt the flush op, not the bytes transferred */
  m_flush_bytes_in_flight += gen_write_entry->ram_entry.write_bytes;

  gen_write_entry->flushing = true;

  /* Construct bl for pmem buffer now while we hold m_entry_reader_lock */
  if (invalidating || log_entry->is_discard()) {
    /* If we're invalidating the RWL, we don't actually flush, so don't create
     * the buffer.  If we're flushing a discard, we also don't need the
     * buffer. */
  } else {
    assert(log_entry->is_write() || log_entry->is_writesame());
    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
    if (COPY_PMEM_FOR_FLUSH) {
      /* Pass a copy of the pmem buffer to ImageWriteback (which may hang on to the bl evcen after flush()). */
      buffer::list entry_bl_copy;
      write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
      entry_bl_copy.copy(0, write_entry->write_bytes(), entry_bl);
    } else {
      /* Pass a bl that refers to the pmem buffers to ImageWriteback */
      entry_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), 0, write_entry->write_bytes());
    }
  }

  /* Flush write completion action */
  Context *ctx = new FunctionContext(
    [this, gen_write_entry, invalidating](int r) {
      {
	Mutex::Locker locker(m_lock);
	gen_write_entry->flushing = false;
	if (r < 0) {
	  lderr(m_image_ctx.cct) << "failed to flush log entry"
				 << cpp_strerror(r) << dendl;
	  m_dirty_log_entries.push_front(gen_write_entry);
	} else {
	  assert(!gen_write_entry->flushed);
	  gen_write_entry->flushed = true;
	  assert(m_bytes_dirty >= gen_write_entry->bytes_dirty());
	  m_bytes_dirty -= gen_write_entry->bytes_dirty();
	  sync_point_writer_flushed(gen_write_entry->sync_point_entry);
	  ldout(m_image_ctx.cct, 20) << "flushed: " << gen_write_entry
	  << " invalidating=" << invalidating << dendl;
	}
	m_flush_ops_in_flight -= 1;
	m_flush_bytes_in_flight -= gen_write_entry->ram_entry.write_bytes;
	wake_up();
      }
    });
  /* Flush through lower cache before completing */
  ctx = new FunctionContext(
    [this, ctx](int r) {
      if (r < 0) {
	lderr(m_image_ctx.cct) << "failed to flush log entry"
			       << cpp_strerror(r) << dendl;
	ctx->complete(r);
      } else {
	m_image_writeback->aio_flush(ctx);
      }
    });

  if (invalidating) {
    /* When invalidating we just do the flush bookkeeping */
    return(ctx);
  } else {
    if (log_entry->is_write()) {
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the
       * m_image_writeback call */
      return new FunctionContext(
	[this, gen_write_entry, entry_bl=move(entry_bl), ctx](int r) {
	  auto captured_entry_bl = std::move(entry_bl);
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, gen_write_entry, entry_bl=move(captured_entry_bl), ctx](int r) {
	      auto captured_entry_bl = std::move(entry_bl);
	      ldout(m_image_ctx.cct, 15) << "flushing:" << gen_write_entry
					 << " " << *gen_write_entry << dendl;
	      m_image_writeback->aio_write({{gen_write_entry->ram_entry.image_offset_bytes,
					     gen_write_entry->ram_entry.write_bytes}},
					   std::move(captured_entry_bl), 0, ctx);
	    }));
	});
    } else if (log_entry->is_writesame()) {
      auto ws_entry = static_pointer_cast<WriteSameLogEntry>(log_entry);
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the
       * m_image_writeback call */
      return new FunctionContext(
	[this, ws_entry, entry_bl=move(entry_bl), ctx](int r) {
	  auto captured_entry_bl = std::move(entry_bl);
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, ws_entry, entry_bl=move(captured_entry_bl), ctx](int r) {
	      auto captured_entry_bl = std::move(entry_bl);
	      ldout(m_image_ctx.cct, 02) << "flushing:" << ws_entry
					 << " " << *ws_entry << dendl;
	      m_image_writeback->aio_writesame(ws_entry->ram_entry.image_offset_bytes,
					       ws_entry->ram_entry.write_bytes,
					       std::move(captured_entry_bl), 0, ctx);
	    }));
	});
    } else if (log_entry->is_discard()) {
      return new FunctionContext(
	[this, log_entry, ctx](int r) {
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, log_entry, ctx](int r) {
	      auto discard_entry = static_pointer_cast<DiscardLogEntry>(log_entry);
	      /* Invalidate from caches below. We always set skip_partial false
	       * here, because we need all the caches below to behave the same
	       * way in terms of discard granularity and alignment so they
	       * remain consistent. */
	      m_image_writeback->aio_discard(discard_entry->ram_entry.image_offset_bytes,
					     discard_entry->ram_entry.write_bytes,
					     false, ctx);
	    }));
	});
    } else {
      lderr(cct) << "Flushing from log entry=" << *log_entry
		 << " unimplemented" << dendl;
      assert(false);
      return nullptr;
    }
  }
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_entries() {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;
  int flushed = 0;

  ldout(cct, 20) << "Look for dirty entries" << dendl;
  {
    DeferredContexts post_unlock;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    while (flushed < IN_FLIGHT_FLUSH_WRITE_LIMIT) {
      Mutex::Locker locker(m_lock);
      if (m_dirty_log_entries.empty()) {
	ldout(cct, 20) << "Nothing new to flush" << dendl;

	/* Check if we should take flush complete actions */
	all_clean = !m_flush_ops_in_flight; // and m_dirty_log_entries is empty
	break;
      }
      auto candidate = m_dirty_log_entries.front();
      bool flushable = can_flush_entry(candidate);
      if (flushable) {
	post_unlock.add(construct_flush_entry_ctx(candidate));
	flushed++;
      }
      if (flushable || !candidate->ram_entry.is_writer()) {
	/* Remove if we're flushing it, or it's not a writer */
	if (!candidate->ram_entry.is_writer()) {
	  ldout(cct, 20) << "Removing non-writing entry from m_dirty_log_entries:"
			 << *m_dirty_log_entries.front() << dendl;
	}
	m_dirty_log_entries.pop_front();
      } else {
	ldout(cct, 20) << "Next dirty entry isn't flushable yet" << dendl;
	break;
      }
    }
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
bool ReplicatedWriteLog<I>::can_retire_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  assert(m_lock.is_locked_by_me());
  if (!log_entry->completed) {
    return false;
  }
  if (log_entry->is_write() || log_entry->is_writesame()) {
    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
    return (write_entry->flushed &&
	    0 == write_entry->reader_count());
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
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  Mutex::Locker retire_locker(m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(m_entry_reader_lock);
    Mutex::Locker locker(m_lock);
    initial_first_valid_entry = m_first_valid_entry;
    first_valid_entry = m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
	   retiring_entries.size() < frees_per_tx &&
	   can_retire_entry(entry)) {
      assert(entry->completed);
      if (entry->log_entry_index != first_valid_entry) {
	lderr(cct) << "Retiring entry index (" << entry->log_entry_index
		   << ") and first valid log entry index (" << first_valid_entry
		   << ") must be ==." << dendl;
      }
      assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if (entry->is_write() || entry->is_writesame()) {
	auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
	m_blocks_to_log_entries.remove_log_entry(write_entry);
	assert(!write_entry->flushing);
	assert(write_entry->flushed);
	assert(!write_entry->reader_count());
	assert(!write_entry->referring_map_entries);
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      Mutex::Locker append_locker(m_log_append_lock);
      {
	Mutex::Locker locker(m_lock);
	flushed_sync_gen = m_flushed_sync_gen;
      }
      //uint32_t last_retired_entry_index;

      tx_start = ceph_clock_now();
      TX_BEGIN(m_internal->m_log_pool) {
	if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
	  ldout(m_image_ctx.cct, 20) << "flushed_sync_gen in log updated from "
				     << D_RO(pool_root)->flushed_sync_gen << " to "
				     << flushed_sync_gen << dendl;
	  D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
	}
	D_RW(pool_root)->first_valid_entry = first_valid_entry;
	for (auto &entry: retiring_entries) {
	  //last_retired_entry_index = entry->log_entry_index;
	  if (entry->is_write() || entry->is_writesame()) {
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo
			     <<	"." << entry->ram_entry.write_data.oid.off << dendl;
	    }
	    TX_FREE(entry->ram_entry.write_data);
	  } else {
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
	    }
	  }
	}
      } TX_ONCOMMIT {
      } TX_ONABORT {
	lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << m_log_pool_name << ")" << dendl;
	assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
      //assert(last_retired_entry_index == (first_valid_entry - 1) % m_total_log_entries);
    }
    m_perfcounter->tinc(l_librbd_rwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_rwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      Mutex::Locker locker(m_lock);

      ceph_assert(m_first_valid_entry == initial_first_valid_entry);
      m_first_valid_entry = first_valid_entry;
      m_free_log_entries += retiring_entries.size();
      for (auto &entry: retiring_entries) {
	if (entry->is_write() || entry->is_writesame()) {
	  assert(m_bytes_cached >= entry->write_bytes());
	  m_bytes_cached -= entry->write_bytes();
	  uint64_t entry_allocation_size = entry->write_bytes();
	  if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
	    entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
	  }
	  assert(m_bytes_allocated >= entry_allocation_size);
	  m_bytes_allocated -= entry_allocation_size;
	} else	if (entry->ram_entry.is_discard()) {
	  /* Discards don't record any allocated or cached bytes,
	   * but do record dirty bytes until they're flushed. */
	}
      }
      m_alloc_failed_since_retire = false;
      wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

/*
 * Flushes all dirty log entries, then flushes the cache below. On completion
 * there will be no dirty entries.
 *
 * Optionally invalidates the cache (RWL invalidate interface comes here with
 * invalidate=true). On completion with invalidate=true there will be no entries
 * in the log. Until the next write, all subsequent reads will complete from the
 * layer below. When invalidatingm the cache below is invalidated instead of
 * flushed.
 *
 * If discard_unflushed_writes is true, invalidate must also be true. Unflushed
 * writes are discarded instead of flushed.
*/
template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish, bool invalidate, bool discard_unflushed_writes) {
  ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
			     << "invalidate=" << invalidate
			     << " discard_unflushed_writes=" << discard_unflushed_writes << dendl;
  if (m_perfcounter) {
    if (discard_unflushed_writes) {
      ldout(m_image_ctx.cct, 1) << "Write back cache discarded (not flushed)" << dendl;
      m_perfcounter->inc(l_librbd_rwl_invalidate_discard_cache, 1);
    } else if (invalidate) {
      m_perfcounter->inc(l_librbd_rwl_invalidate_cache, 1);
    } else {
      m_perfcounter->inc(l_librbd_rwl_flush, 1);
    }
  }

  internal_flush(on_finish, invalidate, discard_unflushed_writes);
}

template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
  flush(on_finish, m_image_ctx.rwl_invalidate_on_flush, false);
};

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish, bool discard_unflushed_writes) {
  flush(on_finish, true, discard_unflushed_writes);
};

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) {
  invalidate(on_finish, false);
};

template <typename I>
void ReplicatedWriteLog<I>::internal_flush(Context *on_finish, bool invalidate, bool discard_unflushed_writes) {
  ldout(m_image_ctx.cct, 20) << "invalidate=" << invalidate
			     << " discard_unflushed_writes=" << discard_unflushed_writes << dendl;
  if (discard_unflushed_writes) {
    assert(invalidate);
  }

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(m_image_ctx.cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish);
    return;
  }

  /* Flush/invalidate must pass through block guard to ensure all layers of
   * cache are consistently flush/invalidated. This ensures no in-flight write leaves
   * some layers with valid regions, which may later produce inconsistent read
   * results. */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext(
      [this, on_finish, invalidate, discard_unflushed_writes](GuardedRequestFunctionContext &guard_ctx) {
	DeferredContexts on_exit;
	ldout(m_image_ctx.cct, 20) << "cell=" << guard_ctx.m_cell << dendl;
	assert(guard_ctx.m_cell);

	Context *ctx = new FunctionContext(
	  [this, cell=guard_ctx.m_cell, invalidate, discard_unflushed_writes, on_finish](int r) {
	    Mutex::Locker locker(m_lock);
	    m_invalidating = false;
	    ldout(m_image_ctx.cct, 6) << "Done flush/invalidating (invalidate="
				      << invalidate << "discard="
				      << discard_unflushed_writes << ")" << dendl;
	    if (m_log_entries.size()) {
	      ldout(m_image_ctx.cct, 1) << "m_log_entries.size()=" << m_log_entries.size() << ", "
					<< "front()=" << *m_log_entries.front() << dendl;
	    }
	    if (invalidate) {
	      assert(m_log_entries.size() == 0);
	    }
	    assert(m_dirty_log_entries.size() == 0);
	    m_image_ctx.op_work_queue->queue(on_finish, r);
	    release_guarded_request(cell);
	  });
	ctx = new FunctionContext(
	  [this, ctx, invalidate, discard_unflushed_writes](int r) {
	    Context *next_ctx = ctx;
	    if (r < 0) {
	      /* Override on_finish status with this error */
	      next_ctx = new FunctionContext([r, ctx](int _r) {
		  ctx->complete(r);
		});
	    }
	    if (invalidate) {
	      {
		Mutex::Locker locker(m_lock);
		assert(m_dirty_log_entries.size() == 0);
		if (discard_unflushed_writes) {
		  assert(m_invalidating);
		} else {
		  /* If discard_unflushed_writes was false, we should only now be
		   * setting m_invalidating. All writes are now flushed.  with
		   * m_invalidating set, retire_entries() will proceed without
		   * the normal limits that keep it from interfering with
		   * appending new writes (we hold the block guard, so that can't
		   * be happening). */
		  assert(!m_invalidating);
		  ldout(m_image_ctx.cct, 6) << "Invalidating" << dendl;
		  m_invalidating = true;
		}
	      }
	      /* Discards all RWL entries */
	      while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }
	      /* Invalidate from caches below */
	      m_image_writeback->invalidate(next_ctx);
	    } else {
	      {
		Mutex::Locker locker(m_lock);
		assert(m_dirty_log_entries.size() == 0);
		  assert(!m_invalidating);
	      }
	      m_image_writeback->flush(next_ctx);
	    }
	  });
	ctx = new FunctionContext(
	  [this, ctx, discard_unflushed_writes](int r) {
	    /* If discard_unflushed_writes was true, m_invalidating should be
	     * set now.
	     *
	     * With m_invalidating set, flush discards everything in the dirty
	     * entries list without writing them to OSDs. It also waits for
	     * in-flight flushes to complete, and keeps the flushing stats
	     * consistent.
	     *
	     * If discard_unflushed_writes was false, this is a normal
	     * flush. */
	    {
		Mutex::Locker locker(m_lock);
		assert(m_invalidating == discard_unflushed_writes);
	    }
	    flush_dirty_entries(ctx);
	  });
	Mutex::Locker locker(m_lock);
	if (discard_unflushed_writes) {
	  ldout(m_image_ctx.cct, 6) << "Invalidating" << dendl;
	  m_invalidating = true;
	}
	/* Even if we're throwing everything away, but we want the last entry to
	 * be a sync point so we can cleanly resume.
	 *
	 * Also, the blockguard only guarantees the replication of this op
	 * can't overlap with prior ops. It doesn't guarantee those are all
	 * completed and eligible for flush & retire, which we require here.
	 */
	auto flush_req = make_flush_req(ctx);
	flush_new_sync_point_if_needed(flush_req, on_exit);
      });
  BlockExtent invalidate_block_extent(block_extent(whole_volume_extent()));
  detain_guarded_request(GuardedRequest(invalidate_block_extent,
					guarded_ctx, true));
}

/*
 * RWL internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 *
 * Librbd internal flushes will arrive at flush(invalidate=false,
 * discard=false), and traverse the block guard to ensure in-flight writes are
 * flushed.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_dirty_entries(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;

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
	flush_dirty_entries(on_finish);
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
