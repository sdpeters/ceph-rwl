// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL

#include "ReplicatedWriteLog.h"
#include "librbd/ImageCtx.h"
#include "rwl/SharedPtrContext.h"
#include <map>
#include <vector>

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

static const bool RWL_VERBOSE_LOGGING = false;

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
SyncPoint<T>::SyncPoint(T &rwl, const uint64_t sync_gen_num)
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
} // namespace cache
} // namespace librbd


#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
