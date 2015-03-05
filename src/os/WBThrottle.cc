// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"

#include "os/WBThrottle.h"
#include "common/perf_counters.h"

WBThrottle::WBThrottle(CephContext *cct) :
  cur_ios(0), cur_size(0),
  cur_ios_pre_fsync(0), cur_size_pre_fsync(0),
  cct(cct),
  logger(NULL),
  stopping(true),
  lock("WBThrottle::lock", false, true, false, cct),
  fs(XFS)
{
  {
    Mutex::Locker l(lock);
    set_from_conf();
  }
  assert(cct);
  PerfCountersBuilder b(
    cct, string("WBThrottle"),
    l_wbthrottle_first, l_wbthrottle_last);
  b.add_u64(l_wbthrottle_bytes_dirtied, "bytes_dirtied");
  b.add_u64(l_wbthrottle_bytes_wb, "bytes_wb");
  b.add_u64(l_wbthrottle_ios_dirtied, "ios_dirtied");
  b.add_u64(l_wbthrottle_ios_wb, "ios_wb");
  b.add_u64(l_wbthrottle_inodes_dirtied, "inodes_dirtied");
  b.add_u64(l_wbthrottle_inodes_wb, "inodes_wb");
  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  for (unsigned i = l_wbthrottle_first + 1; i != l_wbthrottle_last; ++i)
    logger->set(i, 0);

  cct->_conf->add_observer(this);
}

WBThrottle::~WBThrottle() {
  assert(cct);
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
  cct->_conf->remove_observer(this);
}

void WBThrottle::start()
{
  {
    Mutex::Locker l(lock);
    stopping = false;
  }
  flusher_threads.resize(g_conf->filestore_wbthrottle_flusher_threads);
  for (vector<FlusherThread*>::iterator i = flusher_threads.begin();
       i != flusher_threads.end();
       ++i) {
    *i = new FlusherThread(this);
    (*i)->create();
  }
}

void WBThrottle::stop()
{
  {
    Mutex::Locker l(lock);
    stopping = true;
    cond.Signal();
  }

  for (vector<FlusherThread*>::iterator i = flusher_threads.begin();
       i != flusher_threads.end();
       ++i) {
    (*i)->join();
    delete *i;
  }
  flusher_threads.clear();
}

const char** WBThrottle::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "filestore_wbthrottle_btrfs_bytes_start_flusher",
    "filestore_wbthrottle_btrfs_bytes_hard_limit",
    "filestore_wbthrottle_btrfs_ios_start_flusher",
    "filestore_wbthrottle_btrfs_ios_hard_limit",
    "filestore_wbthrottle_btrfs_inodes_start_flusher",
    "filestore_wbthrottle_btrfs_inodes_hard_limit",
    "filestore_wbthrottle_xfs_bytes_start_flusher",
    "filestore_wbthrottle_xfs_bytes_hard_limit",
    "filestore_wbthrottle_xfs_ios_start_flusher",
    "filestore_wbthrottle_xfs_ios_hard_limit",
    "filestore_wbthrottle_xfs_inodes_start_flusher",
    "filestore_wbthrottle_xfs_inodes_hard_limit",
    NULL
  };
  return KEYS;
}

void WBThrottle::set_from_conf()
{
  assert(lock.is_locked());
  if (fs == BTRFS) {
    size_limits.first =
      cct->_conf->filestore_wbthrottle_btrfs_bytes_start_flusher;
    size_limits.second =
      cct->_conf->filestore_wbthrottle_btrfs_bytes_hard_limit;
    io_limits.first =
      cct->_conf->filestore_wbthrottle_btrfs_ios_start_flusher;
    io_limits.second =
      cct->_conf->filestore_wbthrottle_btrfs_ios_hard_limit;
    fd_limits.first =
      cct->_conf->filestore_wbthrottle_btrfs_inodes_start_flusher;
    fd_limits.second =
      cct->_conf->filestore_wbthrottle_btrfs_inodes_hard_limit;
  } else if (fs == XFS) {
    size_limits.first =
      cct->_conf->filestore_wbthrottle_xfs_bytes_start_flusher;
    size_limits.second =
      cct->_conf->filestore_wbthrottle_xfs_bytes_hard_limit;
    io_limits.first =
      cct->_conf->filestore_wbthrottle_xfs_ios_start_flusher;
    io_limits.second =
      cct->_conf->filestore_wbthrottle_xfs_ios_hard_limit;
    fd_limits.first =
      cct->_conf->filestore_wbthrottle_xfs_inodes_start_flusher;
    fd_limits.second =
      cct->_conf->filestore_wbthrottle_xfs_inodes_hard_limit;
  } else {
    assert(0 == "invalid value for fs");
  }
  cond.Signal();
}

void WBThrottle::handle_conf_change(const md_config_t *conf,
				    const std::set<std::string> &changed)
{
  Mutex::Locker l(lock);
  for (const char** i = get_tracked_conf_keys(); *i; ++i) {
    if (changed.count(*i)) {
      set_from_conf();
      return;
    }
  }
}

bool WBThrottle::get_next_should_flush(
  boost::tuple<ghobject_t, FDRef, PendingWB> *next)
{
  assert(lock.is_locked());
  assert(next);
  while (!stopping &&
	 cur_ios_pre_fsync < io_limits.first &&
	 pending_wbs.size() < fd_limits.first &&
	 cur_size_pre_fsync < size_limits.first)
    cond.Wait(lock);
  if (stopping)
    return false;
  assert(!pending_wbs.empty());
  ghobject_t obj(pop_object());
  
  ceph::unordered_map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
    pending_wbs.find(obj);
  *next = boost::make_tuple(obj, i->second.second, i->second.first);
  pending_wbs.erase(i);
  cur_ios_pre_fsync -= next->get<2>().ios;
  cur_size_pre_fsync -= next->get<2>().size;
  return true;
}


void *WBThrottle::FlusherThread::entry()
{
  Mutex::Locker l(parent->lock);
  boost::tuple<ghobject_t, FDRef, PendingWB> wb;
  while (parent->get_next_should_flush(&wb)) {
    ghobject_t clearing = wb.get<0>();
    parent->clearing_in_progress.insert(clearing);
    parent->lock.Unlock();
#ifdef HAVE_FDATASYNC
    ::fdatasync(**wb.get<1>());
#else
    ::fsync(**wb.get<1>());
#endif
#ifdef HAVE_POSIX_FADVISE
    if (g_conf->filestore_fadvise && wb.get<2>().nocache) {
      int fa_r = posix_fadvise(**wb.get<1>(), 0, 0, POSIX_FADV_DONTNEED);
      assert(fa_r == 0);
    }
#endif
    parent->lock.Lock();
    parent->clearing_in_progress.erase(clearing);
    clearing = ghobject_t();
    parent->cur_ios -= wb.get<2>().ios;
    parent->logger->dec(l_wbthrottle_ios_dirtied, wb.get<2>().ios);
    parent->logger->inc(l_wbthrottle_ios_wb, wb.get<2>().ios);
    parent->cur_size -= wb.get<2>().size;
    parent->logger->dec(l_wbthrottle_bytes_dirtied, wb.get<2>().size);
    parent->logger->inc(l_wbthrottle_bytes_wb, wb.get<2>().size);
    parent->logger->dec(l_wbthrottle_inodes_dirtied);
    parent->logger->inc(l_wbthrottle_inodes_wb);
    parent->cond.Signal();
    wb = boost::tuple<ghobject_t, FDRef, PendingWB>();
  }
  return 0;
}

void WBThrottle::queue_wb(
  FDRef fd, const ghobject_t &hoid, uint64_t offset, uint64_t len,
  bool nocache)
{
  Mutex::Locker l(lock);
  ceph::unordered_map<ghobject_t, pair<PendingWB, FDRef> >::iterator wbiter =
    pending_wbs.find(hoid);
  if (wbiter == pending_wbs.end()) {
    wbiter = pending_wbs.insert(
      make_pair(hoid,
	make_pair(
	  PendingWB(),
	  fd))).first;
    logger->inc(l_wbthrottle_inodes_dirtied);
  } else {
    remove_object(hoid);
  }

  cur_ios++;
  cur_ios_pre_fsync++;
  logger->inc(l_wbthrottle_ios_dirtied);
  cur_size += len;
  cur_size_pre_fsync += len;
  logger->inc(l_wbthrottle_bytes_dirtied, len);

  wbiter->second.first.add(nocache, len, 1);
  insert_object(hoid);
  cond.Signal();
}

void WBThrottle::clear()
{
  Mutex::Locker l(lock);
  for (ceph::unordered_map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
	 pending_wbs.begin();
       i != pending_wbs.end();
       ++i) {
#ifdef HAVE_POSIX_FADVISE
    if (g_conf->filestore_fadvise && i->second.first.nocache) {
      int fa_r = posix_fadvise(**i->second.second, 0, 0, POSIX_FADV_DONTNEED);
      assert(fa_r == 0);
    }
#endif

    cur_ios -= i->second.first.ios;
    cur_ios_pre_fsync -= i->second.first.ios;
    logger->dec(l_wbthrottle_ios_dirtied, i->second.first.ios);
    cur_size -= i->second.first.size;
    cur_size_pre_fsync -= i->second.first.size;
    logger->dec(l_wbthrottle_bytes_dirtied, i->second.first.size);
    logger->dec(l_wbthrottle_inodes_dirtied);
  }
  pending_wbs.clear();
  lru.clear();
  rev_lru.clear();
  cond.Signal();
}

void WBThrottle::clear_object(const ghobject_t &hoid)
{
  Mutex::Locker l(lock);
  while (clearing_in_progress.count(hoid))
    cond.Wait(lock);
  ceph::unordered_map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
    pending_wbs.find(hoid);
  if (i == pending_wbs.end())
    return;

  cur_ios -= i->second.first.ios;
  cur_ios_pre_fsync -= i->second.first.ios;
  logger->dec(l_wbthrottle_ios_dirtied, i->second.first.ios);
  cur_size -= i->second.first.size;
  cur_size_pre_fsync -= i->second.first.size;
  logger->dec(l_wbthrottle_bytes_dirtied, i->second.first.size);
  logger->dec(l_wbthrottle_inodes_dirtied);

  pending_wbs.erase(i);
  remove_object(hoid);
}

void WBThrottle::throttle()
{
  Mutex::Locker l(lock);
  while (!stopping && !(
	   cur_ios < io_limits.second &&
	   pending_wbs.size() < fd_limits.second &&
	   cur_size < size_limits.second)) {
    cond.Wait(lock);
  }
}
