// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"

#include "common/debug.h"
#include "common/errno.h"
#include "os/WBThrottle.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "WBThrottle: "

WBThrottle::WBThrottle(CephContext *cct) :
#ifdef HAVE_LIBAIO
  aio_fsync(cct->_conf->filestore_experimental_enable_aio_fsync),
  aio_in_flight(0), next_aio(0),
#endif
  cur_ios(0), cur_size(0),
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

#ifdef HAVE_LIBAIO
  if (aio_fsync) {
    iocbs.resize(cct->_conf->filestore_experimental_aio_fsyncs_limit);
    io_events.resize(iocbs.size());
    io_setup(iocbs.size(), &ctxp);
  }
#endif
}

#ifdef HAVE_LIBAIO
void WBThrottle::wait_fsync_completions(unsigned num) {
  if (aio_in_flight == 0)
    return;
  do {
    int r = io_getevents(ctxp, 1, iocbs.size(), &(io_events[0]), NULL);
    assert(r > 0);
    assert((unsigned)r <= aio_in_flight);
    for (int i = 0; i < r; ++i) {
      aiocb *cb = reinterpret_cast<aiocb*>(io_events[i].obj);
      assert(!cb->done);
      cb->done = true;
    }
    while (aio_in_flight > 0) {
      assert(next_aio >= aio_in_flight);
      int slot = (next_aio - aio_in_flight) % iocbs.size();
      if (iocbs[slot].done) {
	{
	  Mutex::Locker l(lock);
	  complete(iocbs[slot].wb);
	}
	iocbs[slot].clear();
	aio_in_flight--;
      } else {
	break;
      }
    }
  } while (aio_in_flight > num);
}

void WBThrottle::do_aio_fsync(FDRef fd, const PendingWB &wb)
{
  wait_fsync_completions(iocbs.size() - 1);
  int slot = next_aio++ % iocbs.size();
  aio_in_flight++;

  int r = 0;
  int tries = 100;
  while (r >= 0 && r < 1 && tries-- > 0) {
    assert(!iocbs[slot].done);
    iocbs[slot].wb = wb;
    iocbs[slot].fd = fd;
    r = io_fsync(ctxp, &(iocbs[slot].cb), NULL, **fd);
  }
  if (r != 1) {
    int err = errno;
    dout(0) << "io_fsync failed with r = " << r
	    << " and errno: " << err
	    << dendl;
    assert(0 == "error return from io_fsync");
  }
}
#endif

WBThrottle::~WBThrottle() {
#ifdef HAVE_LIBAIO
  wait_fsync_completions(0);
#endif
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
  create();
}

void WBThrottle::stop()
{
  {
    Mutex::Locker l(lock);
    stopping = true;
    cond.Signal();
  }

  join();
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

#ifdef HAVE_LIBAIO
  if (!stopping &&
      cur_ios < io_limits.first &&
      pending_wbs.size() < fd_limits.first &&
      cur_size < size_limits.first &&
      aio_in_flight > 0) {
    lock.Unlock();
    wait_fsync_completions(0);
    lock.Lock();
  }
#endif

  while (!stopping &&
         cur_ios < io_limits.first &&
         pending_wbs.size() < fd_limits.first &&
         cur_size < size_limits.first) {
    cond.Wait(lock);
  }
  if (stopping)
    return false;
  assert(!pending_wbs.empty());
  ghobject_t obj(pop_object());
  
  map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
    pending_wbs.find(obj);
  *next = boost::make_tuple(obj, i->second.second, i->second.first);
  pending_wbs.erase(i);
  return true;
}


void *WBThrottle::entry()
{
  Mutex::Locker l(lock);
  boost::tuple<ghobject_t, FDRef, PendingWB> wb;
  while (get_next_should_flush(&wb)) {
    clearing = wb.get<0>();
    lock.Unlock();
#ifdef HAVE_LIBAIO
    if (aio_fsync) {
      do_aio_fsync(wb.get<1>(), wb.get<2>());
      lock.Lock();
    } else {
#else
#ifdef HAVE_FDATASYNC
    ::fdatasync(**wb.get<1>());
#else
    ::fsync(**wb.get<1>());
#endif
#endif

#ifdef HAVE_POSIX_FADVISE
    if (wb.get<2>().nocache) {
      int fa_r = posix_fadvise(**wb.get<1>(), 0, 0, POSIX_FADV_DONTNEED);
      assert(fa_r == 0);
    }
#endif


    lock.Lock();
    clearing = ghobject_t();
    complete(wb.get<2>());

#ifdef HAVE_LIBAIO
    }
#endif

    wb = boost::tuple<ghobject_t, FDRef, PendingWB>();
  }
  return 0;
}

void WBThrottle::complete(const PendingWB &wb)
{
    cur_ios -= wb.ios;
    logger->dec(l_wbthrottle_ios_dirtied, wb.ios);
    cur_size -= wb.size;
    logger->dec(l_wbthrottle_bytes_dirtied, wb.size);
    logger->dec(l_wbthrottle_inodes_dirtied);
    cond.Signal();
}

void WBThrottle::queue_wb(
  FDRef fd, const ghobject_t &hoid, uint64_t offset, uint64_t len,
  bool nocache)
{
  Mutex::Locker l(lock);
  map<ghobject_t, pair<PendingWB, FDRef> >::iterator wbiter =
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
  logger->inc(l_wbthrottle_ios_dirtied);
  cur_size += len;
  logger->inc(l_wbthrottle_bytes_dirtied, len);

  wbiter->second.first.add(nocache, len, 1);
  insert_object(hoid);
  cond.Signal();
}

void WBThrottle::clear()
{
  Mutex::Locker l(lock);
  for (map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
	 pending_wbs.begin();
       i != pending_wbs.end();
       ++i) {
    cur_ios -= i->second.first.ios;
    logger->dec(l_wbthrottle_ios_dirtied, i->second.first.ios);
    cur_size -= i->second.first.size;
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
  while (clearing == hoid)
    cond.Wait(lock);
  map<ghobject_t, pair<PendingWB, FDRef> >::iterator i =
    pending_wbs.find(hoid);
  if (i == pending_wbs.end())
    return;

  cur_ios -= i->second.first.ios;
  logger->dec(l_wbthrottle_ios_dirtied, i->second.first.ios);
  cur_size -= i->second.first.size;
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
