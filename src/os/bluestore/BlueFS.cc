// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueFS.h"

#include "common/debug.h"
#include "common/errno.h"
#include "BlockDevice.h"
#include "Allocator.h"
#include "StupidAllocator.h"


#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs "

BlueFS::BlueFS()
  : lock("BlueFS::lock"),
    ino_last(1),
    log_seq(1),
    log_writer(NULL),
    ioc(static_cast<void*>(this))
{
  log_t.seq = 1;
}

BlueFS::~BlueFS()
{
}

int BlueFS::add_block_device(unsigned id, string path)
{
  dout(10) << __func__ << " bdev " << id << " path " << path << dendl;
  assert(id == bdev.size());
  BlockDevice *b = new BlockDevice(NULL, NULL);  // no aio callback; use ioc
  int r = b->open(path);
  if (r < 0) {
    delete b;
    return r;
  }
  dout(1) << __func__ << " bdev " << id << " path " << path
	  << " size " << pretty_si_t(b->get_size()) << "B" << dendl;
  bdev.push_back(b);
  block_all.resize(bdev.size());
  return 0;
}

void BlueFS::add_block_extent(unsigned id, uint64_t offset, uint64_t length)
{
  dout(1) << __func__ << " bdev " << id << " " << offset << "~" << length
	  << dendl;
  assert(id < bdev.size());
  assert(bdev[id]->get_size() >= offset + length);
  block_all[id].insert(offset, length);
}

uint64_t BlueFS::get_total(unsigned id)
{
  Mutex::Locker l(lock);
  assert(id < block_all.size());
  uint64_t r = 0;
  interval_set<uint64_t>& p = block_all[id];
  for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
    r += q.get_len();
  }
  return r;       
}

uint64_t BlueFS::get_free(unsigned id)
{
  Mutex::Locker l(lock);
  assert(id < alloc.size());
  return alloc[id]->get_free();
}

int BlueFS::mkfs(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(1) << __func__
	  << " super offsets " << super_offset_a << " " << super_offset_b
	  << dendl;
  assert(bdev.size() >= 1);

  _init_alloc();

  // init log
  File *log_file = new File;
  log_file->fnode.ino = 1;
  _allocate(0, g_conf->bluefs_alloc_size, &log_file->fnode.extents);
  log_writer = new FileWriter(log_file);

  // initial txn
  log_t.op_init();
  for (unsigned bdev = 0; bdev < block_all.size(); ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      dout(20) << __func__ << " op_alloc_add " << bdev << " " << q.get_start()
	       << "~" << q.get_len() << dendl;
      log_t.op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  dout(20) << __func__ << " op_file_update " << log_file->fnode << dendl;  
  log_t.op_file_update(log_file->fnode);
  _flush_log();
    
  // write supers
  super.version = 0;
  super.super_a_offset = super_offset_a;
  super.super_b_offset = super_offset_b;
  super.block_size = bdev[0]->get_block_size();
  super.log_fnode = log_file->fnode;
  _write_super();
  super.version = 1;
  _write_super();
  ioc.aio_wait();
  bdev[0]->flush();

  // clean up
  super = bluefs_super_t();
  delete log_writer;
  log_writer = NULL;
  delete log_file;
  block_all.clear();
  alloc.clear();

  dout(10) << __func__ << " done" << dendl;
  return 0;
}

void BlueFS::_init_alloc()
{
  dout(20) << __func__ << dendl;
  alloc.resize(bdev.size());
  for (unsigned id = 0; id < bdev.size(); ++id) {
    alloc[id] = new StupidAllocator;
    interval_set<uint64_t>& p = block_all[id];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      alloc[id]->init_add_free(q.get_start(), q.get_len());
    }
  }
}

int BlueFS::mount(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(1) << __func__ << " super offsets " << super_offset_a
	  << " " << super_offset_b << dendl;
  assert(!bdev.empty());

  int r = _open_super(super_offset_a, super_offset_b);
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    goto out;
  }

  block_all.clear();
  block_all.resize(bdev.size());
  _init_alloc();

  r = _replay();
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
    goto out;
  }

  // init freelist
  for (auto p : file_map) {
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto q : p.second->fnode.extents) {
      alloc[q.bdev]->init_rm_free(q.offset, q.length);
    }
  }

  // set up the log for future writes
  log_t.seq = ++log_seq;
  log_writer = new FileWriter(_get_file(1));
  assert(log_writer->file->fnode.ino == 1);
  return 0;

 out:
  super = bluefs_super_t();
  return r;
}

void BlueFS::umount()
{
  dout(1) << __func__ << dendl;


}

int BlueFS::_write_super()
{
  // build superblock
  bufferlist bl;
  ::encode(super, bl);
  uint32_t crc = bl.crc32c(-1);
  ::encode(crc, bl);
  assert(bl.length() <= super.block_size);
  bufferptr z(super.block_size - bl.length());
  z.zero();
  bl.append(z);
  bl.rebuild();  

  uint64_t off = (super.version & 1) ?
    super.super_b_offset : super.super_a_offset;
  bdev[0]->aio_write(off, bl, &ioc);
  dout(20) << __func__ << " v " << super.version << " crc " << crc
	   << " offset " << off << dendl;
  return 0;
}

int BlueFS::_open_super(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(10) << __func__ << dendl;

  bufferlist abl, bbl, t;
  bluefs_super_t a, b;
  uint32_t a_crc, b_crc, crc;
  int r;

  r = bdev[0]->read(super_offset_a, bdev[0]->get_block_size(), &abl, &ioc);
  if (r < 0)
    return r;
  r = bdev[0]->read(super_offset_b, bdev[0]->get_block_size(), &bbl, &ioc);
  if (r < 0)
    return r;

  bufferlist::iterator p = abl.begin();
  ::decode(a, p);
  {
    bufferlist t;
    t.substr_of(abl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  ::decode(a_crc, p);
  if (crc != a_crc) {
    derr << __func__ << " bad crc on superblock a, expected " << a_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }

  p = bbl.begin();
  ::decode(b, p);
  {
    bufferlist t;
    t.substr_of(bbl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  ::decode(b_crc, p);
  if (crc != b_crc) {
    derr << __func__ << " bad crc on superblock a, expected " << a_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }

  dout(10) << __func__ << " superblock a " << a.version
	   << " b " << b.version
	   << dendl;

  if (a.version == b.version + 1) {
    dout(10) << __func__ << " using a" << dendl;
    super = a;
  } else if (b.version == a.version + 1) {
    dout(10) << __func__ << " using b" << dendl;
    super = b;
  } else {
    derr << __func__ << " non-adjacent superblock versions "
	 << a.version << " and " << b.version << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  return 0;
}

int BlueFS::_replay()
{
  dout(10) << __func__ << dendl;
  ino_last = 1;
  log_seq = 1;

  File *log_file = _get_file(1);
  log_file->fnode = super.log_fnode;

  FileReader *log_reader = new FileReader(
    log_file, g_conf->bluefs_alloc_size,
    true);  // ignore eof
  while (true) {
    assert((log_reader->pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->pos;
    bufferlist bl;
    {
      bufferptr bp;
      int r = _read(log_reader, super.block_size, &bp, NULL);
      assert(r == (int)super.block_size);
      bl.append(bp);
    }
    uint64_t more = 0;
    uint64_t seq;
    {
      bufferlist::iterator p = bl.begin();
      __u8 a, b;
      uint32_t len;
      ::decode(a, p);
      ::decode(b, p);
      ::decode(len, p);
      ::decode(seq, p);
      if (len - 6 > bl.length()) {
	more = ROUND_UP_TO(len + 6 - bl.length(), super.block_size);
      }
    }
    if (seq != log_seq) {
      dout(10) << __func__ << " " << pos << ": stop: seq " << seq
	       << " != expected " << log_seq << dendl;
      break;
    }
    if (more) {
      bufferptr bp;
      int r = _read(log_reader, more, &bp, NULL);
      if (r < (int)more) {
	dout(10) << __func__ << " " << pos << ": stop: len is "
		 << bl.length() + more << ", which is past eof" << dendl;
	break;
      }
      assert(r == (int)more);
      bl.append(bp);
    }
    bluefs_transaction_t t;
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(t, p);
    }
    catch (buffer::error& e) {
      dout(10) << __func__ << " " << pos << ": stop: failed to decode: "
	       << e.what() << dendl;
      break;
    }
    assert(seq == t.seq);

    bool okay = true;
    bufferlist::iterator p = t.op_bl.begin();
    while (okay && !p.end()) {
      __u8 op;
      ::decode(op, p);
      switch (op) {

      case bluefs_transaction_t::OP_INIT:
	dout(20) << __func__ << " " << pos << ": op_init" << dendl;
	assert(log_seq == 1);
	break;

      case bluefs_transaction_t::OP_ALLOC_ADD:
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " " << pos << ": op_alloc_add "
		   << " " << (int)id << ":" << offset << "~" << length << dendl;
	  block_all[id].insert(offset, length);
	  alloc[id]->init_add_free(offset, length);
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_RM:
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " " << pos << ": op_alloc_add "
		   << " " << (int)id << ":" << offset << "~" << length << dendl;
	  block_all[id].erase(offset, length);	  
	  alloc[id]->init_rm_free(offset, length);
	}
	break;

      case bluefs_transaction_t::OP_DIR_LINK:
        {
	}
	break;

      case bluefs_transaction_t::OP_DIR_UNLINK:
        {
	}
	break;

      case bluefs_transaction_t::OP_FILE_UPDATE:
        {
	  bluefs_fnode_t fnode;
	  ::decode(fnode, p);
	  dout(20) << __func__ << " " << pos << ": op_file_update "
		   << " " << fnode << dendl;
	  File *f = _get_file(fnode.ino);
	  f->fnode = fnode;
	  if (fnode.ino > ino_last) {
	    ino_last = fnode.ino;
	  }
	}
	break;

      case bluefs_transaction_t::OP_FILE_RM:
        {
	  uint64_t ino;
	  ::decode(ino, p);
	  dout(20) << __func__ << " " << pos << ": op_file_rm " << ino << dendl;
	  auto p = file_map.find(ino);
	  assert(p != file_map.end());
	  _rm_file(p->second);
	}
	break;

      default:
	derr << __func__ << " " << pos << ": stop: unrecognized op " << (int)op
	     << dendl;
	okay = false;
	break;
      }
    }

    ++log_seq;
  }

  dout(10) << __func__ << " log file size was " << log_reader->pos << dendl;
  log_file->fnode.size = log_reader->pos;
  log_file->dirty = true;
  delete log_reader;

  dout(10) << __func__ << " done" << dendl;  
  return 0;
}

BlueFS::File *BlueFS::_get_file(uint64_t ino)
{
  File *f;
  auto p = file_map.find(ino);
  if (p == file_map.end()) {
    f = new File;
    file_map[ino] = f;
    dout(20) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;    
    return f;
  } else {
    dout(20) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}

void BlueFS::_rm_file(File *f)
{
  dout(20) << __func__ << " " << f->fnode << dendl;
  for (auto p : f->fnode.extents) {
    alloc[p.bdev]->release(p.offset, p.length);
  }
  file_map.erase(f->fnode.ino);
  delete f;
}

int BlueFS::_read(
  FileReader *h,  ///< [in] read from here
  size_t len,     ///< [in] this many bytes
  bufferptr *bp,  ///< [out] optional: reference the result here
  char *out)      ///< [out] optional: or copy it here
{
  dout(10) << __func__ << " h " << h << " len " << len << dendl;
  if (!h->ignore_eof &&
      h->pos + len > h->file->fnode.size) {
    len = h->file->fnode.size - h->pos;
    dout(20) << __func__ << " reaching eof, len clipped to " << len << dendl;
  }

  int left;
  if (h->pos < h->bl_off || h->pos >= h->get_buf_end()) {
    h->bl.clear();
    h->bl_off = h->pos & super.block_mask();
    uint64_t x_off = 0;
    vector<bluefs_extent_t>::iterator p = h->file->fnode.seek(h->bl_off, &x_off);
    uint64_t l = MIN(p->length - x_off, h->max_prefetch);
    dout(20) << __func__ << " fetching " << x_off << "~" << l << " of "
	     << *p << dendl;
    int r = bdev[p->bdev]->read(p->offset + x_off, l, &h->bl, &ioc);
    assert(r == 0);
  }
  left = h->get_buf_remaining();

  int r = MIN(len, left);
  // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
  if (bp)
    *bp = bufferptr(h->bl.c_str() + h->pos - h->bl_off, r);
  if (out)
    memcpy(out, h->bl.c_str() + h->pos - h->bl_off, r);
  h->pos += r;
  return r;
}

int BlueFS::_flush_log()
{
  dout(10) << __func__ << " " << log_t << dendl;
  assert(!log_t.empty());
  bufferlist bl;
  ::encode(log_t, bl);

  // pad to block boundary
  uint64_t partial = bl.length() % super.block_size;
  if (partial) {
    bufferptr z(super.block_size - partial);
    dout(10) << __func__ << " padding with " << z.length() << " zeros" << dendl;
    z.zero();
    bufferlist zbl;
    zbl.append(z);
    bl.append(z);
  }
  log_writer->append(bl);

  log_t.clear();
  log_t.seq = ++log_seq;

  return _flush(log_writer);
}

int BlueFS::_flush(FileWriter *h)
{
  uint64_t length = h->buffer.length();
  uint64_t offset = h->file->fnode.size;
  dout(10) << __func__ << " " << h << " " << offset << "~" << length
	   << " to " << h->file->fnode << dendl;

  uint64_t allocated = h->file->fnode.get_allocated();
  if (allocated < offset + length) {
    int r = _allocate(0, offset + length - allocated, &h->file->fnode.extents);
    if (r < 0)
      return r;
  }
  h->file->fnode.size += length;

  uint64_t x_off = 0;
  vector<bluefs_extent_t>::iterator p = h->file->fnode.seek(offset, &x_off);
  assert(p != h->file->fnode.extents.end());
  dout(20) << __func__ << " in " << *p << " x_off " << x_off << dendl;

  unsigned partial = x_off & ~super.block_mask();
  bufferlist bl;
  if (partial) {
    dout(20) << __func__ << " using partial tail " << partial << dendl;
    assert(h->tail_block.length());
    bl.claim_append(h->tail_block);
    x_off -= partial;
    length += partial;
  }
  bl.claim_append(h->buffer);

  h->tail_block.clear();

  uint64_t bloff = 0;
  while (length > 0) {
    uint64_t wlen = MIN(p->length, length);
    bufferlist t;
    t.substr_of(bl, bloff, wlen);
    unsigned tail = wlen & ~super.block_mask();
    if (tail) {
      dout(20) << __func__ << " caching tail of " << tail << dendl;
      h->tail_block.substr_of(bl, bl.length() - tail, tail);
      bufferptr z(super.block_size - tail);
      z.zero();
      t.append(z);
    }
    bdev[0]->aio_write(p->offset + x_off, t, &ioc);
    bloff += wlen;
    length -= wlen;
    ++p;
    x_off = 0;
  }

  log_t.op_file_update(h->file->fnode);

  return 0;
}

void BlueFS::_fsync(FileWriter *h)
{
  dout(10) << __func__ << " " << h << " " << h->file->fnode << dendl;
  _flush(h);
  if (h->file->dirty) {
    log_t.op_file_update(h->file->fnode);
    _flush_log();
  }
  for (auto p : bdev) {
    p->flush();
  }
}

int BlueFS::_allocate(unsigned id, uint64_t len, vector<bluefs_extent_t> *ev)
{
  dout(10) << __func__ << " len " << len << " from " << id << dendl;
  assert(id < alloc.size());

  uint64_t left = ROUND_UP_TO(len, g_conf->bluefs_alloc_size);
  int r = alloc[id]->reserve(left);
  if (r < 0)
    return r;

  uint64_t hint = 0;
  if (!ev->empty()) {
    hint = ev->back().end();
  }
  while (left > 0) {
    bluefs_extent_t e;
    e.bdev = id;
    int r = alloc[id]->allocate(left, g_conf->bluefs_alloc_size, hint,
				&e.offset, &e.length);
    if (r < 0)
      return r;
    ev->push_back(e);
    if (e.length >= left)
      break;
    left -= e.length;
    hint = e.end();
  }
  return 0;
}

int BlueFS::create_and_open_for_write(
  const string& dirname,
  const string& filename,
  FileWriter **h)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  Dir *dir;
  if (p == dir_map.end()) {
    // implicitly create the dir
    dout(20) << __func__ << "  creating dir " << dirname
	     << " (" << dir << ")" << dendl;
    dir = new Dir;
    dir_map[dirname] = dir;
  } else {
    dir = p->second;
  }

  File *file;
  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    file = new File;
    file->fnode.ino = ++ino_last;
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
  } else {
    // overwrite existing file?
    assert(0 == "not implemented");
  }

  log_t.op_file_update(file->fnode);
  log_t.op_dir_link(dirname, filename, file->fnode.ino);

  *h = new FileWriter(file);
  return 0;
}

int BlueFS::open_for_read(
  const string& dirname,
  const string& filename,
  FileReader **h,
  bool random)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;

  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second;

  *h = new FileReader(file, random ? 4096 : g_conf->bluefs_max_prefetch);
  return 0;
}
