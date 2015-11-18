// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_H
#define CEPH_OS_BLUESTORE_BLUEFS_H

#include "bluefs_types.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "BlockDevice.h"

class Allocator;

class BlueFS {
public:
  struct File {
    bluefs_fnode_t fnode;
    int refs;
    bool dirty;

    File()
      : refs(0),
	dirty(false)
      {}
  };

  struct Dir {
    map<string,File*> file_map;
  };

  struct FileWriter {
    File *file;
    bufferlist buffer;      ///< new data to write (at end of file)
    bufferlist tail_block;  ///< existing partial block at end of file, if any

    FileWriter(File *f) : file(f) {}

    void append(bufferlist& bl) {
      buffer.claim_append(bl);
    }
    void append(bufferptr& bp) {
      buffer.append(bp);
    }
  };

  struct FileReader {
    File *file;
    uint64_t pos;           ///< current logical offset
    uint64_t max_prefetch;  ///< max allowed prefetch
    bool ignore_eof;        ///< used when reading our log file

    uint64_t bl_off;  ///< prefetch buffer logical offset
    bufferlist bl;    ///< prefetch buffer

    FileReader(File *f, uint64_t mpf, bool ie = false)
      : file(f),
	pos(0),
	max_prefetch(mpf),
	ignore_eof(ie),
	bl_off(0)
      { }

    uint64_t get_buf_end() {
      return bl_off + bl.length();
    }
    uint64_t get_buf_remaining() {
      if (bl_off + bl.length() > pos)
	return bl_off + bl.length() - pos;
      return 0;
    }
    
    void skip(size_t n) {
      pos += n;
    }
    void seek(uint64_t offset) {
      pos = offset;
    }
  };
  
private:
  Mutex lock;
  Cond cond;

  // cache
  map<string, Dir*> dir_map;                    ///< dirname -> Dir
  ceph::unordered_map<uint64_t,File*> file_map; ///< ino -> File

  bluefs_super_t super;       ///< latest superblock (as last written)
  uint64_t ino_last;          ///< last assigned ino (this one is in use)
  uint64_t log_seq;           ///< last used log seq (by current pending log_t)
  FileWriter *log_writer;     ///< writer for the log
  bluefs_transaction_t log_t; ///< pending, unwritten log transaction

  IOContext ioc;              ///< IOContext for all of our IO

  vector<BlockDevice*> bdev;                  ///< block devices we can use
  vector<interval_set<uint64_t> > block_all;  ///< extents in bdev we own
  vector<Allocator*> alloc;                   ///< allocators for bdevs

  void _init_alloc();
  
  File *_get_file(uint64_t ino);
  void _rm_file(File *f);
  
  int _allocate(unsigned bdev, uint64_t len, vector<bluefs_extent_t> *ev);
  int _flush(FileWriter *h);
  int _flush_log();
  void _fsync(FileWriter *h);

  
  int _read(
    FileReader *h,  ///< [in] read from here
    size_t len,     ///< [in] this many bytes
    bufferptr *bp,  ///< [out] optional: reference the result here
    char *out);     ///< [out] optional: or copy it here

  int _open_super(uint64_t super_offset_a, uint64_t super_offset_b);
  int _write_super();
  int _replay(); ///< replay journal

public:
  BlueFS();
  ~BlueFS();

  // the super is always stored on bdev 0
  int mkfs(uint64_t super_offset_a, uint64_t super_offset_b);
  int mount(uint64_t super_offset_a, uint64_t super_offset_b);
  void umount();

  uint64_t get_total(unsigned id);
  uint64_t get_free(unsigned id);

  int create_and_open_for_write(
    const string& dir,
    const string& file,
    FileWriter **h);
  int open_for_read(
    const string& dir,
    const string& file,
    FileReader **h,
    bool random = false);
  
  /// sync any uncommitted state to disk
  int sync();

  /// compact metadata  
  int compact();

  int add_block_device(unsigned bdev, string path);

  /// gift more block space
  void add_block_extent(unsigned bdev, uint64_t offset, uint64_t len);

  void flush(FileWriter *h) {
    Mutex::Locker l(lock);
    _flush(h);
  }
  void fsync(FileWriter *h) {
    Mutex::Locker l(lock);
    _fsync(h);
  }
  int read(FileReader *h, size_t len, bufferptr *bp, char *out) {
    Mutex::Locker l(lock);
    return _read(h, len, bp, out);
  }
};

#endif
