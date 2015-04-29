// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "include/util.h"

#include "mds/CInode.h"

#include "DataScan.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

void DataScan::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-data-scan init\n"
    << "  cephfs-data-scan scan_extents <data pool name>\n"
    << "  cephfs-data-scan scan_inodes <data pool name>\n"
    << std::endl;

  generic_client_usage();
}

int DataScan::main(const std::vector<const char*> &args)
{
  // Parse args
  // ==========
  if (args.size() < 1) {
    usage();
    return -EINVAL;
  }

  // Common RADOS init: open metadata pool
  // =====================================
  librados::Rados rados;
  int r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable" << dendl;
    return r;
  }

  dout(4) << "connecting to RADOS..." << dendl;
  rados.connect();

  {
    int const metadata_pool_id = mdsmap->get_metadata_pool();
    dout(4) << "resolving metadata pool " << metadata_pool_id << dendl;
    std::string metadata_pool_name;
    r = rados.pool_reverse_lookup(metadata_pool_id, &metadata_pool_name);
    if (r < 0) {
      derr << "Pool " << metadata_pool_id
        << " identified in MDS map not found in RADOS!" << dendl;
      return r;
    }
    dout(4) << "found metadata pool '" << metadata_pool_name << "'" << dendl;
    r = rados.ioctx_create(metadata_pool_name.c_str(), metadata_io);
    if (r != 0) {
      return r;
    }
  }

  std::string const &command = args[0];
  if (command == "scan_inodes" || command == "scan_extents") {
    if (args.size() < 2) {
      usage();
      return -EINVAL;
    }

    const std::string data_pool_name = args[1];
    {
      data_pool_id = rados.pool_lookup(data_pool_name.c_str());
      if (data_pool_id < 0) {
        std::cerr << "Data pool '" << data_pool_name << "' not found!" << std::endl;
        return -ENOENT;
      } else {
        dout(4) << "data pool '" << data_pool_name
          << "' has ID " << data_pool_id << dendl;
      }

      if (!mdsmap->is_data_pool(data_pool_id)) {
        std::cerr << "Warning: pool '" << data_pool_name << "' is not a "
          "CephFS data pool!" << std::endl;
      }

      dout(4) << "opening data pool '" << data_pool_name << "'" << dendl;
      r = rados.ioctx_create(data_pool_name.c_str(), data_io);
      if (r != 0) {
        return r;
      }
    }

    // Parse `n` and `m` arguments
    if (args.size() >= 4) {
      std::string err;
      n = strict_strtoll(args[2], 10, &err);
      if (!err.empty()) {
        std::cerr << "Invalid worker number '" << args[2] << "'" << std::endl;
        return -EINVAL;
      }
      m = strict_strtoll(args[3], 10, &err);
      if (!err.empty()) {
        std::cerr << "Invalid worker count '" << args[3] << "'" << std::endl;
        return -EINVAL;
      }
    }

    if (command == "scan_inodes") {
      return recover();
    } else if (command == "scan_extents") {
      return recover_extents();
    }

  } else if (command == "init") {
    return init_metadata();
  } else {
    std::cerr << "Unknown command '" << command << "'" << std::endl;
    return -EINVAL;
  }

  return recover();
}

int DataScan::inject_unlinked_inode(inodeno_t inono, int mode)
{
  // Compose
  object_t oid = InodeStore::get_object_name(inono, frag_t(), ".inode");
  InodeStore inode;
  inode.inode.ino = inono;
  inode.inode.version = 1;
  inode.inode.xattr_version = 1;
  inode.inode.mode = 0500 | mode;
  // Fake size to 1, so that the directory doesn't appear to be empty
  // (we won't actually give the *correct* size here though)
  inode.inode.size = 1;
  inode.inode.dirstat.nfiles = 1;

  inode.inode.ctime = 
    inode.inode.mtime = ceph_clock_now(g_ceph_context);
  inode.inode.nlink = 1;
  inode.inode.truncate_size = -1ull;
  inode.inode.truncate_seq = 1;

  // Force layout to default: should we let users override this so that
  // they don't have to mount the filesystem to correct it?
  inode.inode.layout = g_default_file_layout;
  inode.inode.layout.fl_pg_pool = mdsmap->get_first_data_pool();

  // Serialize
  bufferlist inode_bl;
  ::encode(std::string(CEPH_FS_ONDISK_MAGIC), inode_bl);
  inode.encode(inode_bl);

  // Write
  int r = metadata_io.write_full(oid.name, inode_bl);
  if (r != 0) {
    derr << "Error writing '" << oid.name << "': " << cpp_strerror(r) << dendl;
    return r;
  }

  return r;
}

int DataScan::root_exists(inodeno_t ino, bool *result)
{
  object_t oid = InodeStore::get_object_name(ino, frag_t(), ".inode");
  uint64_t size;
  time_t mtime;
  int r = metadata_io.stat(oid.name, &size, &mtime);
  if (r == -ENOENT) {
    *result = false;
    return 0;
  } else if (r < 0) {
    return r;
  }

  *result = true;
  return 0;
}

int DataScan::init_metadata()
{
  int r = 0;
  r = inject_unlinked_inode(MDS_INO_ROOT, S_IFDIR|0755);
  if (r != 0) {
    return r;
  }
  r = inject_unlinked_inode(MDS_INO_MDSDIR(0), S_IFDIR);
  if (r != 0) {
    return r;
  }

  return 0;
}

int DataScan::check_roots(bool *result)
{
  int r;
  r = root_exists(MDS_INO_ROOT, result);
  if (r != 0) {
    return r;
  }
  if (!*result) {
    return 0;
  }

  r = root_exists(MDS_INO_MDSDIR(0), result);
  if (r != 0) {
    return r;
  }
  if (!*result) {
    return 0;
  }

  return 0;
}

/**
 * Stages:
 * PARALLEL
 *  1. Size and mtime recovery: scan ALL objects, and update 0th
 *   objects with max size and max mtime seen.
 * PARALLEL
 *  2. Inode recovery: scan ONLY 0th objects, and inject metadata
 *   into dirfrag OMAPs, creating blank dirfrags as needed.  No stats
 *   or rstats at this stage.  Inodes without backtraces go into
 *   lost+found
 * SERIAL
 *  3. Dirfrag statistics: depth first traverse into metadata tree,
 *    rebuilding dir sizes.
 *    XXX hmm, *or* we could use an object class to accumulate
 *    the number of insertions we've done on any given frag, and
 *    then do a pass where we just inject that accumulated number
 *    of insertions into the directory's size and the dirfrags stats?
 * PARALLEL
 *  4. Cleanup; go over all 0th objects (and dirfrags if we tagged
 *   anything onto them) and remove any of the xattrs that we
 *   used for accumulating.
 */


int parse_oid(const std::string &oid, uint64_t *inode_no, uint64_t *obj_id)
{
  if (oid.find(".") == std::string::npos || oid.find(".") == oid.size() - 1) {
    return -EINVAL;
  }

  std::string err;
  std::string inode_str = oid.substr(0, oid.find("."));
  *inode_no = strict_strtoll(inode_str.c_str(), 16, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  std::string pos_string = oid.substr(oid.find(".") + 1);
  *obj_id = strict_strtoll(pos_string.c_str(), 16, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  return 0;
}

int DataScan::recover_extents()
{
  float progress = 0.0;
  librados::NObjectIterator i = data_io.nobjects_begin(n, m);
  librados::NObjectIterator i_end = data_io.nobjects_end();
  int r = 0;

  for (; i != i_end; ++i) {
    const std::string oid = i->get_oid();
    if (i.get_progress() != progress) {
      if (int(i.get_progress() * 100) / 5 != int(progress * 100) / 5) {
        std::cerr << percentify(i.get_progress()) << "%" << std::endl;
      }
      progress = i.get_progress();
    }

    // Read size
    uint64_t size;
    time_t mtime;
    // FIXME: by using librados, I throw away precision because it wants
    // to make me use time_t instead of utime_t :-(
    r = data_io.stat(oid, &size, &mtime);
    if (r != 0) {
      dout(4) << "Cannot stat '" << oid << "': skipping" << dendl;
      continue;
    }

    // I need to keep track of
    //  * The highest object ID seen
    //  * The size of the highest object ID seen
    //  * The largest object seen
    //
    //  Given those things, I can later infer the object chunking
    //  size, the offset of the last object (chunk size * highest ID seen)
    //  and the actual size (offset of last object + size of highest ID seen)
    //
    //  This logic doesn't take account of striping.
    uint64_t inode_no = 0;
    uint64_t obj_id = 0;
    r = parse_oid(oid, &inode_no, &obj_id);
    if (r != 0) {
      dout(4) << "Bad object name '" << oid << "' skipping" << dendl;
      continue;
    }

    // Generate 0th object name, where we will accumulate sizes/mtimes
    object_t zeroth_object = InodeStore::get_object_name(inode_no, frag_t(), "");

    // Construct a librados operation invoking our class method
    librados::ObjectReadOperation op;
    bufferlist inbl;
    ::encode(std::string("scan_size"), inbl);
    ::encode(std::string("scan_mtime"), inbl);
    ::encode(obj_id, inbl);
    ::encode(size, inbl);
    ::encode(mtime, inbl);

    op.exec("cephfs", "accumulate_inode_metadata", inbl);

    bufferlist outbl;
    int r = data_io.operate(zeroth_object.name, &op, &outbl);
    if (r < 0) {
      derr << "Failed to store size data from '"
        << oid << "' to '" << zeroth_object.name << "': "
        << cpp_strerror(r) << dendl;
      continue;
    }
  }

  return 0;
}

/*
 * Other handy commands:
 *  A Find inode by backtrace expression, e.g. "myimportantfile"
 *  B Selectively recover only particular inodes, e.g. those identified
 *    by A.
 *  C When recovering inodes, write files out to local disk instead of
 *    trying to inject them into the CephFS metadata pool.
 */

int DataScan::recover()
{
  float progress = 0.0;
  librados::NObjectIterator i = data_io.nobjects_begin(n, m);
  librados::NObjectIterator i_end = data_io.nobjects_end();

  bool roots_present;
  int r = check_roots(&roots_present);
  if (r != 0) {
    derr << "Unexpected error checking roots: '"
      << cpp_strerror(r) << "'" << dendl;
    return r;
  }

  if (!roots_present) {
    std::cerr << "Some or all system inodes are absent.  Run 'init' from "
      "one node before running 'recover'" << std::endl;
    return -EIO;
  }

  for (; i != i_end; ++i) {
    const std::string oid = i->get_oid();
    if (i.get_progress() != progress) {
      if (int(i.get_progress() * 100) / 5 != int(progress * 100) / 5) {
        std::cerr << percentify(i.get_progress()) << "%" << std::endl;
      }
      progress = i.get_progress();
    }

    uint64_t obj_name_ino = 0;
    uint64_t obj_name_offset = 0;
    r = parse_oid(oid, &obj_name_ino, &obj_name_offset);
    if (r != 0) {
      dout(4) << "Bad object name '" << oid << "', skipping" << dendl;
      continue;
    }

    // We are only interested in 0th objects during this phase: we touched
    // the other objects during recover_extents
    if (obj_name_offset != 0) {
      continue;
    }

    // Read backtrace
    bool have_backtrace = false;
    bufferlist parent_bl;
    int r = data_io.getxattr(oid, "parent", parent_bl);
    if (r == -ENODATA) {
      dout(10) << "No backtrace on '" << oid << "'" << dendl;
    } else if (r < 0) {
      // TODO: accumulate these errors in a structure we can output
      dout(4) << "Unexpected error on '" << oid << "': " << cpp_strerror(r) << dendl;
      continue;
    }

    // Deserialize backtrace
    inode_backtrace_t backtrace;
    if (parent_bl.length()) {
      try {
        bufferlist::iterator q = parent_bl.begin();
        backtrace.decode(q);
        have_backtrace = true;
      } catch (buffer::error &e) {
        dout(4) << "Corrupt backtrace on '" << oid << "': " << e << dendl;
      }
    }

    // Santity checking backtrace ino against object name
    if (have_backtrace && backtrace.ino != obj_name_ino) {
      dout(4) << "Backtrace ino 0x" << std::hex << backtrace.ino
        << " doesn't match object name ino 0x" << obj_name_ino
        << std::dec << dendl;
      have_backtrace = false;
    }

    // Read size
    uint64_t obj_size = 0;
    time_t obj_mtime = 0;
    r = data_io.stat(oid, &obj_size, &obj_mtime);
    if (r != 0) {
      dout(4) << "Cannot stat '" << oid << "': skipping" << dendl;
      continue;
    }

    // Read accumulated size from scan_extents phase
    uint64_t file_size = 0;
    uint32_t chunk_size = g_default_file_layout.fl_object_size;
    bufferlist scan_size_bl;
    r = data_io.getxattr(oid, "scan_size", scan_size_bl);
    if (r >= 0) {
      try {
        bufferlist::iterator scan_size_bl_iter = scan_size_bl.begin();
        uint64_t max_obj_id;
        uint64_t max_obj_size;
        ::decode(max_obj_id, scan_size_bl_iter);
        ::decode(max_obj_size, scan_size_bl_iter);

        if (max_obj_id > 0) {
          // If there are more objects, and this object is a power of two
          // that is greater than the size of the last object, then make
          // an educated guess that this object's size is probably the chunk
          // size.
        
          // TODO: provide a means for the user to specify the layout, as
          // for e.g. striped layouts there is no way we can infer it.
          if ((obj_size & (obj_size - 1)) == 0 && obj_size >= max_obj_size) {
            chunk_size = obj_size;
          } else {
            chunk_size = g_default_file_layout.fl_object_size;
          }
          file_size = chunk_size * max_obj_id + max_obj_size;
        } else {
          file_size = obj_size;
        }
      } catch (const buffer::error &err) {
        dout(4) << "Invalid size attr on '" << oid << "'" << dendl;
        file_size = obj_size;
      }
    } else {
      file_size = obj_size;
    }

    // Read accumulated mtime from scan_extents phase
    time_t file_mtime = 0;
    bufferlist scan_mtime_bl;
    r = data_io.getxattr(oid, "scan_mtime", scan_mtime_bl);
    if (r >= 0) {
      try {
        bufferlist::iterator scan_mtime_bl_iter = scan_mtime_bl.begin();
        ::decode(file_mtime, scan_mtime_bl_iter);
      } catch (const buffer::error &err) {
        dout(4) << "Invalid mtime attr on '" << oid << "'" << dendl;
        file_mtime = obj_mtime;
      }
    } else {
      file_mtime = obj_mtime;
    }

    // Inject inode to the metadata pool
    if (have_backtrace) {
      // TODO: inspect backtrace and fall back to lost+found
      // for some or all stray cases.
      r = inject_with_backtrace(backtrace, file_size, file_mtime, chunk_size);
      if (r < 0) {
        dout(4) << "Error injecting 0x" << std::hex << backtrace.ino
          << std::dec << " with backtrace: " << cpp_strerror(r) << dendl;
      }
    } else {
      r = inject_lost_and_found(obj_name_ino, file_size, file_mtime, chunk_size);
      if (r < 0) {
        dout(4) << "Error injecting 0x" << std::hex << obj_name_ino
          << std::dec << " into lost+found: " << cpp_strerror(r) << dendl;
      }
    }
  }

  return 0;
}

int DataScan::read_fnode(inodeno_t ino, frag_t frag, fnode_t *fnode)
{
  assert(fnode != NULL);

  object_t frag_oid = InodeStore::get_object_name(ino, frag, "");
  bufferlist old_fnode_bl;
  int r = metadata_io.omap_get_header(frag_oid.name, &old_fnode_bl);
  if (r < 0) {
    return r;
  }

  bufferlist::iterator old_fnode_iter = old_fnode_bl.begin();
  try {
    fnode_t old_fnode;
    old_fnode.decode(old_fnode_iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return 0;
}

int DataScan::read_dentry(inodeno_t parent_ino, frag_t frag,
                const std::string &dname, InodeStore *inode)
{
  assert(inode != NULL);

  std::string key;
  dentry_key_t dn_key(CEPH_NOSNAP, dname.c_str());
  dn_key.encode(key);

  std::set<std::string> keys;
  keys.insert(key);
  std::map<std::string, bufferlist> vals;
  object_t frag_oid = InodeStore::get_object_name(parent_ino, frag, "");
  int r = metadata_io.omap_get_vals_by_keys(frag_oid.name, keys, &vals);  
  assert (r == 0);  // I assume success because I checked object existed
  if (vals.find(key) == vals.end()) {
    return -ENOENT;
  }

  try {
    bufferlist::iterator q = vals[key].begin();
    inode->decode_bare(q);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return 0;
}

int DataScan::inject_lost_and_found(
    inodeno_t ino, uint64_t file_size, time_t file_mtime, uint32_t chunk_size)
{
  // Create lost+found if doesn't exist
  bool created = false;
  int r = find_or_create_dirfrag(CEPH_INO_ROOT, &created);
  if (r < 0) {
    return r;
  }
  InodeStore lf_ino;
  r = read_dentry(CEPH_INO_ROOT, frag_t(), "lost+found", &lf_ino);
  if (r == -ENOENT || r == -EINVAL) {
    // Inject dentry
    lf_ino.inode.mode = 0755 | S_IFDIR;
    lf_ino.inode.dirstat.nfiles = 1;
    lf_ino.inode.size = 1;
    lf_ino.inode.nlink = 1;
    lf_ino.inode.ino = CEPH_INO_LOST_AND_FOUND;
    lf_ino.inode.version = 1;
    lf_ino.inode.backtrace_version = 1;
    r = inject_linkage(CEPH_INO_ROOT, "lost+found", lf_ino);
    if (r < 0) {
      return r;
    }
  } else {
    if (!(lf_ino.inode.mode & S_IFDIR)) {
      derr << "lost+found exists but is not a directory!" << dendl;
      // In this case we error out, and the user should do something about
      // this problem.
      return -EINVAL;
    }
  }

  r = find_or_create_dirfrag(CEPH_INO_LOST_AND_FOUND, &created);
  if (r < 0) {
    return r;
  }

  InodeStore recovered_ino;
  recovered_ino.inode.mode = 0500 | S_IFREG;
  recovered_ino.inode.size = file_size;
  recovered_ino.inode.max_size_ever = file_size;
  recovered_ino.inode.mtime.tv.tv_sec = file_mtime;
  recovered_ino.inode.atime.tv.tv_sec = file_mtime;
  recovered_ino.inode.ctime.tv.tv_sec = file_mtime;

  recovered_ino.inode.layout = g_default_file_layout;
  recovered_ino.inode.layout.fl_object_size = chunk_size;
  recovered_ino.inode.layout.fl_stripe_unit = chunk_size;
  recovered_ino.inode.layout.fl_pg_pool = data_pool_id;

  recovered_ino.inode.truncate_seq = 1;
  recovered_ino.inode.truncate_size = -1ull;

  recovered_ino.inode.inline_data.version = CEPH_INLINE_NONE;

  recovered_ino.inode.nlink = 1;
  recovered_ino.inode.ino = ino;
  recovered_ino.inode.version = 1;
  recovered_ino.inode.backtrace_version = 1;


  char s[20];
  snprintf(s, sizeof(s), "%llx", (unsigned long long)ino);
  const std::string dname = s;

  // Write dentry into lost+found dirfrag
  return inject_linkage(lf_ino.inode.ino, dname, recovered_ino);
}

int DataScan::inject_with_backtrace(
    const inode_backtrace_t &backtrace, uint64_t file_size, time_t file_mtime,
    uint32_t chunk_size)
{
  // My immediate ancestry should be correct, so if we can find that
  // directory's dirfrag then go inject it there

  // There are various strategies here:
  //   - when a parent dentry doesn't exist, create it with a new inono (i.e.
  //     don't care about inode numbers for directories at all)
  //   - when a parent dentry doesn't exist, create it using the inodeno
  //     from the backtrace: assumes that nothing else in the hierarchy
  //     exists, so there won't be dupes
  //   - only insert inodes when their direct parent directory fragment
  //     already exists: this only risks multiple-linkage of files,
  //     rather than directories.

  // When creating linkage for a directory, *only* create it if we are
  // also creating the object.  That way, we might not manage to get the
  // *right* linkage for a directory, but at least we won't multiply link
  // it.  We assume that if a root dirfrag exists for a directory, then
  // it is linked somewhere (i.e. that the metadata pool is not already
  // inconsistent).
  // Making sure *that* is true is someone else's job!  Probably someone
  // who is not going to run in parallel, so that they can self-consistently
  // look at versions and move things around as they go.
  inodeno_t ino = backtrace.ino;
  dout(10) << "  inode: 0x" << std::hex << ino << std::dec << dendl;
  for (std::vector<inode_backpointer_t>::const_iterator i = backtrace.ancestors.begin();
      i != backtrace.ancestors.end(); ++i) {
    const inode_backpointer_t &backptr = *i;
    dout(10) << "  backptr: 0x" << std::hex << backptr.dirino << std::dec
      << "/" << backptr.dname << dendl;

    // TODO handle fragmented directories: if there is a root that
    // contains a valid fragtree, use it to decide where to inject.  Else (the simple
    // case) just always inject into the root.
    // TODO: look where the fragtree tells you, not just in the root.
    //       to get the fragtree we will have to read the backtrace
    //       on the dirfrag to learn who the immediate parent of
    //       it is.

    // Examine root dirfrag for parent
    const inodeno_t parent_ino = backptr.dirino;
    const std::string dname = backptr.dname;
    object_t frag_oid = InodeStore::get_object_name(parent_ino, frag_t(), "");

    // Find or create dirfrag
    // ======================
    bool created_dirfrag;
    int r = find_or_create_dirfrag(parent_ino, &created_dirfrag);
    if (r < 0) {
      break;
    }

    // TODO: if backtrace does not end at root, and the most distance ancestor
    // did not exist, then link the most distant ancestor into lost+found

    // Check if dentry already exists
    // ==============================
    InodeStore existing_dentry;
    r = read_dentry(parent_ino, frag_t(), dname, &existing_dentry);
    bool write_dentry = false;
    if (r == -ENOENT || r == -EINVAL) {
      // Missing or corrupt dentry
      write_dentry = true;
    } else if (r < 0) {
      derr << "Unexpected error reading dentry 0x" << std::hex
        << parent_ino << std::dec << "/"
        << dname << ": " << cpp_strerror(r) << dendl;
      break;
    } else {
      // Dentry already present, does it link to me?
      if (existing_dentry.inode.ino == ino) {
        dout(20) << "Dentry 0x" << std::hex
          << parent_ino << std::dec << "/"
          << dname << " already exists and points to me" << dendl;
        // TODO: an option to overwrite the size data here with
        // what we just recovered?  Or another tool that lets you
        // do that...
      } else {
        // FIXME: at this point we should set a flag to recover
        // this inode in a /_recovery/<inodeno>.data file as we
        // can't recover it into its desired filesystem position.
        derr << "Dentry 0x" << std::hex
          << parent_ino << std::dec << "/"
          << dname << " already exists but points to 0x"
          << std::hex << existing_dentry.inode.ino << std::dec << dendl;
        // Fall back to lost+found!
        break;
      }
    }

    // Inject linkage
    // ==============
    if (write_dentry) {
      InodeStore dentry;
      if (i == backtrace.ancestors.begin()) {
        // This is the linkage for a file
        dentry.inode.mode = 0500 | S_IFREG;
        dout(10) << "Linking inode 0x" << std::hex << ino
          << " at 0x" << parent_ino << "/" << dname << std::dec
          << " with size=" << file_size << " bytes" << dendl;

        // The file size and mtime we learned by scanning globally
        dentry.inode.size = file_size;
        dentry.inode.max_size_ever = file_size;
        dentry.inode.mtime.tv.tv_sec = file_mtime;
        dentry.inode.atime.tv.tv_sec = file_mtime;
        dentry.inode.ctime.tv.tv_sec = file_mtime;

        dentry.inode.layout = g_default_file_layout;
        dentry.inode.layout.fl_object_size = chunk_size;
        dentry.inode.layout.fl_stripe_unit = chunk_size;
        dentry.inode.layout.fl_pg_pool = data_pool_id;

        dentry.inode.truncate_seq = 1;
        dentry.inode.truncate_size = -1ull;

        dentry.inode.inline_data.version = CEPH_INLINE_NONE;
      } else {
        // This is the linkage for a directory
        dentry.inode.mode = 0755 | S_IFDIR;

        // Set nfiles to something non-zero, to fool any other code
        // that tries to ignore 'empty' directories.  This won't be
        // accurate, but it should avoid functional issues.
        dentry.inode.dirstat.nfiles = 1;
        dentry.inode.size = 1;

      }
      dentry.inode.nlink = 1;
      dentry.inode.ino = ino;
      dentry.inode.version = 1;
      dentry.inode.backtrace_version = 1;
      r = inject_linkage(parent_ino, dname, dentry);
      if (r < 0) {
        break;
      }
    }

    if (!created_dirfrag) {
      // If the parent dirfrag already existed, then stop traversing the
      // backtrace: assume that the other ancestors already exist too.  This
      // is an assumption rather than a truth, but it's a convenient way
      // to avoid the risk of creating multiply-linked directories while
      // injecting data.  If there are in fact missing ancestors, this
      // should be fixed up using a separate tool scanning the metadata
      // pool.
      break;
    } else {
      // Proceed up the backtrace, creating parents
      ino = parent_ino;
    }

    // TODO ensure that injected inode has layout pointing to this pool (if
    // none of its ancestor layouts does)

    // TODO handle backtraces pointing to stray dirs of MDS ranks that
    // don't exist

    // TODO handle strays in general: if something was stray but had
    // hard links, we can't know its linked name, but we can shove it
    // some recovery directory.

    // TODO for objects with no backtrace, OPTIONALLY (don't do it by
    // default) write them a /_recovered/<inodeno>.data backtrace and
    // link them in there.  In general backtrace-less objects are
    // just journalled metadata that hasn't landed yet, we should only
    // override that if we are explicitly told to, or if a full
    // forward scrub has failed to tag them.

    // TODO scan objects for size of recovered inode.  We can either
    // do this inline here, OR we can rely on a full scan also
    // touching all other objects, and updating max size of inode
    // every time we see it (but that gets complicated with multiple
    // workers).  Maybe also a fast path for <4MB objects that sets size
    // to the size of the 0000th object when that size is <4MB.  More generally,
    // we could do this in an initial pass that just looks at all objects,
    // and sets an xattr on the 0000th object (including creating it if necessary)
    // to the maximum seen (may need a special RADOS op to do a "set if greater"
    // xattr write)
  }

  return 0;
}

int DataScan::find_or_create_dirfrag(inodeno_t ino, bool *created)
{
  assert(created != NULL);

  fnode_t existing_fnode;
  *created = false;

  object_t frag_oid = InodeStore::get_object_name(ino, frag_t(), "");

  int r = read_fnode(ino, frag_t(), &existing_fnode);
  if (r == -ENOENT || r == -EINVAL) {
    // Missing or corrupt fnode, create afresh
    bufferlist fnode_bl;
    fnode_t blank_fnode;
    blank_fnode.version = 1;
    blank_fnode.encode(fnode_bl);
    r = metadata_io.omap_set_header(frag_oid.name, fnode_bl);
    if (r < 0) {
      derr << "Failed to create dirfrag 0x" << std::hex
        << ino << std::dec << ": " << cpp_strerror(r) << dendl;
      return r;
    } else {
      dout(10) << "Created dirfrag: 0x" << std::hex
        << ino << std::dec << dendl;
      *created = true;
    }
  } else if (r < 0) {
    derr << "Unexpected error reading dirfrag 0x" << std::hex
      << ino << std::dec << " : " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(20) << "Dirfrag already exists: 0x" << std::hex
      << ino << std::dec << dendl;
  }

  return 0;
}

int DataScan::inject_linkage(
    inodeno_t dir_ino, const std::string &dname, const InodeStore &inode)
{
  // We have no information about snapshots, so everything goes
  // in as CEPH_NOSNAP
  snapid_t snap = CEPH_NOSNAP;

  object_t frag_oid = InodeStore::get_object_name(dir_ino, frag_t(), "");

  std::string key;
  dentry_key_t dn_key(snap, dname.c_str());
  dn_key.encode(key);

  bufferlist dentry_bl;
  ::encode(snap, dentry_bl);
  ::encode('I', dentry_bl);
  inode.encode_bare(dentry_bl);

  // Write out
  std::map<std::string, bufferlist> vals;
  vals[key] = dentry_bl;
  int r = metadata_io.omap_set(frag_oid.name, vals);
  if (r != 0) {
    derr << "Error writing dentry 0x" << std::hex
      << dir_ino << std::dec << "/"
      << dname << ": " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(20) << "Injected dentry 0x" << std::hex
      << dir_ino << "/" << dname << " pointing to 0x"
      << inode.inode.ino << std::dec << dendl;
    return 0;
  }
}
