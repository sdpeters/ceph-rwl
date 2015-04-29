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


#include "MDSUtility.h"
#include "include/rados/librados.hpp"

class InodeStore;

class DataScan : public MDSUtility
{
  protected:
    // IoCtx for metadata pool (where we inject inodes to)
    librados::IoCtx metadata_io;
    // IoCtx for data pool (where we scrap backtraces from)
    librados::IoCtx data_io;
    // Remember the data pool ID for use in layouts
    int64_t data_pool_id;

    uint32_t n;
    uint32_t m;

    /**
     * Pre-injection check that all the roots are present in
     * the metadata pool.  Used to avoid parallel workers interfering
     * with one another, by cueing the user to go run 'init' on a
     * single node before running a parallel scan.
     *
     * @param result: set to true if roots are present, else set to false
     * @returns 0 on no unexpected errors, else error code.  Missing objects
     *          are not considered an unexpected error: check *result for
     *          this case.
     */
    int check_roots(bool *result);

    /**
     * Create any missing roots (i.e. mydir, strays, root inode)
     */
    int init_metadata();

    /**
     * Scan data pool for backtraces, and inject inodes to metadata pool
     */
    int recover();

    /**
     * Scan data pool for file sizes and mtimes
     */
    int recover_extents();

    /**
     * Create a .inode object, i.e. root or mydir
     */
    int inject_unlinked_inode(inodeno_t inono, int mode);

    /**
     * Check for existence of .inode objects, before
     * trying to go ahead and inject metadata.
     */
    int root_exists(inodeno_t ino, bool *result);

    /**
     * Try and read an fnode from a dirfrag
     */
    int read_fnode(inodeno_t ino, frag_t frag, fnode_t *fnode);

    /**
     * Try and read a dentry from a dirfrag
     */
    int read_dentry(inodeno_t parent_ino, frag_t frag,
                    const std::string &dname, InodeStore *inode);

    /**
     * Inject an inode + dentry parents into the metadata pool,
     * based on a backtrace recovered from the data pool
     */
    int inject_with_backtrace(
        const inode_backtrace_t &bt, uint64_t size, time_t mtime,
        uint32_t chunk_size);

    /**
     * Inject and inode + dentry into the lost+found directory,
     * when all wel know about a file is its inode.
     */
    int inject_lost_and_found(
        inodeno_t ino, uint64_t size, time_t mtime, uint32_t chunk_size);

    int find_or_create_dirfrag(inodeno_t ino, bool *created);

    int inject_linkage(
        inodeno_t dir_ino, const std::string &dname, const InodeStore &inode);

  public:
    void usage();
    int main(const std::vector<const char *> &args);

    DataScan()
      : data_pool_id(-1), n(0), m(1)
    {}
};

