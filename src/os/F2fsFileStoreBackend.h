// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_F2FSFILESTOREBACKEDN_H
#define CEPH_F2FSFILESTOREBACKEDN_H

#if defined(__linux__)
#include "GenericFileStoreBackend.h"

class F2fsFileStoreBackend : public GenericFileStoreBackend {
private:
  bool m_can_snap;
public:
  F2fsFileStoreBackend(FileStore *fs);
  ~F2fsFileStoreBackend() {};
  const char *get_name() {
    return "f2fs";
  }
  int detect_features();
  bool can_checkpoint();
  int list_checkpoints(list<string>& ls);
  int create_checkpoint(const string& name, uint64_t *cid);
  int sync_checkpoint(uint64_t cid);
  int rollback_to(const string& name);
  int destroy_checkpoint(const string& name);
};
#endif
#endif
