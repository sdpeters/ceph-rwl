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

#include "include/int_types.h"
#include "include/types.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include "include/compat.h"
#include "include/linux_fiemap.h"
#include "include/color.h"
#include "include/buffer.h"
#include "include/assert.h"

#include <iostream>
#include <fstream>
#include <sstream>

#include "F2fsFileStoreBackend.h"

#include "common/errno.h"
#include "common/config.h"

#if defined(__linux__)


#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "f2fsfilestorebackend(" << get_basedir_path() << ") "

#define ALIGN_DOWN(x, by) ((x) - ((x) % (by)))
#define ALIGNED(x, by) (!((x) % (by)))
#define ALIGN_UP(x, by) (ALIGNED((x), (by)) ? (x) : (ALIGN_DOWN((x), (by)) + (by)))

F2fsFileStoreBackend::F2fsFileStoreBackend(FileStore *fs)
  : GenericFileStoreBackend(fs),
    m_can_snap(false)
{}

int F2fsFileStoreBackend::detect_features()
{
  int r;

  r = GenericFileStoreBackend::detect_features();
  if (r < 0)
    return r;

  // XXX detect whether f2fs has the necessary snap ioctl(s)

  return 0;
}

bool F2fsFileStoreBackend::can_checkpoint()
{
  return m_can_snap;
}

int F2fsFileStoreBackend::list_checkpoints(list<string>& ls)
{
  if (!m_can_snap)
    return 0;

  // XXX return list of checkpoint names

  return 0;
}

int F2fsFileStoreBackend::create_checkpoint(const string& name, uint64_t *transid)
{
  dout(10) << "create_checkpoint: '" << name << "'" << dendl;
  assert(m_can_snap);

  // XXX create a named snap.
  // if there is a second 'wait' type ioctl that waits for it to become
  // durable, return the cookie in transid.  otherwise ignore it.

  return 0;
}

int F2fsFileStoreBackend::sync_checkpoint(uint64_t transid)
{
  assert(m_can_snap);

  // XXX wait for a started checkpoint to become durable.  or, do nothing.
  return 0;
}

int F2fsFileStoreBackend::rollback_to(const string& name)
{
  dout(10) << "rollback_to: to '" << name << "'" << dendl;
  assert(m_can_snap);

  // XXX roll back to a previous checkpoint

  return 0;
}

int F2fsFileStoreBackend::destroy_checkpoint(const string& name)
{
  dout(10) << "destroy_checkpoint: '" << name << "'" << dendl;
  assert(m_can_snap);

  // ...

  return 0;
}

#endif
