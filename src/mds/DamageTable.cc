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

#include "common/debug.h"

#include "DamageTable.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << *rank << ".damage " << __func__ << " "


bool DamageTable::notify_dentry(
    inodeno_t ino, frag_t frag,
    snapid_t snap_id, const std::string &dname)
{
  if (oversized()) {
    return true;
  }

  return false;
}

bool DamageTable::notify_dirfrag(inodeno_t ino, frag_t frag)
{
  // Special cases: damage to these dirfrags is considered fatal to
  // the MDS rank that owns them.
  if (
      (MDS_INO_IS_MDSDIR(ino) && MDS_INO_MDSDIR_OWNER(ino) == *rank)
      ||
      (MDS_INO_IS_STRAY(ino) && MDS_INO_STRAY_OWNER(ino) == *rank)
     ) {
    derr << "Damage to fragment " << frag << " of ino " << ino
         << "is fatal because it is a system directory for this rank" << dendl;
    return true;
  }

  if (oversized()) {
    return true;
  }

  return false;
}

bool DamageTable::oversized() const
{
  return by_id.size() > g_conf->mds_damage_table_max_entries;
}

