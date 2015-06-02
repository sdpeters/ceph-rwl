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


#ifndef DAMAGE_TABLE_H_
#define DAMAGE_TABLE_H_

#include "mdstypes.h"

typedef uint64_t damage_entry_id_t;

class DamageEntry
{
  public:
  damage_entry_id_t id;
};


typedef ceph::shared_ptr<DamageEntry> DamageEntryRef;

/**
 * Record damage to a particular dirfrag, implicitly affecting
 * any dentries within it.
 */
class DirfragDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;
};

/**
 * Record damage to a particular dname within a particular dirfrag
 */
class DentryDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;
  snapid_t snap_id;
  std::string dname;
};


/**
 * Registry of in-RADOS metadata damage identified
 * during forward scrub or during normal fetches.
 *
 * Used to indicate damage to the administrator, and
 * to cache known-bad paths so that we don't hit them
 * repeatedly.
 *
 * Callers notifying damage must check return code; if
 * an fatal condition is indicated then they should mark the MDS
 * rank damaged.
 *
 * An artificial limit on the number of damage entries
 * is imposed to avoid this structure growing indefinitely.  If
 * a notification causes the limit to be exceeded, the fatal
 * condition will be indicated in the return code and the MDS
 * rank should be marked damaged.
 *
 * Protected by MDS::mds_lock
 */
class DamageTable
{
  public:

    /**
     * Indicate that a dirfrag cannot be loaded.
     *
     * @return true if fatal
     */
    bool notify_dirfrag(inodeno_t ino, frag_t frag);

    /**
     * Indicate that a particular dentry cannot be loaded.
     *
     * @return true if fatal
     */
    bool notify_dentry(
      inodeno_t ino, frag_t frag,
      snapid_t snap_id, const std::string &dname);



    /*
     * XXX
     * Queries:
     *  * Is this dentry I'm about to load damaged?
     *  * Is this dirfrag I'm about to touch damaged?
     *  * Does this dirfrag contain any damaged dentries?
     *
     *  Show me all the damage
     */

    DamageTable(const mds_rank_t *rank_)
      : rank(rank_)
    {
      assert(rank_ != NULL);
    }

  protected:

    /*
     * Maintain a multi-index on the damage entries, so that
     * we can reference them by unique ID, but also quickly
     * check for damage by inode/dentry.
     */
    std::map<damage_entry_id_t, DamageEntryRef> by_id;

    // I need to know my MDS rank so that I can check if
    // metadata items are part of my mydir.
    const mds_rank_t *rank;

    bool oversized() const;

};

#endif // DAMAGE_TABLE_H_

