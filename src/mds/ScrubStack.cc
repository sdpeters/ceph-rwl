// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>

#include "ScrubStack.h"
#include "mds/MDS.h"
#include "mds/MDCache.h"
#include "common/Formatter.h"
#include "common/Continuation.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, scrubstack->mdcache->mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".scrubstack ";
}

void ScrubStack::push_dentry(CDentry *dentry)
{
  assert(!dentry->item_scrubqueue.is_on_list());
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  dentry_stack.push_front(&dentry->item_scrubqueue);
}

void ScrubStack::push_dentry_bottom(CDentry *dentry)
{
  assert(!dentry->item_scrubqueue.is_on_list());
  dentry->get(CDentry::PIN_SCRUBQUEUE);
  dentry_stack.push_back(&dentry->item_scrubqueue);
}

CDentry *ScrubStack::pop_dentry()
{
  CDentry *dentry = dentry_stack.front();
  dentry_stack.pop_front();
  dentry->put(CDentry::PIN_SCRUBQUEUE);
  return dentry;
}

void ScrubStack::scrub_entry()
{
  dout(0) << "scrub_entry" << dendl;
  class ScrubContinuation : public Continuation {
  public:
    ScrubStack *scrubstack;
    CDentry *dentry;
    string path;
    CInode::validated_data validation;

    enum {
      START = 0,
      CHECK_VALIDATION,
      LOOKUP_NEXT_DENTRY
    };

    ScrubContinuation(CDentry *dn, ScrubStack *ss) :
      Continuation(NULL),
      scrubstack(ss), dentry(dn) {
      set_callback(START, static_cast<Continuation::stagePtr>(
          &ScrubContinuation::_start));
      set_callback(CHECK_VALIDATION, static_cast<Continuation::stagePtr>(
          &ScrubContinuation::_check_validation));
      set_callback(LOOKUP_NEXT_DENTRY, static_cast<Continuation::stagePtr>(
          &ScrubContinuation::_lookup_next_dentry));
    }

    bool _start(int rval) {
      ++scrubstack->scrubs_in_progress;
      assert(dentry);
      assert(dentry->is_auth());
      dentry->make_path_string(path); // there must be a better way to do this
      CInode *in = dentry->get_projected_inode();
      utime_t now = ceph_clock_now(g_ceph_context);
      in->set_scrub_start_stamp(now);
      version_t v = dentry->scrub_info()->scrub_parent->get_version();
      in->set_scrub_start_version(v);
      scrubstack->mdcache->scrub_dentry(path, &validation,
                                        get_callback(CHECK_VALIDATION));
      return false;
    }

    bool _check_validation(int rval) {
      --scrubstack->scrubs_in_progress;
      CInode *in = dentry->get_projected_inode();
      if (validation.passed_validation) {
        in->set_completed_scrub_stamp(in->get_completed_scrub_stamp());
        in->set_completed_scrub_version(in->get_scrub_start_version());
      } else { // uh-oh?
        bool okay = true;
        if (validation.backtrace.checked &&
            !validation.backtrace.passed &&
            !validation.backtrace.ondisk_read_retval == -ENOENT) {
          okay = false;
        }

        if (validation.inode.checked &&
            !validation.inode.passed &&
            !validation.inode.ondisk_read_retval == -ENOENT) {
          okay = false;
        }

        if (validation.raw_rstats.checked &&
            !validation.raw_rstats.passed &&
            !validation.raw_rstats.ondisk_read_retval == -ENOENT) {
          okay = false;
        }

        if (!okay) {
          JSONFormatter f;
          CInode::dump_validation_results(validation, &f);
          stringstream ss;
          f.flush(ss);
          dout(0) << ss << dendl;
          assert(0 == "failed scrub check");
        }
      }
      return immediate(LOOKUP_NEXT_DENTRY, rval);
    }

    bool _lookup_next_dentry(int rval) {
      CDentry *next = scrubstack->dentry_stack.front();

      if (next->get_projected_inode()->is_dir() &&
          next->get_projected_inode() ==
          dentry->scrub_info()->scrub_parent->get_inode()) {
        // we should scrub the next dentry in this dir
        ;
      }
      return true;
    }
  };

  ScrubContinuation *sc = new ScrubContinuation(pop_dentry(), this);
  sc->begin();
}
