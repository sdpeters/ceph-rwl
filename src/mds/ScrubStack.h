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

#ifndef SCRUBSTACK_H_
#define SCRUBSTACK_H_

#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "include/elist.h"

class MDCache;

class ScrubStack {
  elist<CDentry*> dentry_stack;
  int scrubs_in_progress;
  ScrubStack *scrubstack; // hack for dout
public:
  MDCache *mdcache;
  ScrubStack(MDCache *mdc) :
    dentry_stack(member_offset(CDentry, item_scrubqueue)),
    scrubs_in_progress(0), scrubstack(this), mdcache(mdc) {}
  /**
   * Put a dentry on the top of the scrub stack, so it is the highest priority.
   * If there are other scrubs in progress, they will not continue scrubbing new
   * entries until this one is completed.
   * @param dn The dentry to scrub
   * @param recursive True if we want to recursively scrub the
   * entire hierarchy under dn.
   * @param children True if we want to scrub the direct children of
   * dn but aren't doing a recursive scrub. (Otherwise, all checks are
   * local to dn's disk state.)
   */
  void enqueue_dentry_top(CDentry *dn, bool recursive, bool children) {
    enqueue_dentry(dn, recursive, children, true);
  }
  /** Like enqueue_dentry_top, but we wait for all pending scrubs before
   * starting this one.
   */
  void enqueue_dentry_bottom(CDentry *dn, bool recursive, bool children) {
    enqueue_dentry(dn, recursive, children, false);
  }
  void scrub_entry();
private:
  /**
   * Put the dentry at either the top or bottom of the stack, with
   * the given scrub params.
   */
  void enqueue_dentry(CDentry *dn, bool recursive, bool children, bool top);
  /**
   * Push a dentry on top of the stack.
   */
  void push_dentry(CDentry *dentry);
  /**
   * Push a dentry to the bottom of the stack.
   */
  void push_dentry_bottom(CDentry *dentry);
  /**
   * Pop the top dentry off the stack.
   */
  CDentry *pop_dentry();
};

#endif /* SCRUBSTACK_H_ */
