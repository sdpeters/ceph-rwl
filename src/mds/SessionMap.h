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

#ifndef CEPH_MDS_SESSIONMAP_H
#define CEPH_MDS_SESSIONMAP_H

#include <set>
using std::set;

#include "include/unordered_map.h"

#include "include/Context.h"
#include "include/xlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "mdstypes.h"
#include "mds/MDSAuthCaps.h"

class CInode;
struct MDRequestImpl;

#include "CInode.h"
#include "Capability.h"
#include "msg/Message.h"
#include "Session.h"

/*
 * session map
 */

class MDS;

class SessionMap : public SessionMapStore {
public:
  MDS *mds;

public:  // i am lazy
  version_t projected, committing, committed;
  map<int,xlist<Session*>* > by_state;
  uint64_t set_state(Session *session, int state);
  map<version_t, list<MDSInternalContextBase*> > commit_waiters;

  SessionMap(MDS *m) : mds(m),
		       projected(0), committing(0), committed(0) 
  { }

  // sessions
  void decode(bufferlist::iterator& blp);
  bool empty() { return session_map.empty(); }
  const ceph::unordered_map<entity_name_t, Session*> &get_sessions() const
  {
    return session_map;
  }

  bool is_any_state(int state) {
    map<int,xlist<Session*>* >::iterator p = by_state.find(state);
    if (p == by_state.end() || p->second->empty())
      return false;
    return true;
  }

  bool have_unclosed_sessions() {
    return
      is_any_state(Session::STATE_OPENING) ||
      is_any_state(Session::STATE_OPENING) ||
      is_any_state(Session::STATE_OPEN) ||
      is_any_state(Session::STATE_CLOSING) ||
      is_any_state(Session::STATE_STALE) ||
      is_any_state(Session::STATE_KILLING);
  }
  bool have_session(entity_name_t w) {
    return session_map.count(w);
  }
  Session* get_session(entity_name_t w) {
    if (session_map.count(w))
      return session_map[w];
    return 0;
  }
  const Session* get_session(entity_name_t w) const {
    ceph::unordered_map<entity_name_t, Session*>::const_iterator p = session_map.find(w);
    if (p == session_map.end()) {
      return NULL;
    } else {
      return p->second;
    }
  }

  void add_session(Session *s);
  void remove_session(Session *s);
  void touch_session(Session *session);

  Session *get_oldest_session(int state) {
    if (by_state.count(state) == 0 || by_state[state]->empty())
      return 0;
    return by_state[state]->front();
  }

  void dump();

  void get_client_set(set<client_t>& s) {
    for (ceph::unordered_map<entity_name_t,Session*>::iterator p = session_map.begin();
	 p != session_map.end();
	 ++p)
      if (p->second->info.inst.name.is_client())
	s.insert(p->second->info.inst.name.num());
  }
  void get_client_session_set(set<Session*>& s) const {
    for (ceph::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
	 p != session_map.end();
	 ++p)
      if (p->second->info.inst.name.is_client())
	s.insert(p->second);
  }

  void open_sessions(map<client_t,entity_inst_t>& client_map) {
    for (map<client_t,entity_inst_t>::iterator p = client_map.begin(); 
	 p != client_map.end(); 
	 ++p) {
      Session *s = get_or_add_session(p->second);
      set_state(s, Session::STATE_OPEN);
    }
    version++;
  }

  // helpers
  entity_inst_t& get_inst(entity_name_t w) {
    assert(session_map.count(w));
    return session_map[w]->info.inst;
  }
  version_t inc_push_seq(client_t client) {
    return get_session(entity_name_t::CLIENT(client.v))->inc_push_seq();
  }
  version_t get_push_seq(client_t client) {
    return get_session(entity_name_t::CLIENT(client.v))->get_push_seq();
  }
  bool have_completed_request(metareqid_t rid) {
    Session *session = get_session(rid.name);
    return session && session->have_completed_request(rid.tid, NULL);
  }
  void trim_completed_requests(entity_name_t c, ceph_tid_t tid) {
    Session *session = get_session(c);
    assert(session);
    session->trim_completed_requests(tid);
  }

  void wipe();
  void wipe_ino_prealloc();

  // -- loading, saving --
  inodeno_t ino;
  list<MDSInternalContextBase*> waiting_for_load;

  object_t get_object_name();

  void load(MDSInternalContextBase *onload);
  void _load_finish(int r, bufferlist &bl);
  void save(MDSInternalContextBase *onsave, version_t needv=0);
  void _save_finish(version_t v);
 
};


#endif
