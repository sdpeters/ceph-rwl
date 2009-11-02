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

#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonMap.h"
#include "messages/MClientMount.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"

#include "include/nstring.h"

#include "messages/MClientMountAck.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "common/ConfUtils.h"

#include "MonClient.h"
#include "MonMap.h"

#include "auth/Auth.h"
#include "auth/KeyRing.h"

#include "include/str_list.h"

#include "config.h"


#define DOUT_SUBSYS monc
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "monclient" << (hunting ? "(hunting)":"") << ": "


/*
 * build an initial monmap with any known monitor
 * addresses.
 */
int MonClient::build_initial_monmap()
{
  dout(10) << "build_initial_monmap" << dendl;

  // file?
  if (g_conf.monmap) {
    const char *monmap_fn = g_conf.monmap;
    int r = monmap.read(monmap_fn);
    if (r >= 0)
      return 0;
    char buf[80];
    cerr << "unable to read monmap from " << monmap_fn << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
  }

  // -m foo?
  if (g_conf.mon_host) {
    vector<entity_addr_t> addrs;
    if (parse_ip_port_vec(g_conf.mon_host, addrs)) {
      for (unsigned i=0; i<addrs.size(); i++) {
	entity_inst_t inst;
	inst.name = entity_name_t::MON(i);
	inst.addr = addrs[i];
	monmap.add_mon(inst);
      }
      return 0;
    }
    cerr << "unable to parse addrs in '" << g_conf.mon_host << "'" << std::endl;
  }

  // config file?
  ConfFile a(g_conf.conf);
  ConfFile b("ceph.conf");
  ConfFile *c = 0;
  
  if (a.parse())
    c = &a;
  else if (b.parse())
    c = &b;
  if (c) {
    static string monstr;
    for (int i=0; i<15; i++) {
      char monname[20];
      char *val = 0;
      sprintf(monname, "mon%d", i);
      c->read(monname, "mon addr", &val, 0);
      if (!val || !val[0])
	break;
      
      entity_inst_t inst;
      if (!parse_ip_port(val, inst.addr)) {
	cerr << "unable to parse conf's addr for " << monname << " (" << val << ")" << std::endl;
	continue;
      }
      inst.name = entity_name_t::MON(monmap.mon_inst.size());
      monmap.add_mon(inst);
    }
    if (monmap.size())
      return 0;
    cerr << "unable to find any monitors in conf" << std::endl;
    return -EINVAL;
  }

  cerr << "please specify monitors via -m monaddr or -c ceph.conf" << std::endl;
  return -ENOENT;
}


int MonClient::get_monmap()
{
  dout(10) << "get_monmap" << dendl;
  Mutex::Locker l(monc_lock);
  
  _sub_want("monmap", monmap.get_epoch());
  want_monmap = true;
  if (cur_mon < 0)
    _reopen_session();

  while (want_monmap)
    map_cond.Wait(monc_lock);

  dout(10) << "get_monmap done" << dendl;
  return 0;
}

int MonClient::get_monmap_privately()
{
  dout(10) << "get_monmap_privately" << dendl;
  Mutex::Locker l(monc_lock);
  
  SimpleMessenger *rank = NULL; 
  bool temp_msgr = false;
  if (!messenger) {
    rank = new SimpleMessenger;
    messenger = rank->register_entity(entity_name_t::CLIENT(-1));
    messenger->add_dispatcher_head(this);
    rank->start(true);  // do not daemonize!
    temp_msgr = true; 
  }
  
  int attempt = 10;
  int i = 0;
  srand(getpid());
  
  dout(10) << "have " << monmap.epoch << dendl;
  
  while (monmap.epoch == 0) {
    i = rand() % monmap.mon_inst.size();
    dout(10) << "querying " << monmap.mon_inst[i] << dendl;
    messenger->send_message(new MMonGetMap, monmap.mon_inst[i]);
    
    if (--attempt == 0)
      break;
    
    utime_t interval(1, 0);
    map_cond.WaitInterval(monc_lock, interval);
  }
  
  if (temp_msgr) {
    messenger->shutdown();
    rank->wait();
    messenger->destroy();
    messenger = 0;
  }
 
  hunting = true;  // reset this to true!

  if (monmap.epoch)
    return 0;
  return -1;
}


bool MonClient::ms_dispatch(Message *m)
{
  if (my_addr == entity_addr_t())
    my_addr = messenger->get_myaddr();

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap((MMonMap*)m);
    return true;

  case CEPH_MSG_CLIENT_MOUNT_ACK:
    handle_mount_ack((MClientMountAck*)m);
    return true;

  case CEPH_MSG_AUTH_REPLY:
    handle_auth((MAuthReply*)m);
    return true;

  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    handle_subscribe_ack((MMonSubscribeAck*)m);
    return true;
  }


  return false;
}

void MonClient::handle_monmap(MMonMap *m)
{
  dout(10) << "handle_monmap " << *m << dendl;
  monc_lock.Lock();

  _finish_hunting();

  bufferlist::iterator p = m->monmapbl.begin();
  ::decode(monmap, p);

  _sub_got("monmap", monmap.get_epoch());

  map_cond.Signal();
  want_monmap = false;

  monc_lock.Unlock();
  delete m;
}

// ----------------------
// mount

void MonClient::_send_mount()
{
  dout(10) << "_send_mount" << dendl;
  _send_mon_message(new MClientMount);
  mount_started = g_clock.now();
}

void MonClient::init()
{
  dout(10) << "init" << dendl;
  messenger->add_dispatcher_head(this);

  entity_name = *g_conf.entity_name;
  
  Mutex::Locker l(monc_lock);
  timer.add_event_after(10.0, new C_Tick(this));


  auth_supported.clear();
  string str = g_conf.supported_auth;
  list<string> sup_list;
  get_str_list(str, sup_list);
  for (list<string>::iterator iter = sup_list.begin(); iter != sup_list.end(); ++iter) {
    if (iter->compare("cephx") == 0) {
      dout(10) << "supporting cephx auth protocol" << dendl;
      auth_supported.insert(CEPH_AUTH_CEPHX);
    } else if (iter->compare("none") == 0) {
      auth_supported.insert(CEPH_AUTH_NONE);
      dout(10) << "supporting *none* auth protocol" << dendl;
    } else {
      dout(0) << "WARNING: unknown auth protocol defined: " << *iter << dendl;
    }
  }
}

void MonClient::shutdown()
{
  timer.cancel_all_events();
}

int MonClient::mount(double mount_timeout)
{
  Mutex::Locker lock(monc_lock);

  if (clientid >= 0) {
    dout(5) << "already mounted" << dendl;;
    return 0;
  }

  // only first mounter does the work
  _sub_want("monmap", monmap.get_epoch());
  mounting++;
  if (mounting == 1) {
    if (cur_mon < 0)
      _reopen_session();
    else
      _send_mount();
  }
  while (clientid < 0 && !mount_err)
    mount_cond.Wait(monc_lock);
  mounting--;

  if (clientid >= 0)
    dout(5) << "mount success, client" << clientid << dendl;

  return mount_err;
}

void MonClient::handle_mount_ack(MClientMountAck* m)
{
  dout(10) << "handle_mount_ack " << *m << dendl;

  _finish_hunting();

  if (m->result) {
    mount_err = m->result;
    dout(0) << "mount error " << m->result << " (" << m->result_msg << ")" << dendl;
  } else {
    // monmap
    bufferlist::iterator p = m->monmap_bl.begin();
    ::decode(monmap, p);

    clientid = m->client;
    messenger->set_myname(entity_name_t::CLIENT(m->client.v));
  }

  mount_cond.SignalAll();
  delete m;
}

void MonClient::handle_auth(MAuthReply *m)
{
  if (state == MC_STATE_NEGOTIATING) {
    if (!auth || (int)m->protocol != auth->get_protocol()) {
      delete auth;
      auth = get_auth_client_handler(m->protocol);
      if (!auth) {
	delete m;
	return;
      }
      auth->set_want_keys(want_keys);
      auth->init(entity_name);
    } else {
      auth->reset();
    }
    state = MC_STATE_AUTHENTICATING;
  }
  assert(auth);

  bufferlist::iterator p = m->result_bl.begin();
  int ret = auth->handle_response(m->result, p);
  delete m;

  if (ret == -EAGAIN) {
    MAuth *m = new MAuth;
    m->protocol = auth->get_protocol();
    ret = auth->build_request(m->auth_payload);
    _send_mon_message(m, true);
    return;
  }

  if (ret == 0) {
    if (state != MC_STATE_HAVE_SESSION) {
      state = MC_STATE_HAVE_SESSION;
      while (!waiting_for_session.empty()) {
	_send_mon_message(waiting_for_session.front());
	waiting_for_session.pop_front();
      }
      authenticate_cond.SignalAll();
    }
  
    _check_auth_rotating();

    auth_cond.SignalAll();
  }

  /*
    switch (state) {
    case MC_STATE_AUTHENTICATING:
      {
        dout(0) << "done authenticating" << dendl;
        state = MC_STATE_AUTHENTICATED;
        auth.send_session_request(this, &authorize_handler);
      }
      break;
    case MC_STATE_AUTHENTICATED:
      {
        dout(0) << "done authorizing" << dendl;
        state = MC_STATE_HAVE_SESSION;
	while (!waiting_for_session.empty()) {
	  _send_mon_message(waiting_for_session.front());
	  waiting_for_session.pop_front();
	}
        authenticate_cond.SignalAll();
      }
      break;
    default:
      assert(false);
    }
  }
  */
}


// ---------

void MonClient::_send_mon_message(Message *m, bool force)
{
  assert(cur_mon >= 0);
  if (force || state == MC_STATE_HAVE_SESSION) {
    messenger->send_message(m, monmap.mon_inst[cur_mon]);
  } else {
    waiting_for_session.push_back(m);
  }
}

void MonClient::_pick_new_mon()
{
  if (cur_mon >= 0)
    messenger->mark_down(monmap.get_inst(cur_mon).addr);
  cur_mon = rand() % monmap.size();
  dout(10) << "_pick_new_mon picked mon" << cur_mon << dendl;
}

void MonClient::_reopen_session()
{
  dout(10) << "_reopen_session" << dendl;

  _pick_new_mon();

  // throw out old queued messages
  while (!waiting_for_session.empty()) {
    delete waiting_for_session.front();
    waiting_for_session.pop_front();
  }

  // restart authentication process?
  if (state != MC_STATE_HAVE_SESSION) {
    state = MC_STATE_NEGOTIATING;

    MAuth *m = new MAuth;
    m->protocol = 0;
    ::encode(auth_supported, m->auth_payload);
    ::encode(entity_name, m->auth_payload);
    _send_mon_message(m, true);
  }

  if (mounting)
    _send_mount();
  if (!sub_have.empty())
    _renew_subs();
  if (!mounting && sub_have.empty())
    _send_mon_message(new MMonGetMap);
}


bool MonClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker lock(monc_lock);

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    if (con->get_peer_addr() != monmap.get_inst(cur_mon).addr) {
      dout(10) << "ms_handle_reset stray mon " << con->get_peer_addr() << dendl;
      return true;
    } else {
      dout(10) << "ms_handle_reset current mon " << con->get_peer_addr() << dendl;
      if (hunting)
	return true;
      
      dout(0) << "hunting for new mon" << dendl;
      hunting = true;
      _reopen_session();
    }
  }
  return false;
}

void MonClient::_finish_hunting()
{
  if (hunting) {
    dout(0) << "found mon" << cur_mon << dendl; 
    hunting = false;
  }
}

void MonClient::tick()
{
  dout(10) << "tick" << dendl;

  _check_auth_rotating();
  
  if (hunting) {
    dout(0) << "continuing hunt" << dendl;
    _reopen_session();

  } else {
    // just renew as needed
    utime_t now = g_clock.now();
    if (now > sub_renew_after)
      _renew_subs();

    messenger->send_keepalive(monmap.mon_inst[cur_mon]);
  }

  if (auth)
    auth->tick();

  timer.add_event_after(10.0, new C_Tick(this));
}


// ---------

void MonClient::_renew_subs()
{
  if (sub_have.empty()) {
    dout(10) << "renew_subs - empty" << dendl;
    return;
  }

  dout(10) << "renew_subs" << dendl;
  if (cur_mon < 0)
    _reopen_session();
  else {
    if (sub_renew_sent == utime_t())
      sub_renew_sent = g_clock.now();

    MMonSubscribe *m = new MMonSubscribe;
    m->what = sub_have;
    _send_mon_message(m);
  }
}

void MonClient::handle_subscribe_ack(MMonSubscribeAck *m)
{
  _finish_hunting();

  if (sub_renew_sent != utime_t()) {
    sub_renew_after = sub_renew_sent;
    sub_renew_after += m->interval / 2.0;
    dout(10) << "handle_subscribe_ack sent " << sub_renew_sent << " renew after " << sub_renew_after << dendl;
    sub_renew_sent = utime_t();
  } else {
    dout(10) << "handle_subscribe_ack sent " << sub_renew_sent << ", ignoring" << dendl;
  }

  delete m;
}

int MonClient::wait_authenticate(double timeout)
{
  Mutex::Locker l(monc_lock);
  utime_t interval;
  interval += timeout;

  if (state == MC_STATE_HAVE_SESSION)
    return 0;

  if (cur_mon < 0)
    _reopen_session();

  int ret = authenticate_cond.WaitInterval(monc_lock, interval);
  dout(0) << "wait_authenticate ended, returned " << ret << dendl;
  return ret;
}

int MonClient::_check_auth_rotating()
{
  if (state == MC_STATE_HAVE_SESSION && auth && auth->need_tickets()) {
    dout(10) << "need new tickets!" << dendl;
    MAuth *m = new MAuth;
    m->protocol = auth->get_protocol();
    auth->build_request(m->auth_payload);
    _send_mon_message(m);
  }

  if (!g_keyring.need_rotating_secrets())
    return 0;

  if (!auth_principal_needs_rotating_keys(entity_name)) {
    dout(20) << "_check_auth_rotating not needed by " << entity_name << dendl;
    return 0;
  }

  if (auth) {
    MAuth *m = new MAuth;
    m->protocol = auth->get_protocol();
    if (auth->build_rotating_request(m->auth_payload)) {
      _send_mon_message(m);
    } else {
      delete m;
    }
  }
  return 0;
}

int MonClient::wait_auth_rotating(double timeout)
{
  Mutex::Locker l(monc_lock);
  utime_t interval;
  interval += timeout;

  if (auth->get_protocol() == CEPH_AUTH_NONE) {
    return 0;
  }
  
  while (auth_principal_needs_rotating_keys(entity_name) &&
	 g_keyring.need_rotating_secrets())
    auth_cond.WaitInterval(monc_lock, interval);
  return 0;
}


