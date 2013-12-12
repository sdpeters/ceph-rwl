// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "msg/msg_types.h"
#include "msg/Messenger.h"
#include "messages/MPing.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "perfglue/heap_profiler.h"
#include "address_helper.h"
#include "simple_dispatcher.h"

#define dout_subsys ceph_subsys_simple_client

int main(int argc, const char **argv)
{
	vector<const char*> args;
	Messenger* messenger;
	SimpleDispatcher *dispatcher;
	entity_addr_t dest_addr;	
	ConnectionRef conn;
	int r = 0;

	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	global_init(NULL, args, CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_UTILITY,
		    0);

	messenger = Messenger::create(g_ceph_context,
				      entity_name_t::GENERIC(),
				      "client",
				      getpid());

	messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));

	entity_addr_from_url(&dest_addr, "tcp://localhost:1234");
	entity_inst_t dest_server(entity_name_t::GENERIC(), dest_addr);

        Mutex lock("simple_client");
        Cond cond;
	dispatcher = new SimpleDispatcher(messenger, &lock, &cond);
	messenger->add_dispatcher_head(dispatcher);

	dispatcher->set_active(); // this side is the pinger

	r = messenger->start();
	if (r < 0) {
          cerr << "ERROR: messenger->start() returned" << -r << std::endl;
		return r;
        }

	conn = messenger->get_connection(dest_server);

        utime_t start = ceph_clock_now(NULL);
	// do stuff

	for (int i = 0; i < 10000; i++) {
	  messenger->send_message(new MPing(), conn);
          Mutex::Locker l(lock);
          cond.Wait(lock);
	}

        utime_t end = ceph_clock_now(NULL);

        cout << end - start << std::endl;

        return 0;
}
