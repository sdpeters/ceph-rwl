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
#include "msg/Messenger.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "perfglue/heap_profiler.h"
#include "address_helper.h"
#include "simple_dispatcher.h"

#define dout_subsys ceph_subsys_simple_server


int main(int argc, const char **argv)
{
	vector<const char*> args;
	Messenger *messenger;
	Dispatcher *dispatcher;
	entity_addr_t bind_addr;
	int r = 0;

	using std::endl;

	cout << "Simple Server starting..." << endl;

	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	entity_addr_from_url(&bind_addr, "tcp://localhost:1234");

	global_init(NULL, args, CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_DAEMON,
		    0);

	messenger = Messenger::create(g_ceph_context,
				      entity_name_t::GENERIC(),
				      "simple_server",
				      0 /* nonce */);
	messenger->set_default_policy(
	  Messenger::Policy::stateless_server(CEPH_FEATURES_ALL, 0));

	r = messenger->bind(bind_addr);
	if (r < 0)
		goto out;

	// Set up crypto, daemonize, etc.
	//global_init_daemonize(g_ceph_context, 0);
	common_init_finish(g_ceph_context);

	dispatcher = new SimpleDispatcher(messenger);

	messenger->add_dispatcher_head(dispatcher); // should reach ready()
	messenger->start();
	messenger->wait(); // can't be called until ready()

	// done
	delete messenger;

out:
	cout << "Simple Server exit" << endl;
	return r;
}

