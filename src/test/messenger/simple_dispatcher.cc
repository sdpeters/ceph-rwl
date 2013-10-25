/*
 * SimpleDispatcher.cpp
 *
 *  Created on: Nov 21, 2013
 *      Author: matt
 */

#include "simple_dispatcher.h"
#include "messages/MPing.h"

SimpleDispatcher::SimpleDispatcher(Messenger *msgr) :
	Dispatcher(msgr->cct),
	active(false),
	messenger(msgr) {

}

SimpleDispatcher::~SimpleDispatcher() {
	// nothing
}

bool SimpleDispatcher::ms_dispatch(Message *m)
{
	ConnectionRef conn;

	switch (m->get_type()) {
	case CEPH_MSG_PING:
		if (active) {
			cout << "pong!" << std::endl;
		} else {
			cout << "ping!" << std::endl;
			conn = m->get_connection();
			messenger->send_message(new MPing(), conn);
		}
		m->put();
		break;
	default:
		abort();
	}
	return true;
}

bool SimpleDispatcher::ms_handle_reset(Connection *con)
{
	return true;
}

void SimpleDispatcher::ms_handle_remote_reset(Connection *con)
{
	// nothing
}

