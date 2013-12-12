/*
 * SimpleDispatcher.cpp
 *
 *  Created on: Nov 21, 2013
 *      Author: matt
 */

#include "simple_dispatcher.h"
#include "messages/MPing.h"

SimpleDispatcher::SimpleDispatcher(Messenger *msgr, Mutex *l, Cond *c) :
	Dispatcher(msgr->cct),
	active(false),
	messenger(msgr),
        lock(l),
        cond(c) {

}

SimpleDispatcher::~SimpleDispatcher() {
	// nothing
}

bool SimpleDispatcher::ms_dispatch(Message *m)
{
	ConnectionRef conn;

	switch (m->get_type()) {
	case CEPH_MSG_PING:
          if (cond) {
            lock->Lock();
            cond->Signal();
            lock->Unlock();
          }

          if (!active) {
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

