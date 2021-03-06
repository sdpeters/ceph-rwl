// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/journal/Types.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "journal/Settings.h"
#include <list>
#include <boost/variant.hpp>

void register_test_journal_entries() {
}

class TestJournalEntries : public TestFixture {
public:
  typedef std::list<journal::Journaler *> Journalers;

  struct ReplayHandler : public journal::ReplayHandler {
    Mutex lock;
    Cond cond;
    bool entries_available;
    bool complete;

    ReplayHandler()
      : lock("ReplayHandler::lock"), entries_available(false), complete(false) {
    }

    void get() override {
    }
    void put() override {
    }

    void handle_entries_available() override  {
      Mutex::Locker locker(lock);
      entries_available = true;
      cond.Signal();
    }

    void handle_complete(int r) override {
      Mutex::Locker locker(lock);
      complete = true;
      cond.Signal();
    }
  };

  ReplayHandler m_replay_handler;
  Journalers m_journalers;

  void TearDown() override {
    for (Journalers::iterator it = m_journalers.begin();
         it != m_journalers.end(); ++it) {
      journal::Journaler *journaler = *it;
      journaler->stop_replay();
      journaler->shut_down();
      delete journaler;
    }

    TestFixture::TearDown();
  }

  journal::Journaler *create_journaler(librbd::ImageCtx *ictx) {
    journal::Journaler *journaler = new journal::Journaler(
      ictx->md_ctx, ictx->id, "dummy client", {});

    int r = journaler->register_client(bufferlist());
    if (r < 0) {
      ADD_FAILURE() << "failed to register journal client";
      delete journaler;
      return NULL;
    }

    C_SaferCond cond;
    journaler->init(&cond);
    r = cond.wait();
    if (r < 0) {
      ADD_FAILURE() << "failed to initialize journal client";
      delete journaler;
      return NULL;
    }

    journaler->start_live_replay(&m_replay_handler, 0.1);
    m_journalers.push_back(journaler);
    return journaler;
  }

  bool wait_for_entries_available(librbd::ImageCtx *ictx) {
    Mutex::Locker locker(m_replay_handler.lock);
    while (!m_replay_handler.entries_available) {
      if (m_replay_handler.cond.WaitInterval(m_replay_handler.lock,
					     utime_t(10, 0)) != 0) {
	return false;
      }
    }
    m_replay_handler.entries_available = false;
    return true;
  }

  bool get_event_entry(const journal::ReplayEntry &replay_entry,
                       librbd::journal::EventEntry *event_entry) {
    try {
      bufferlist data_bl = replay_entry.get_data();
      auto it = data_bl.cbegin();
      decode(*event_entry, it);
    } catch (const buffer::error &err) {
      return false;
    }
    return true;
  }

};

TEST_F(TestJournalEntries, AioWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  journal::Journaler *journaler = create_journaler(ictx);
  ASSERT_TRUE(journaler != NULL);

  std::string buffer(512, '1');
  bufferlist write_bl;
  write_bl.append(buffer);

  C_SaferCond cond_ctx;
  auto c = librbd::io::AioCompletion::create(&cond_ctx);
  c->get();
  ictx->io_work_queue->aio_write(c, 123, buffer.size(), std::move(write_bl), 0);
  ASSERT_EQ(0, c->wait_for_complete());
  c->put();

  ASSERT_TRUE(wait_for_entries_available(ictx));

  journal::ReplayEntry replay_entry;
  ASSERT_TRUE(journaler->try_pop_front(&replay_entry));

  librbd::journal::EventEntry event_entry;
  ASSERT_TRUE(get_event_entry(replay_entry, &event_entry));

  ASSERT_EQ(librbd::journal::EVENT_TYPE_AIO_WRITE,
            event_entry.get_event_type());

  librbd::journal::AioWriteEvent aio_write_event =
    boost::get<librbd::journal::AioWriteEvent>(event_entry.event);
  ASSERT_EQ(123U, aio_write_event.offset);
  ASSERT_EQ(buffer.size(), aio_write_event.length);

  bufferlist buffer_bl;
  buffer_bl.append(buffer);
  ASSERT_TRUE(aio_write_event.data.contents_equal(buffer_bl));

  ASSERT_EQ(librbd::journal::AioWriteEvent::get_fixed_size() +
              aio_write_event.data.length(), replay_entry.get_data().length());
}

TEST_F(TestJournalEntries, AioDiscard) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  CephContext* cct = reinterpret_cast<CephContext*>(_rados.cct());
  REQUIRE(!cct->_conf.get_val<bool>("rbd_skip_partial_discard"));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  journal::Journaler *journaler = create_journaler(ictx);
  ASSERT_TRUE(journaler != NULL);

  C_SaferCond cond_ctx;
  auto c = librbd::io::AioCompletion::create(&cond_ctx);
  c->get();
  ictx->io_work_queue->aio_discard(c, 123, 234,
                                   ictx->discard_granularity_bytes);
  ASSERT_EQ(0, c->wait_for_complete());
  c->put();

  ASSERT_TRUE(wait_for_entries_available(ictx));

  journal::ReplayEntry replay_entry;
  ASSERT_TRUE(journaler->try_pop_front(&replay_entry));

  librbd::journal::EventEntry event_entry;
  ASSERT_TRUE(get_event_entry(replay_entry, &event_entry));

  ASSERT_EQ(librbd::journal::EVENT_TYPE_AIO_DISCARD,
            event_entry.get_event_type());

  librbd::journal::AioDiscardEvent aio_discard_event =
    boost::get<librbd::journal::AioDiscardEvent>(event_entry.event);
  ASSERT_EQ(123U, aio_discard_event.offset);
  ASSERT_EQ(234U, aio_discard_event.length);
}

TEST_F(TestJournalEntries, AioFlush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);
  /* This can't work with IMAGE_CACHE because FLUSH_SOURCE_USER
   * terminates there. The only flushes that ever leave the image
   * cache are FLUSH_SOURCE_INTERNAL, which are not appended to the
   * journal. */
  REQUIRE(!is_feature_enabled(RBD_FEATURE_IMAGE_CACHE));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  journal::Journaler *journaler = create_journaler(ictx);
  ASSERT_TRUE(journaler != NULL);

  C_SaferCond cond_ctx;
  auto c = librbd::io::AioCompletion::create(&cond_ctx);
  c->get();
  ictx->io_work_queue->aio_flush(c);
  ASSERT_EQ(0, c->wait_for_complete());
  c->put();

  ASSERT_TRUE(wait_for_entries_available(ictx));

  journal::ReplayEntry replay_entry;
  ASSERT_TRUE(journaler->try_pop_front(&replay_entry));

  librbd::journal::EventEntry event_entry;
  ASSERT_TRUE(get_event_entry(replay_entry, &event_entry));

  ASSERT_EQ(librbd::journal::EVENT_TYPE_AIO_FLUSH,
            event_entry.get_event_type());
}
