// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>

#include "osd/OSDMap.h"
#include "mon/PGMap.h"
#include "include/memory.h"


class ProgressEvent
{
  public:
    typedef uint32_t EventCode;
    static const EventCode OSD_OUT = 1;
    static const EventCode OSD_IN = 2;
    static const EventCode MDS_START = 3;
    static const EventCode SCRUB = 4;

    ProgressEvent(
        const std::string &desc,
        const EventCode ec);
    virtual ~ProgressEvent() {}

    virtual void init(
        const OSDMap &osd_map,
        const OSDMap &pending_osd_map,
        const PGMap &pg_map) = 0;
    virtual void tick(const OSDMap &osd_map, const PGMap &pg_map) = 0;
    virtual float progress(const OSDMap &osd_map, const PGMap &pg_map) = 0;

    bool is_complete();

    // TODO getter list of PGs finished with
    // TODO getter list of PGs still to do
    //
    const std::string &get_description() const
    {
      return description;
    }

  protected:
    EventCode event_code;
    std::string description;
    bool finished;
};

typedef ceph::shared_ptr<ProgressEvent> ProgressEventRef;

/**
 * A progress event which works its way through a series of
 * PGs known at initialisation time, where each PG in the
 * set may be said to be done or pending.
 */
class PgProgressEvent : public ProgressEvent
{
  public:
  virtual float progress(const OSDMap &osd_map, const PGMap &pg_map);
  PgProgressEvent(
      const std::string &desc,
      const EventCode ec)
    : ProgressEvent(desc, ec)
  {}

  protected:
    std::vector<pg_t> pgs_working;
    std::vector<pg_t> pgs_done;
};

/**
 * A progressevent for an event that refers to a particular OSD.
 * TODO: generalise this to any crush subtree: we should handle
 * a host going in/out the same way we handle an individual OSD.
 */
class OsdProgressEvent : public PgProgressEvent
{
  protected:
    uint32_t target;

  public:
    OsdProgressEvent(
        const std::string &desc,
        uint32_t target_,
        EventCode code_
        )
      : PgProgressEvent(desc, code_), target(target_)
    {};
};

class ProgressOsdOut : public OsdProgressEvent
{
  public:
    ProgressOsdOut(
        const std::string &desc,
        uint32_t target_osd
        )
      : OsdProgressEvent(desc, target_osd, ProgressEvent::OSD_OUT)
    {};

    void init(
        const OSDMap &osd_map,
        const OSDMap &pending_osd_map,
        const PGMap &pg_map);
    void tick(const OSDMap &osd_map, const PGMap &pg_map);
    ~ProgressOsdOut() {}
};

class ProgressOsdIn : public OsdProgressEvent
{
  public:
    ProgressOsdIn(
        const std::string &desc,
        uint32_t target_osd
        )
      : OsdProgressEvent(desc, target_osd, ProgressEvent::OSD_IN)
    {};

    void init(
        const OSDMap &osd_map,
        const OSDMap &pending_osd_map,
        const PGMap &pg_map);
    void tick(const OSDMap &osd_map, const PGMap &pg_map);
    ~ProgressOsdIn() {}
};

/**
 * Unlike OsdProgressEvent, my scope is defined in the PG (data) space, not
 * in the CRUSH (physical) space.  I can act on any subset of PGs.
 *
 * (although the current "ceph osd scrub" interface is limited to
 * individual PGs, individual OSDs, or all OSDs.
 */
class ProgressScrub : public PgProgressEvent
{
  protected:
    // True if it's a deep scrub
    const bool deep;

    // TODO: generalize to scrubs that target a PG, a pool, a set of PGs
    std::vector<uint32_t> osd_ids;

    // Anything scrubbed after this time is considered complete
    utime_t start_time;

  public:
    ProgressScrub(
        const std::string &desc_,
        const bool deep_,
        std::vector<uint32_t> osd_ids_)
      : PgProgressEvent(desc_, ProgressEvent::SCRUB),
        deep(deep_), osd_ids(osd_ids_)
    {
    }

  void init(
      const OSDMap &osd_map,
      const OSDMap &pending_osd_map,
      const PGMap &pg_map);
  void tick(const OSDMap &osd_map, const PGMap &pg_map);
};

