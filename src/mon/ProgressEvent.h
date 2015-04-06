
#include <string>

#include "osd/OSDMap.h"
#include "mon/PGMap.h"
#include "include/memory.h"


class ProgressEvent
{
    public:
    typedef uint32_t EventCode;
    static const EventCode OSD_OUT = 1;

    ProgressEvent(
            const std::string &desc,
            uint32_t target_osd, // TODO generalise to crush node
            const EventCode ec
            );

    void init(const OSDMap &osd_map, const PGMap &pg_map);
    void tick(const OSDMap &osd_map, const PGMap &pg_map);
    float progress(const OSDMap &osd_map, const PGMap &pg_map);

    void tick_osd_out(const OSDMap &osd_map, const PGMap &pg_map);

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
    uint32_t target;
    std::vector<pg_t> pgs_working;
    std::vector<pg_t> pgs_done;
};

typedef ceph::shared_ptr<ProgressEvent> ProgressEventRef;
