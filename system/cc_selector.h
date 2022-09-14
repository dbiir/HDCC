#ifndef _CC_SELECTOR_H_
#define _CC_SELECTOR_H_

#include "global.h"
#include "message.h"
#include "helper.h"
#include "ycsb_query.h"
#include "ycsb.h"

class CCSelector {
public:
    ~CCSelector();
    void init();
    int get_best_cc(Workload *wl,Message *msg);//for a txn, pick optimal concurrency control
    void update_conflict_stats(uint64_t shard,uint64_t value=1);//add conflict number, add 1 at a time by default
    void update_ccselector();
    Message* pack_msg();
    void process_conflict_msg(ConflictStaticsMessage *msg);
private:
    uint64_t *pstats;
    bool *is_high_conflict;
};

#endif
