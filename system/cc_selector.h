#ifndef _CC_SELECTOR_H_
#define _CC_SELECTOR_H_

#include "global.h"
#include "message.h"
#include "helper.h"

class CCSelector {
public:
    ~CCSelector();
    void init();
    int get_best_cc(Workload *wl,Message *msg);//for a txn, pick optimal concurrency control
    void update_conflict_stats(uint64_t shard,uint64_t value=1);//add conflict number, add 1 at a time by default
    void update_ccselector();
    Message* pack_msg();
    void process_conflict_msg(ConflictStaticsMessage *msg);
    uint64_t get_total_conflict();
    uint64_t get_highest_conflict();
private:
    uint64_t *pstats;   //冲突统计信息指针
    bool *is_high_conflict; //是否为高冲突分区
#if WORKLOAD == TPCC
    enum KeyRange{
        WAREHOUSE_START = 1,
        WAREHOUSE_END   = 1,
        DISTRICT_START  = 11,
        DISTRICT_END    = 20,
        CUST_BY_ID_START    = 33001,
        CUST_BY_ID_END      = 63000,
        CUST_BY_NAME_START  = 1569803,
        CUST_BY_NAME_END    = 477324308,
        STOCK_START         = 100001,
        STOCK_END           = 200000,
        ITEM_START          = 200001,
        ITEM_END            = 300000
    };
#endif
};

#endif
