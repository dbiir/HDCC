#ifndef ROW_HDCC
#define ROW_HDCC

#if CC_ALG == HDCC

#include "row_lock.h"
// #if EXTREME_MODE
#include <unordered_set>
// #endif

class Row_hdcc {
public:
    ts_t _tid;
    int isIntermediateState;
    uint64_t max_calvin_read_tid;
    uint64_t max_calvin_read_bid;
    uint64_t max_calvin_write_tid;
    uint64_t max_calvin_write_bid;

    RC lock_get(lock_t type, TxnManager * txn);
    RC lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(TxnManager * txn);
    void init(row_t * row);
// #if EXTREME_MODE
    bool validate(Access *access, bool in_write_set, unordered_set<uint64_t> &waitFor, bool &benefited);
// #else
    bool validate(Access *access, bool in_write_set);
// #endif
private:
    row_t * _row;
    pthread_mutex_t * _latch;
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    LockEntry * owners; 
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
    uint64_t own_starttime;

    void 		return_entry(LockEntry * entry);
    bool conflict_lock(lock_t l1, lock_t l2);
#if EXTREME_MODE
    bool conflict_lock_extreme_mode(lock_t l1, lock_t l2, TxnManager *txn);
#endif
    LockEntry* get_entry();
};

#endif

#endif
