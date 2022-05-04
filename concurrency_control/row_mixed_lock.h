#ifndef ROW_MIXED_LOCK
#define ROW_MIXED_LOCK

#if CC_ALG == MIXED_LOCK

#include "row_lock.h"

class Row_mixed_lock {
public:
    ts_t _tid;

    RC lock_get(lock_t type, TxnManager * txn);
    RC lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(TxnManager * txn);
    void init(row_t * row);
    bool validate(ts_t tid, bool in_write_set);
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
    LockEntry* get_entry();
};

#endif

#endif
