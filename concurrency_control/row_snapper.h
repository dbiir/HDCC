#ifndef ROW_SNAPPER
#define ROW_SNAPPER

#if CC_ALG == SNAPPER

#include "row_lock.h"

class Row_snapper {
public:
    RC lock_get(lock_t type, TxnManager * txn);
    RC lock_release(TxnManager * txn);
    bool validate(TxnManager * txn);
    void init(row_t* row);
    void enter_critical_section();
    void leave_critical_section();

   private:
    row_t * _row;
    pthread_mutex_t * _latch;
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    LockEntry * owners;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
    uint64_t max_owner_ts;
    uint64_t own_starttime;
    uint64_t last_batch_id;

    void 		return_entry(LockEntry * entry);
    bool conflict_lock(lock_t l1, lock_t l2);
    LockEntry* get_entry();
};

#endif

#endif