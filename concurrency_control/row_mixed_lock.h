#ifndef ROW_MIXED_LOCK
#define ROW_MIXED_LOCK

#if CC_ALG == MIXED_LOCK

struct LockEntry {
    lock_t type;
    ts_t start_ts;
    TxnManager * txn;
    LockEntry * next;
    LockEntry * prev;
}

class Row_mixed_lock {
public:
    RC lock_get(lock_t type, TxnManager * txn);
    RC lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(TxnManager * txn);
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

    ts_t _tid;
};

#endif

#endif