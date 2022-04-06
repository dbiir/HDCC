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
    void init(row_t * row);
private:
    row_t * _row;
    pthread_mutex_t * _latch;
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    LockEntry * latest_owner;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;

    ts_t _tid;
};

#endif

#endif