#include "txn.h"
#include "row.h"
#include "row_mixed_lock.h"
#include "mem_alloc.h"

#if CC_ALG==MIXED_LOCK

void Row_mixed_lock::init(row_t * row) {
    _row = row;
    _latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init( _latch, NULL );
    lock_type = LOCK_NONE;
    owner_cnt = 0;
    waiter_cnt = 0;
    latest_owner = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;

    _tid = 0;
}

#endif