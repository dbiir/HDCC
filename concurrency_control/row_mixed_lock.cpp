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
  owners = NULL;
  own_starttime = 0;

    _tid = 0;
}

// lock_get
RC Row_mixed_lock::lock_get(lock_t type, TxnManager *txn) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt);
}

void Row_mixed_lock::return_entry(LockEntry *entry) {
  // DEBUG_M("row_lock::return_entry free %lx\n",(uint64_t)entry);
  mem_allocator.free(entry, sizeof(LockEntry));
}

bool Row_mixed_lock::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)  //有任意写锁均不相容
    return true;
  else
    return false;
}

LockEntry *Row_mixed_lock::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
  entry->type = LOCK_NONE;
  entry->txn = NULL;
  // DEBUG_M("row_lock::get_entry alloc %lx\n",(uint64_t)entry);
  return entry;
}

// Calvin和Silo共用锁机制
RC Row_mixed_lock::lock_get(lock_t type, TxnManager *txn, uint64_t *&txnids, int &txncnt) {
  RC rc;
  uint64_t starttime = get_sys_clock();
  uint64_t lock_get_start_time = starttime;  

  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(latch);
  INC_STATS(txn->get_thd_id(), mtx[17], get_sys_clock() - mtx_wait_starttime);  

  INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);

  // 冲突判断
  bool conflict = conflict_lock(lock_type, type);
  if (txn->algo == CALVIN && !conflict) {  
    if (waiters_head) conflict = true;   
  }

  // 冲突处理
  if (conflict) {
    
    LockEntry *entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    entry->type = type;

    DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
          txn->get_txn_id(), txn->get_batch_id(), owner_cnt, lock_type, type,
          _row->get_primary_key(), (uint64_t)_row);

    if (txn->algo == CALVIN) {  // calvin
      
      LIST_PUT_TAIL(waiters_head, waiters_tail, entry); // waiters++
      waiter_cnt++;

      // ready状态从true转换为false
      ATOM_CAS(txn->lock_ready, true, false);
      txn->incr_lr();
      rc = WAIT;
    } else if (txn->algo == SILO) {  // silo
      //出现冲突就回滚
      rc = Abort;
      DEBUG("abort %ld, %ld %lx\n", txn->get_txn_id(), _row->get_primary_key(), (uint64_t)_row);
      goto final;
    }
  } else {  // no conflict
    DEBUG("1lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n", txn->get_txn_id(),
          txn->get_batch_id(), owner_cnt, lock_type, type, _row->get_primary_key(), (uint64_t)_row);

    LockEntry *entry = get_entry();
    entry->type = type;
    entry->start_ts = get_sys_clock();
    entry->txn = txn;

    // entry->next = owners
    STACK_PUSH(owners, entry);

    owner_cnt++;
    if (lock_type == LOCK_NONE) {
      own_starttime = get_sys_clock();
    }
    lock_type = type; // 类型为写锁以后是加不上锁的
    rc = RCOK;
  }

final:
  uint64_t curr_time = get_sys_clock();
  uint64_t timespan = curr_time - starttime;
  if (rc == WAIT && txn->twopl_wait_start == 0) {  // twopl_wait_start后面要用来计算cc_block_time
    txn->twopl_wait_start = curr_time;
  }
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;

  pthread_mutex_unlock(latch);

  return rc;
}

RC Row_mixed_lock::lock_release(TxnManager *txn) {
#if txn->algo == CALVIN
  if (txn->isRecon()) {
    return RCOK;
  }
#endif
  uint64_t starttime = get_sys_clock();
  uint64_t mtx_wait_starttime = get_sys_clock();

  pthread_mutex_lock(latch);
  INC_STATS(txn->get_thd_id(), mtx[18], get_sys_clock() - mtx_wait_starttime);

  // DEBUG("unlock (%ld,%ld): owners %d, own type %d, key %ld %lx\n", txn->get_txn_id(),
  // txn->get_batch_id(), owner_cnt, lock_type, _row->get_primary_key(), (uint64_t)_row);

   // Try to find the entry in the owners
  LockEntry *en = owners;
  LockEntry *prev = NULL;
  while (en != NULL && en->txn != txn) {  // en=null走到栈底
    prev = en;
    en = en->next;
  }

  if (en) {  // find the entry in the owner list! free the entry
    if (prev)
      prev->next = en->next;
    else  // en is the head of owners
      owners = en->next;  
    return_entry(en);   // free
    owner_cnt--;
    if (owner_cnt == 0) {
      uint64_t endtime = get_sys_clock();
      lock_type = LOCK_NONE;
    }

  } else {  // en = null 走到栈底 
    assert(false);
    en = waiters_head;
    while (en != NULL && en->txn != txn) en = en->next; //找到当前txn的waiter
    ASSERT(en);

    LIST_REMOVE(en);  // remove en->txn == txn entry
    if (en == waiters_head) waiters_head = en->next;
    if (en == waiters_tail) waiters_tail = en->prev;
    return_entry(en);
    waiter_cnt--;
  }

  if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);

  LockEntry *entry;

  // 不冲突的waiter加入owners
  while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
    LIST_GET_HEAD(waiters_head, waiters_tail, entry);  // 取到entry, 删除waiters_head
    uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
    entry->txn->twopl_wait_start = 0;
#if txn->algo != CALVIN
    entry->txn->txn_stats.cc_block_time += timespan;
    entry->txn->txn_stats.cc_block_time_short += timespan;
#endif

    STACK_PUSH(owners, entry);  // 放入owners前部

    owner_cnt++;
    waiter_cnt--;
    ASSERT(entry->txn->lock_ready == false);

    if (entry->txn->decr_lr() == 0) {
      if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
#if txn->algo == CALVIN
        entry->txn->txn_stats.cc_block_time += timespan;
        entry->txn->txn_stats.cc_block_time_short += timespan;
#endif
        txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),
                              entry->txn->get_batch_id());
      }
    }
    if (lock_type == LOCK_NONE) {
      own_starttime = get_sys_clock();
    }
    lock_type = entry->type;
  }

  //并发时间统计
  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;

  pthread_mutex_unlock(latch);

  return RCOK;
}

// silo的验证
bool Row_mixed_lock::validate(ts_t tid, bool in_write_set) {
  if (in_write_set)
       // 写验证
        return tid == _tid;

  //读验证
  if (owners->type = LOCK_EX) return false;  // 已加写锁

  bool valid = (tid == _tid);  // 时间戳校对，是否数据被修改过导致版本变化
  return valid;
}
#endif