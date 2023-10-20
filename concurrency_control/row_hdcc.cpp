#include "txn.h"
#include "row.h"
#include "row_hdcc.h"
#include "mem_alloc.h"
#include "cc_selector.h"
#include "tpcc_query.h"

#if CC_ALG==HDCC

void Row_hdcc::init(row_t * row) {
    _row = row;
    _latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init( _latch, NULL );
    lock_type = LOCK_NONE;
    owner_cnt = 0;
    waiter_cnt = 0;
    waiters_head = NULL;
    waiters_tail = NULL;
    owners = NULL;
    own_starttime = 0;

    _tid = 0;
    isIntermediateState = false;
    max_calvin_read_tid = 0;
    max_calvin_read_bid = 0;
    max_calvin_write_tid = 0;
    max_calvin_write_bid = 0;
}

// lock_get
RC Row_hdcc::lock_get(lock_t type, TxnManager *txn) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt);
}

void Row_hdcc::return_entry(LockEntry *entry) {
  // DEBUG_M("row_lock::return_entry free %lx\n",(uint64_t)entry);
  mem_allocator.free(entry, sizeof(LockEntry));
}

bool Row_hdcc::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)  // exclusive lock
    return true;
  else
    return false;
}
#if EXTREME_MODE
bool Row_hdcc::conflict_lock_extreme_mode(lock_t l1, lock_t l2, TxnManager *txn) {
  if(txn->algo == CALVIN){
    return conflict_lock(l1, l2);
  }
  // Calvin read，OCC write || Calvin's write is not finished，OCC write --> conflict
  // l2 == LOCK_EX is assured at this point since OCC invoke lock get only for its write set
  // if l1 corresponds to an OCC txn, 
  bool writeNotStart = false;
  if(owners){
    if(owners->txn->algo == SILO){
      return true;
    }
    writeNotStart = _tid != owners->txn->get_txn_id();
  }
  if (l1 == LOCK_SH || (l1 == LOCK_EX && (isIntermediateState || writeNotStart)))
    return true;
  else
    return false;
}
#endif

LockEntry *Row_hdcc::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
  entry->type = LOCK_NONE;
  entry->txn = NULL;
  // DEBUG_M("row_lock::get_entry alloc %lx\n",(uint64_t)entry);
  return entry;
}

// lock sharing mechanism for calvin and silo
RC Row_hdcc::lock_get(lock_t type, TxnManager *txn, uint64_t *&txnids, int &txncnt) {
  RC rc;
  uint64_t starttime = get_sys_clock();
  uint64_t lock_get_start_time = starttime;  

  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(_latch);
  INC_STATS(txn->get_thd_id(), mtx[17], get_sys_clock() - mtx_wait_starttime);  

  INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);

  // conflict check
#if EXTREME_MODE
  bool conflict = conflict_lock_extreme_mode(lock_type, type, txn);
#else
  bool conflict = conflict_lock(lock_type, type);
#endif
  if (txn->algo == CALVIN && !conflict) {  
    if (waiters_head) conflict = true;   
  }

  // conflict handle
  if (conflict) {
    DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
          txn->get_txn_id(), txn->get_batch_id(), owner_cnt, lock_type, type,
          _row->get_primary_key(), (uint64_t)_row);

    if (txn->algo == CALVIN) {  // calvin
      LockEntry *entry = get_entry();
      entry->start_ts = get_sys_clock();
      entry->txn = txn;
      entry->type = type;

      LIST_PUT_TAIL(waiters_head, waiters_tail, entry); // waiters++
      waiter_cnt++;

      // set ready from true to false
      ATOM_CAS(txn->lock_ready, true, false);
      txn->incr_lr();
      rc = WAIT;

      // Calvin determines conflicts on all rows accessed, so we only process each data that actually conflicts
	  #if WORKLOAD == TPCC
      cc_selector.update_conflict_stats(((TPCCQuery*)(txn->query)), _row);
    #else
      cc_selector.update_conflict_stats(_row);
    #endif
    } else if (txn->algo == SILO) {  // silo
      // for silo, if conflict happens, rollback immediately
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
    lock_type = type; // if type == LOCK_EX, it is exclusively owned
    rc = RCOK;
  }

final:
  uint64_t curr_time = get_sys_clock();
  uint64_t timespan = curr_time - starttime;
  if (rc == WAIT && txn->twopl_wait_start == 0) {  // use twopl_wait_start to compute cc_block_time
    txn->twopl_wait_start = curr_time;
  }
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;

  pthread_mutex_unlock(_latch);

  return rc;
}

RC Row_hdcc::lock_release(TxnManager *txn) {
  if (txn->algo == CALVIN && txn->isRecon()) {
    return RCOK;
  }

  uint64_t starttime = get_sys_clock();
  uint64_t mtx_wait_starttime = get_sys_clock();

  pthread_mutex_lock(_latch);
  INC_STATS(txn->get_thd_id(), mtx[18], get_sys_clock() - mtx_wait_starttime);

  // DEBUG("unlock (%ld,%ld): owners %d, own type %d, key %ld %lx\n", txn->get_txn_id(),
  // txn->get_batch_id(), owner_cnt, lock_type, _row->get_primary_key(), (uint64_t)_row);

   // Try to find the entry in the owners
  LockEntry *en = owners;
  LockEntry *prev = NULL;
  while (en != NULL && en->txn != txn) {
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
      lock_type = LOCK_NONE;
    }

  } else {  // en = null 
    assert(false);
    en = waiters_head;
    while (en != NULL && en->txn != txn) en = en->next;
    ASSERT(en);

    LIST_REMOVE(en);  // remove en->txn == txn entry
    if (en == waiters_head) waiters_head = en->next;
    if (en == waiters_tail) waiters_tail = en->prev;
    return_entry(en);
    waiter_cnt--;
  }

  if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);

  LockEntry *entry;

  // add waiters with non-conflict lock request to owners
  while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
    LIST_GET_HEAD(waiters_head, waiters_tail, entry);  // get entry, remove waiters_head
    uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
    entry->txn->twopl_wait_start = 0;
    if(txn->algo != CALVIN) {
      entry->txn->txn_stats.cc_block_time += timespan;
      entry->txn->txn_stats.cc_block_time_short += timespan;
    }

    STACK_PUSH(owners, entry);  // push into owners

    owner_cnt++;
    waiter_cnt--;
    ASSERT(entry->txn->lock_ready == false);

    if (entry->txn->decr_lr() == 0) {
      if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
        if(txn->algo == CALVIN) {
          entry->txn->txn_stats.cc_block_time += timespan;
          entry->txn->txn_stats.cc_block_time_short += timespan;  
        }

        txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),
                              entry->txn->get_batch_id());
      }
    }
    if (lock_type == LOCK_NONE) {
      own_starttime = get_sys_clock();
    }
    lock_type = entry->type;
  }

  // concurrency time stats
  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;

  pthread_mutex_unlock(_latch);

  return RCOK;
}

// silo validation
// #if EXTREME_MODE
bool Row_hdcc::validate(Access *access, bool in_write_set, unordered_set<uint64_t> &waitFor, bool &benefited) {
  ts_t tid_at_read = access->tid;
  bool readIntermediateState = access->isIntermediateState;

  // write validation
  if (in_write_set){
    if(tid_at_read != _tid){
      return false;
    }
    pthread_mutex_lock(_latch);
    // as implemented in lock_get, owners must be this occ txn itself, 
    // but there maybe exists another Calvin txn holding an EX_lock which it depends on, add it to waitFor set
    if(owners->next){
      waitFor.insert(owners->next->txn->get_txn_id());
      benefited = true;
    }
    pthread_mutex_unlock(_latch);
    return true;
  }

  //read validation
  if(tid_at_read != _tid || readIntermediateState){ //fail to pass validation
    return false;
  }
  if(lock_type != LOCK_EX){
    return true;
  }
  pthread_mutex_lock(_latch);
  // if(lock_type != LOCK_EX){
  //   pthread_mutex_unlock(_latch);
  //   return true;
  // }
  // if OCC utilize Calvin's time window between lock and real write operation, or final write and unlock
  // additional check need to be done
  benefited = true;
  LockEntry *p = owners;
  while(p){
    if(p->txn->algo == CALVIN){
      if(p->txn->get_txn_id() == _tid){ //calvin txn has written this row, occ must wait, otherwise it can eschew
        waitFor.insert(p->txn->get_txn_id());
      }
      break;
    }
    p = p->next;
  }
  pthread_mutex_unlock(_latch);
  return true;
}
// #else
bool Row_hdcc::validate(Access *access, bool in_write_set) {
  ts_t tid_at_read = access->tid;
  bool readIntermediateState = access->isIntermediateState;
  if (in_write_set)
      // write validation
        return tid_at_read == _tid;

  // read validation
  pthread_mutex_lock(_latch);
  if (lock_type == LOCK_EX) {
    pthread_mutex_unlock(_latch);
    return false;  // current lock is LOCK_EX
  }
  pthread_mutex_unlock(_latch);

  bool valid = (tid_at_read == _tid && !readIntermediateState);  // timestamp check, whether data has been modified
  return valid;
}
// #endif
#endif
