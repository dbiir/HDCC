#include "global.h"
#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "query.h"
#include "row.h"
#include "row_hdcc.h"
#include "row_mvcc.h"
#include "row_ts.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "cc_selector.h"
#include "sequencer.h"
#include <unordered_set>

#if CC_ALG == HDCC
RC TxnManager::validate_once() {
  RC rc = RCOK;
  uint64_t wr_cnt = txn->write_cnt;
  int cur_wr_idx = 0;
  int read_set[txn->row_cnt - txn->write_cnt];
  int cur_rd_idx = 0;
  for (uint64_t rid = 0; rid < txn->row_cnt; rid++) {
    if (txn->accesses[rid]->type == WR)
      write_set[cur_wr_idx++] = rid;
    else
      read_set[cur_rd_idx++] = rid;
  }

  	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
					txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
				{
					int tmp = write_set[j];
					write_set[j] = write_set[j+1];
					write_set[j+1] = tmp;
				}
			}
		}
	}

  num_locks = 0;
  bool done = false;
  if (_pre_abort) {
    for (uint64_t i = 0; i < wr_cnt; i++) {  // pre_abort
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->_tid != txn->accesses[write_set[i]]->tid) {
        rc = Abort;
        return rc;
      }
    }
    for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i++) {
      Access* access = txn->accesses[read_set[i]];
      if (access->orig_row->manager->_tid != txn->accesses[read_set[i]]->tid) {
        rc = Abort;
        return rc;
      }
    }
  }

  while (!done) {
    num_locks = 0;
    for (uint64_t i = 0; i < wr_cnt; i++) {
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->lock_get(LOCK_EX, this) == Abort)
      {
        break;
      }
      DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
      num_locks++;
    }
    if (num_locks == wr_cnt) {
      DEBUG("TRY LOCK true %ld\n", get_txn_id());
      done = true;
    } else {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }

  COMPILER_BARRIER

#if EXTREME_MODE
  unordered_set<uint64_t> waitFor;
  bool benefited = false;
#endif
  for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i++) {
    Access* access = txn->accesses[read_set[i]];
#if EXTREME_MODE
    bool success = access->orig_row->manager->validate(access, false, waitFor, benefited);
#else
    bool success = access->orig_row->manager->validate(access, false);
#endif
    if (!success) {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }

  for (uint64_t i = 0; i < wr_cnt; i++) {
    Access* access = txn->accesses[write_set[i]];
#if EXTREME_MODE
    bool success = access->orig_row->manager->validate(access, true, waitFor, benefited);
#else
    bool success = access->orig_row->manager->validate(access, true);
#endif
    if (!success) {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }

#if EXTREME_MODE
  // check dependent txns
  uint64_t starttime = get_sys_clock();
  while(!waitFor.empty()){
    for(auto it = waitFor.begin(); it != waitFor.end(); ){
      auto ptxn = txn_table.find_txn_manager(get_thd_id(), *it);
      if(!ptxn){  //this txn is finished
        it = waitFor.erase(it);
      }else if(ptxn->aborted){  //this txn is aborted already
        rc = Abort;
        goto extreme_mode_stats;
      }else{  //this txn is working in process
        ++it;
      }
    }
  }
extreme_mode_stats:
  INC_STATS(get_thd_id(), extreme_mode_wait_time, get_sys_clock() - starttime);
  if(benefited){
    INC_STATS(get_thd_id(), saved_txn_cnt, 1);
  }
#endif

  if (!simulation->checkpoint_state && max_calvin_bid > simulation->checkpoint_epoch) {
    return Abort;
  }
  return rc;
}

RC TxnManager::validate_lock() {
  RC rc = RCOK;
  uint64_t wr_cnt = txn->write_cnt;
  int cur_wr_idx = 0;
  int read_set[txn->row_cnt - txn->write_cnt];
  int cur_rd_idx = 0;
  for (uint64_t rid = 0; rid < txn->row_cnt; rid++) {
    if (txn->accesses[rid]->type == WR)
      write_set[cur_wr_idx++] = rid;
    else
      read_set[cur_rd_idx++] = rid;
  }

  // TODO:
  	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
					txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
				{
					int tmp = write_set[j];
					write_set[j] = write_set[j+1];
					write_set[j+1] = tmp;
				}
			}
		}
	}

  num_locks = 0;
  bool done = false;
  if (_pre_abort) {
    for (uint64_t i = 0; i < wr_cnt; i++) {
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->_tid != txn->accesses[write_set[i]]->tid) {
        rc = Abort;
        return rc;
      }
    }
    for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i++) {
      Access* access = txn->accesses[read_set[i]];
      if (access->orig_row->manager->_tid != txn->accesses[read_set[i]]->tid) {
        rc = Abort;
        return rc;
      }
    }
  }

  while (!done) {
    num_locks = 0;
    for (uint64_t i = 0; i < wr_cnt; i++) {
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->lock_get(LOCK_EX, this) == Abort)
      {
        break;
      }
      DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
      num_locks++;
      if (row->manager->max_calvin_read_bid > max_calvin_bid || 
      (row->manager->max_calvin_read_bid == max_calvin_bid &&
       row->manager->max_calvin_read_tid % g_node_cnt > max_calvin_tid % g_node_cnt) || 
      (row->manager->max_calvin_read_bid == max_calvin_bid &&
       row->manager->max_calvin_read_tid % g_node_cnt == max_calvin_tid % g_node_cnt &&
       row->manager->max_calvin_read_tid > max_calvin_tid))
      {
        max_calvin_tid = row->manager->max_calvin_read_tid;
        max_calvin_bid = row->manager->max_calvin_read_bid;
      }
    }
    if (num_locks == wr_cnt) {
      DEBUG("TRY LOCK true %ld\n", get_txn_id());
      done = true;
    } else {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }
  return rc;
}

RC TxnManager::validate_cont() {
  RC rc = RCOK;
  uint64_t wr_cnt = txn->write_cnt;
  int read_set[txn->row_cnt - txn->write_cnt];
  int cur_rd_idx = 0;
  for (uint64_t rid = 0; rid < txn->row_cnt; rid++) {
    if (txn->accesses[rid]->type == RD) {
      read_set[cur_rd_idx++] = rid;
    }
  }

//   #if EXTREME_MODE
//   unordered_set<uint64_t> waitFor;
//   bool benefited = false;
// #endif
  for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i++) {
    Access* access = txn->accesses[read_set[i]];
// #if EXTREME_MODE
//     bool success = access->orig_row->manager->validate(access, false, waitFor, benefited);
// #else
    bool success = access->orig_row->manager->validate(access, false);
// #endif
    if (!success) {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }

  for (uint64_t i = 0; i < wr_cnt; i++) {
    Access* access = txn->accesses[write_set[i]];
// #if EXTREME_MODE
//     bool success = access->orig_row->manager->validate(access, true, waitFor, benefited);
// #else
    bool success = access->orig_row->manager->validate(access, true);
// #endif
    if (!success) {
      for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t* row = txn->accesses[i]->orig_row;
        if (key_to_part(row->get_primary_key()) == g_node_id) {
        #if WORKLOAD == TPCC
          cc_selector.update_conflict_stats(((TPCCQuery*)(this->query)), row);
        #else
          cc_selector.update_conflict_stats(row);
        #endif
        }
      }
      rc = Abort;
      return rc;
    }
  }

// #if EXTREME_MODE
//   // check dependent txns
//   uint64_t starttime = get_sys_clock();
//   while(!waitFor.empty()){
//     for(auto it = waitFor.begin(); it != waitFor.end(); ){
//       auto ptxn = txn_table.find_txn_manager(get_thd_id(), *it);
//       if(!ptxn){  //this txn is finished
//         it = waitFor.erase(it);
//       }else if(ptxn->aborted){  //this txn is aborted already
//         rc = Abort;
//         goto extreme_mode_stats;
//       }else{  //this txn is working in process
//         ++it;
//       }
//     }
//   }
// extreme_mode_stats:
//   INC_STATS(get_thd_id(), extreme_mode_wait_time, get_sys_clock() - starttime);
//   if(benefited){
//     INC_STATS(get_thd_id(), saved_txn_cnt, 1);
//   }
// #endif

  if (!seq_man.checkDependency(max_calvin_bid, max_calvin_tid)) {
    rc = Abort;
    return Abort;
  }
  return rc;
}

RC TxnManager::finish(RC rc) {
  if (rc == Abort) {
    for (uint64_t i = 0; i < this->num_locks; i++) {  
      txn->accesses[write_set[i]]->orig_row->manager->lock_release(this);
      DEBUG("silo %ld abort release row %ld \n", this->get_txn_id(),
            txn->accesses[write_set[i]]->orig_row->get_primary_key());
    }
  } else {
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
      Access* access = txn->accesses[write_set[i]];
      row_t* row = access->orig_row;
      if (!belong_checkpoint) {
        if (row->live_data == NULL) {
          row->live_data = (char *)mem_allocator.alloc(sizeof(char) * row->tuple_size);
          row->data = row->live_data;
        }
      }
      row->copy(access->data);
      row->manager->_tid = get_txn_id();
      txn->accesses[write_set[i]]->orig_row->manager->lock_release(this);
      DEBUG("silo %ld commit release row %ld \n", this->get_txn_id(),
            txn->accesses[write_set[i]]->orig_row->get_primary_key());
    }
  }
  num_locks = 0;
  memset(write_set, 0, sizeof(write_set));
  // mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
  return rc;
}


#endif