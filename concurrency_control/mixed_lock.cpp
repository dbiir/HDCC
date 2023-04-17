#include "global.h"
#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "query.h"
#include "row.h"
#include "row_mixed_lock.h"
#include "row_mvcc.h"
#include "row_ts.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "cc_selector.h"
#include <unordered_set>

#if CC_ALG == MIXED_LOCK
// txn->algo == silo
// silo验证
RC TxnManager::validate_silo() {
  RC rc = RCOK;
  uint64_t wr_cnt = txn->write_cnt;
  int cur_wr_idx = 0;
  int read_set[txn->row_cnt - txn->write_cnt];
  int cur_rd_idx = 0;
  for (uint64_t rid = 0; rid < txn->row_cnt; rid++) {  //读写集标记
    if (txn->accesses[rid]->type == WR)
      write_set[cur_wr_idx++] = rid;
    else
      read_set[cur_rd_idx++] = rid;
  }

  // 对写集中的数据进行排序(primary key)，防止给写数据加锁时出现死锁
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
    for (uint64_t i = 0; i < wr_cnt; i++) {  // pre_abort
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->_tid != txn->accesses[write_set[i]]->tid) {   //版本应该为当前事务id
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

  // lock all rows in the write set.
  //对写集中的数据加锁
  while (!done) {
    num_locks = 0;
    for (uint64_t i = 0; i < wr_cnt; i++) {
      row_t* row = txn->accesses[write_set[i]]->orig_row;
      if (row->manager->lock_get(LOCK_EX, this) == Abort)  //失败（calvin读写锁、silo写锁），回滚；成功，在此处加锁
      {
        break;
      }
      DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
      num_locks++;
    }
    if (num_locks == wr_cnt) {  //所有写集数据加锁成功
      DEBUG("TRY LOCK true %ld\n", get_txn_id());
      done = true;
    } else {  //中途加锁失败，回滚
      // If silo fail to lock one row, we cannot know if the other rows are locked or not, so we count all rows as conflict.
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
  // validate rows in the read set
  // for repeatable_read, no need to validate the read set.
  // 检查读集的wts信息是否发生过变化/读集中数据是否被其他事务加锁->读写冲突

  //读验证时检测到calvin的写锁，回滚；版本变化，回滚；
  for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i++) {
    Access* access = txn->accesses[read_set[i]];
#if EXTREME_MODE
    bool success = access->orig_row->manager->validate(access, false, waitFor, benefited);
#else
    bool success = access->orig_row->manager->validate(access, false);  //当前行上有写锁/版本变化
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

  // validate rows in the write set
  // 写验证
  for (uint64_t i = 0; i < wr_cnt; i++) {
    Access* access = txn->accesses[write_set[i]];
#if EXTREME_MODE
    bool success = access->orig_row->manager->validate(access, true, waitFor, benefited);
#else
    bool success = access->orig_row->manager->validate(access, true);  //时间戳版本正确即可
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
  return rc;
}

// silo 执行
RC TxnManager::finish(RC rc) {
  if (rc == Abort) {
    for (uint64_t i = 0; i < this->num_locks; i++) {  
      txn->accesses[write_set[i]]->orig_row->manager->lock_release(this);
      DEBUG("silo %ld abort release row %ld \n", this->get_txn_id(),
            txn->accesses[write_set[i]]->orig_row->get_primary_key());
    }
  } else {  //写入
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
      Access* access = txn->accesses[write_set[i]];
      row_t* row = access->orig_row;
      row->copy(access->data);                                           //写入数据
      row->manager->_tid = get_txn_id();                                      //更新时间戳(事务号)
      txn->accesses[write_set[i]]->orig_row->manager->lock_release(this);  //释放锁
      DEBUG("silo %ld commit release row %ld \n", this->get_txn_id(),
            txn->accesses[write_set[i]]->orig_row->get_primary_key());
    }
  }
  num_locks = 0;
  memset(write_set, 0, 100);
  // mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
  return rc;
}


#endif