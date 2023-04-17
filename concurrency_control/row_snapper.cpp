#include "txn.h"
#include "row.h"
#include "row_snapper.h"
#include "mem_alloc.h"
#include "cc_selector.h"
#include "tpcc_query.h"
#include <algorithm>

#if CC_ALG == SNAPPER

void Row_snapper::init(row_t * row) {
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
    last_batch_id = 0;
}

void Row_snapper::return_entry(LockEntry *entry) {
    // DEBUG_M("row_lock::return_entry free %lx\n",(uint64_t)entry);
    mem_allocator.free(entry, sizeof(LockEntry));
}

bool Row_snapper::conflict_lock(lock_t l1, lock_t l2) {
    if (l1 == LOCK_NONE || l2 == LOCK_NONE)
        return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else
        return false;
}

LockEntry *Row_snapper::get_entry() {
    LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    // DEBUG_M("row_lock::get_entry alloc %lx\n",(uint64_t)entry);
    return entry;
}

void Row_snapper::enter_critical_section() {
    pthread_mutex_lock(_latch);
}

void Row_snapper::leave_critical_section() {
    pthread_mutex_unlock(_latch);
}

RC Row_snapper::lock_get(lock_t type, TxnManager *txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t lock_get_start_time = starttime;

    uint64_t mtx_wait_starttime = get_sys_clock();
    if (txn->algo == WAIT_DIE) {
        pthread_mutex_lock(_latch);
    }
    INC_STATS(txn->get_thd_id(), mtx[17], get_sys_clock() - mtx_wait_starttime);

    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);

    bool conflict = conflict_lock(lock_type, type);
    if (txn->algo == CALVIN && !conflict) {
        if (waiters_head) conflict = true;
    }
    if (txn->algo == WAIT_DIE && !conflict) {
        if (waiters_head && txn->get_timestamp() < waiters_head->txn->get_timestamp()) {
            conflict = true;
        }
    }

    if (conflict) {
        DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
            txn->get_txn_id(), txn->get_batch_id(), owner_cnt, lock_type, type,
            _row->get_primary_key(), (uint64_t)_row);

        if (txn->algo == CALVIN) {
            LockEntry *entry = get_entry();
            entry->start_ts = get_sys_clock();
            entry->txn = txn;
            entry->type = type;

            LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
            waiter_cnt++;

            ATOM_CAS(txn->lock_ready, true, false);
            rc = WAIT;
        } else if (txn->algo == WAIT_DIE) {
            bool canwait = true;
            LockEntry * en;
            en = owners;
            while (en != NULL) {
                assert(txn->get_txn_id() != en->txn->get_txn_id());
                assert(txn->get_timestamp() != en->txn->get_timestamp());
                if (en->txn->algo == WAIT_DIE && txn->get_timestamp() > en->txn->get_timestamp()) {
                    INC_STATS(txn->get_thd_id(), twopl_diff_time, txn->get_timestamp() - en->txn->get_timestamp());
                    canwait = false;
                    break;
                }
                en = en->next;
            }
            if (canwait) {
                // insert txn to the right position
                LockEntry * entry = get_entry();
                entry->start_ts = get_sys_clock();
                entry->txn = txn;
                entry->type = type;
                LockEntry * en;
                ATOM_CAS(txn->lock_ready, true, false);
                
                txn->incr_lr();
                
                int64_t batch_id;
                en = waiters_head;
                while (en != NULL && en->txn->algo != CALVIN) {
                    en = en->next;
                }
                if (en) {
                    batch_id = en->txn->get_batch_id();
                }
                en = waiters_tail;
                while (en != NULL && txn->get_timestamp() > en->txn->get_timestamp() && (en->txn->algo == WAIT_DIE || batch_id == en->txn->get_batch_id())) {
                    en = en->prev;
                }
                if (en) {
                    LIST_INSERT_AFTER(en, entry, waiters_tail);
                    while(en != NULL) {
                        if (en->txn->algo == CALVIN) {
                            entry->txn->dependOn.insert(en->txn->get_batch_id());
                        }
                        en = en->prev;
                    }
                } else {
                    LIST_PUT_HEAD(waiters_head, waiters_tail, entry);
                    entry->txn->dependOn.insert(last_batch_id);
                }
                waiter_cnt ++;
                txn->last_lock_ts = entry->start_ts;
                DEBUG("lk_wait (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
                    txn->get_txn_id(), txn->get_batch_id(), owner_cnt, lock_type, type,
                    _row->get_primary_key(), (uint64_t)_row);
                //txn->twopl_wait_start = get_sys_clock();
                rc = WAIT;
                //txn->wait_starttime = get_sys_clock();
            } else {
                rc = Abort;
            }
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
        if (txn->algo == WAIT_DIE) {
            ATOM_CAS(entry->txn->lock_ready, false, true);
        }

        if(owner_cnt > 0) {
          assert(type == LOCK_SH);
          INC_STATS(txn->get_thd_id(),twopl_sh_bypass_cnt,1);
        }
        if(txn->get_timestamp() > max_owner_ts) {
          max_owner_ts = txn->get_timestamp();
        }
        owner_cnt++;
        if (lock_type == LOCK_NONE) {
            own_starttime = get_sys_clock();
        }
        lock_type = type;

        if (txn->algo == CALVIN) {
            while(!ATOM_CAS(entry->txn->wait_for_locks_ready, true, false)) {}
            int delete_cnt = txn->wait_for_locks.erase(&*_row);
            assert (delete_cnt == 1);
            ATOM_CAS(entry->txn->wait_for_locks_ready, false, true);

            if (txn->get_batch_id() != last_batch_id) {
                last_batch_id = txn->get_batch_id();
            }
        }
        rc = RCOK;
    }

    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    if (txn->algo == WAIT_DIE) {
        pthread_mutex_unlock(_latch);
    }

    return rc;
}

RC Row_snapper::lock_release(TxnManager *txn) {
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
            INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
            uint64_t endtime = get_sys_clock();
            INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
            if(lock_type == LOCK_SH) {
                INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
                INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
            } else {
                INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
                INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
            }
            lock_type = LOCK_NONE;
        }
    } else {
        en = waiters_head;
        while (en != NULL && en->txn != txn) en = en->next;
        ASSERT(en && en->txn->algo != CALVIN);
        LIST_REMOVE(en);
        if (en == waiters_head) waiters_head = en->next;
        if (en == waiters_tail) waiters_tail = en->prev;
        return_entry(en);
        waiter_cnt--;
        // goto final;
    }

    if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);

    LockEntry *entry;

    en = waiters_head;
    while (en && !conflict_lock(lock_type, en->type)) {
        if (en->txn->algo == WAIT_DIE && en->txn->lock_ready) {
            en = en->next;
            continue;
        }
        entry = en;
        en = en->next;
        LIST_REMOVE(entry);
        if (entry == waiters_head) waiters_head = entry->next;
        if (entry == waiters_tail) waiters_tail = entry->prev;
        uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
        entry->txn->twopl_wait_start = 0;
        if(txn->algo != CALVIN) {
            entry->txn->txn_stats.cc_block_time += timespan;
            entry->txn->txn_stats.cc_block_time_short += timespan;
        }
        INC_STATS(txn->get_thd_id(),twopl_wait_time,timespan);

        STACK_PUSH(owners, entry);

        owner_cnt++;
        waiter_cnt--;
        if(entry->txn->get_timestamp() > max_owner_ts) {
            max_owner_ts = entry->txn->get_timestamp();
        }


        if (entry->txn->algo == CALVIN) {
            while(!ATOM_CAS(entry->txn->wait_for_locks_ready, true, false)) {}
            int delete_cnt = entry->txn->wait_for_locks.erase(&*_row);
            if (delete_cnt != 1) {
                    assert(false);
            }
            ATOM_CAS(entry->txn->wait_for_locks_ready, false, true);

            if (entry->txn->get_batch_id() != last_batch_id) {
                last_batch_id = entry->txn->get_batch_id();
            }
            if (entry->txn->wait_for_locks.empty()) {
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
        } else {
            if (entry->txn->decr_lr() == 0) {
                if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                    txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),
                                        entry->txn->get_batch_id());
                } else {
                    INC_STATS(txn->get_thd_id(), snapper_false_deadlock,1)
                }
            }
            if (lock_type == LOCK_NONE) {
                own_starttime = get_sys_clock();
            }
            lock_type = entry->type;
        }
    }

final:
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(),twopl_release_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_release_cnt,1);

    pthread_mutex_unlock(_latch);

    return RCOK;
}

void Row_snapper::findDependBy(TxnManager * txn) {
    LockEntry * en = waiters_head;
    while (en != NULL) {
        if (en->txn->algo == CALVIN) {
            txn->dependBy.insert(en->txn->get_batch_id());
        }
        en = en->next;
    }
}

#endif