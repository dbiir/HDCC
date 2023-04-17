#include "txn.h"
#include "row.h"
#include "row_snapper.h"

#if CC_ALG == SNAPPER
RC TxnManager::validate_snapper() {
    assert(algo != CALVIN);
    RC rc = RCOK;
    
    // Under 2PC, master and slave check themselves first, (dependBy is empty at this point)
    // if inter-dependency exists, then Abort; else, slave send its dependency info to master
    // then master will have a global view of dependency to take a second check (dependBy shall NOT be empty at this point)
    if(dependBy.empty()){
        for (int rid = txn->row_cnt - 1; rid >= 0; rid --) {
            txn->accesses[rid]->orig_row->manager->findDependBy(this);
        }
    }
    // printf("txn id: %ld\ndependon\n", get_txn_id());
    // for(auto it = dependOn.begin(); it != dependOn.end(); ++it){
    //     printf("%ld\t", *it);
    // }
    // printf("\n dependby\n");
    // for(auto it = dependBy.begin(); it != dependBy.end(); ++it){
    //     printf("%ld\t", *it);
    // }
    // printf("\n");
    // check if two sets are intersected
    for(auto it1 = dependOn.begin(), it2 = dependBy.begin(); it1 != dependOn.end() && it2 != dependBy.end(); ){
        if(*it1 == *it2){
            rc = Abort;
            INC_STATS(get_thd_id(), snapper_validate_abort_cnt, 1);
            break;
        }else if(*it1 < *it2){
            ++it1;
        }else{
            ++it2;
        }
    }
    return rc;
}
#endif