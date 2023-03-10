#if CC_ALG == SNAPPER
#include "txn.h"
#include "row.h"
#include "row_snapper.h"

RC TxnManager::validate_snapper() {
    assert(algo != CALVIN);
    RC rc = RCOK;
    
    for (int rid = txn->row_cnt - 1; rid >= 0; rid --) {
        bool valid = txn->accesses[rid]->orig_row->manager->validate(this);
        if (!valid) {
            rc = Abort;
            break;
        }
    }
    return rc;
}
#endif