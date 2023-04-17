#include "snapper_check_thread.h"

#if CC_ALG == SNAPPER
void SnapperCheckThread::setup() {}

RC SnapperCheckThread::run() {
    tsetup();
    printf("Running SnapperCheckThread %ld\n",_thd_id);
    while (!simulation->is_done()) {
        heartbeat();
        txn_table.snapper_check();
    }
    return FINISH;
}
#endif