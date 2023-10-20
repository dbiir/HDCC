#include "global.h"
#include "thread.h"
#include "conflict_thread.h"
#include "msg_queue.h"

RC ConflictThread::run() {
    tsetup();
    printf("Running ConflictThread %ld\n",_thd_id);

    uint64_t last_time = get_sys_clock(), now_time;
    while (!simulation->is_done()){
        now_time = get_sys_clock();
        if (now_time - last_time > g_conflict_send_interval) {
            cc_selector.update_ccselector();
            last_time = now_time;
        }
    }

    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
    return FINISH;
}

void ConflictThread::setup() {
}
