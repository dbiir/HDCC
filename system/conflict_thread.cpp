#include "global.h"
#include "thread.h"
#include "conflict_thread.h"
#include "msg_queue.h"

RC ConflictThread::run() {
    tsetup();
    printf("Running ConflictThread %ld\n",_thd_id);

#if NODE_CNT == 1
    uint64_t last_time = get_sys_clock(), now_time;
    while (!simulation->is_done()){
        now_time = get_sys_clock();
        if (now_time - last_time > g_conflict_send_interval) {
            cc_selector.update_ccselector();
            last_time = now_time;
        }
    }
#else
    uint64_t last_send_time = get_sys_clock(), now_time;
    bool isReceived[g_node_cnt] = {false};
    uint16_t recv_cnt = 0;

    while (!simulation->is_done()) {
        //send msg
        now_time = get_sys_clock();
        if (now_time - last_send_time > g_conflict_send_interval) {
            send_msg();
            last_send_time = now_time;
        }
        //receive msg
        if (g_conflict_queue.empty()){
            continue;
        }
        ConflictStaticsMessage* msg = g_conflict_queue.front();
        assert(msg);
        g_conflict_queue.pop();
        if (isReceived[msg->get_return_id()]){
            g_conflict_queue.push(msg);
        } else {
            recv_cnt++;
            isReceived[msg->get_return_id()] = true;
            cc_selector.process_conflict_msg(msg);
            if (recv_cnt == g_node_cnt - 1){
                recv_cnt = 0;
                memset(isReceived, false, sizeof(isReceived));
                cc_selector.update_ccselector();
            }
        }
    }
#endif
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
    return FINISH;
}

void ConflictThread::setup() {
}

void ConflictThread::send_msg(){
    auto msg=cc_selector.pack_msg();
    //send msg to all other servers excepet itself by using msg_queue
    for(uint64_t i=0;i<g_node_cnt;i++){
        if(i!=g_node_id){
            msg_queue.enqueue(_thd_id,msg,i);
        }
    }
}
