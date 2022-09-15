#include "global.h"
#include "thread.h"
#include "conflict_thread.h"
#include "msg_queue.h"

RC ConflictThread::run() {
    tsetup();
    printf("Running ConflictThread %ld\n",_thd_id);

    uint64_t last_send_time = get_sys_clock();
    // bool isReceived[g_node_cnt]={false};
    // uint16_t recv_cnt=0;
    // int send_round=0,recv_round=0;

    int loop_round = 0;

    while (!simulation->is_done()) {
        // //send msg
        // uint64_t now_time = get_sys_clock();
        // if (now_time - last_send_time > g_conflict_send_interval) {
        //     if(now_time-last_send_time>2*g_conflict_send_interval){
        //         printf("msg overloaded, time interval:%lu\n",(now_time-last_send_time)/g_conflict_send_interval);
        //     }
        //     if(send_round-recv_round>1){
        //         printf("msg overloaded, send_round is %d rounds ahead of recv_round\n",send_round-recv_round);
        //     }
        //     send_msg();
        //     last_send_time = now_time;
        //     send_round++;
        // }

        // //receive msg
        // if (g_conflict_queue.empty()){
        //     continue;
        // }
        // ConflictStaticsMessage* msg = g_conflict_queue.front();
        // assert(msg);
        // g_conflict_queue.pop();
        // if (isReceived[msg->get_return_id()]){
        //     g_conflict_queue.push(msg);
        // } else {
        //     recv_cnt++;
        //     isReceived[msg->get_return_id()] = true;
        //     cc_selector.process_conflict_msg(msg);
        //     if (recv_cnt == g_node_cnt - 1)
        //     {
        //         recv_cnt = 0;
        //         memset(isReceived, false, sizeof(isReceived));
        //         cc_selector.update_ccselector();
        //         recv_round++;
        //     }
        // }

//for single node
        uint64_t now_time = get_sys_clock();
        if (now_time - last_send_time > g_conflict_send_interval) {
            INC_STATS(_thd_id, row_conflict_total_cnt[loop_round], cc_selector.get_total_conflict());
            INC_STATS(_thd_id, row_conflict_highest_cnt[loop_round], cc_selector.get_highest_conflict());
            cc_selector.update_ccselector();
            loop_round++;
            last_send_time = now_time;
        }
    }
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
            printf("send to %lu\n",i);
            msg_queue.enqueue(_thd_id,msg,i);
        }
    }
}
