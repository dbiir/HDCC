#include "global.h"
#include "thread.h"
#include "conflict_thread.h"
#include "msg_queue.h"

RC ConflictThread::run() {
    RC rc = RCOK;
    while (!simulation->is_done()) {
        sleep(1);
        //first send local msg
        send_msg(cc_selector.pack_msg());
        //then process msg received from remote nodes
        recv_msg();
        //switch pointers periodically
        cc_selector.ptr_switch();
    }
    return rc;
}

void ConflictThread::setup() {
}

void ConflictThread::send_msg(Message *msg){
    //send msg to all other servers excepet itself by using msg_queue
    for(uint64_t i=0;i<g_node_cnt;i++){
        if(i!=g_node_id){
            msg_queue.enqueue(_thd_id,msg,i);
        }
    }
}

void ConflictThread::recv_msg(){
    bool isReceived[g_node_cnt]={false};
    uint16_t recv_cnt=0;
    while(true){
        if(!g_conflict_queue.empty()){
            auto msg=g_conflict_queue.front();
            g_conflict_queue.pop();
            if(isReceived[msg->get_return_id()]){//receive msg from node from which we already get corresponding msg, push it back to the queue
                g_conflict_queue.push(msg);
            }else{
                isReceived[msg->get_return_id()]=true;
                cc_selector.process_conflict_msg(msg);
                recv_cnt++;
                if(recv_cnt==g_node_cnt-1){
                    break;//receive msg from all other nodes
                }
            }
        }
    }
}
