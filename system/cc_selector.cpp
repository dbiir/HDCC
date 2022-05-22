#include "global.h"
#include "cc_selector.h"
#include "mem_alloc.h"
#include "msg_queue.h"

CCSelector::CCSelector(){
}

CCSelector::~CCSelector(){
    delete old_ptr;
    delete new_ptr;
    delete is_high_conflict;
}

void CCSelector::init() {
    old_ptr=new uint16_t[g_shard_num];
    new_ptr=new uint16_t[g_shard_num];
    is_high_conflict=new bool[g_shard_num];//this array has not been used for this version
    memset(old_ptr,0,g_shard_num*sizeof(uint16_t));
    memset(new_ptr,0,g_shard_num*sizeof(uint16_t));
    memset(is_high_conflict,false,g_shard_num*sizeof(bool));
}

int CCSelector::get_best_cc(Workload *wl,Message *msg){
    auto req=((YCSBClientQueryMessage*)msg)->requests;
    for(uint64_t i=0;i<req.size();i++){
        auto shard=((YCSBWorkload*)wl)->key_to_shard(req[i]->key);
        if(old_ptr[shard]>g_lower_bound){//if txn visits high contention shard, use CALVIN
            return CALVIN;
        }
    }
    return SILO;//if txn visits only low contention shards, use SILO
}
void CCSelector::update_conflict_stats(uint64_t shard,uint16_t value){
    if(new_ptr[shard]>g_upper_bound){
        return;//no need to add, cause the conflict value is already big enough
    }else{
        ATOM_ADD(new_ptr[shard],value);
    }
}
void CCSelector::ptr_switch(){
    uint16_t *tmp=old_ptr;
    old_ptr=new_ptr;
    new_ptr=tmp;
    memset(new_ptr,0,g_shard_num*sizeof(uint16_t));
}
Message* CCSelector::pack_msg(){
    Message *msg=Message::create_message(CONF_STAT);//initialization of conflict_statistics finishes inside this function
    for(uint64_t i=0;i<g_shard_num;i++){
        ((ConflictStaticsMessage*)msg)->conflict_statics.add(new_ptr[i]);
    }
    return msg;
}
void CCSelector::process_conflict_msg(ConflictStaticsMessage *msg){
    printf("\nCCSelector process conflict msg\n");
    for(uint64_t i=0;i<g_shard_num;i++){
        if(i<100){
            printf("%u\t",(unsigned int)msg->conflict_statics[i]);
        }
        update_conflict_stats(i,msg->conflict_statics[i]);
    }
    msg->release();//msg life ends here
}