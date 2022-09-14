#include "global.h"
#include "cc_selector.h"
#include "mem_alloc.h"
#include "msg_queue.h"

void CCSelector::init(){
    pstats=new uint64_t[g_total_shard_num];
    is_high_conflict=new bool[g_total_shard_num];
    memset(pstats,0,g_total_shard_num*sizeof(uint64_t));
    memset(is_high_conflict,false,g_total_shard_num*sizeof(bool));
    printf("\nMax key:%lu, node:%lu, shard:%lu\n",g_synth_table_size,g_synth_table_size%g_part_cnt,key_to_shard(g_synth_table_size));
}
CCSelector::~CCSelector(){
    delete [] pstats;
    delete [] is_high_conflict;
}
int CCSelector::get_best_cc(Workload *wl,Message *msg){
    auto req=((YCSBClientQueryMessage*)msg)->requests;
    for(uint64_t i=0;i<req.size();i++){
        auto shard=((YCSBWorkload*)wl)->key_to_shard(req[i]->key);
        if(is_high_conflict[shard]){//if txn visits high contention shard, use CALVIN
            return CALVIN;
        }
    }
    return SILO;//if txn visits only low contention shards, use SILO
}
void CCSelector::update_conflict_stats(uint64_t shard,uint64_t value){
    ATOM_ADD(pstats[shard],value);
}
void CCSelector::update_ccselector(){
    for(uint64_t i=0;;i++){//i equals to shard_number_in_node, refer to key_to_shard for more information
        uint64_t shard=i*g_node_cnt+g_node_id;
        if(shard>=g_total_shard_num){
            break;
        }
        // if(pstats[shard]>2){
        //     printf("shard:%lu, value:%lu\n",shard,pstats[shard]);
        // }
        if(pstats[shard]<g_lower_bound){
            is_high_conflict[shard]=false;
            // printf("shard %ld is not high conflict\n",shard);
        }else if(pstats[shard]>g_upper_bound){
            is_high_conflict[shard]=true;
            // printf("shard %ld is high conflict\n",shard);
        }
    }
    //fflush(stdout);
    memset(pstats,0,g_total_shard_num*sizeof(uint64_t));
}
Message* CCSelector::pack_msg(){
    Message *msg=Message::create_message(CONF_STAT);//initialization of conflict_statistics finishes inside this function
    for(uint64_t i=0;i<g_total_shard_num;i++){
        ((ConflictStaticsMessage*)msg)->conflict_statics.add(is_high_conflict[i]);
    }
    return msg;
}
void CCSelector::process_conflict_msg(ConflictStaticsMessage *msg){
    printf("\nCCSelector process conflict msg\n");
    uint64_t node_num=msg->get_return_id();
    for(uint64_t i=0;;i++){//i equals to shard_number_in_node, refer to key_to_shard for more information
        uint64_t shard=i*g_node_cnt+node_num;
        if(shard>=g_total_shard_num){
            break;
        }
        is_high_conflict[shard]=msg->conflict_statics[shard];
    }
    msg->release();//msg life ends here
}