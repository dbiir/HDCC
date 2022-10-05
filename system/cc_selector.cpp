#include "global.h"
#include "cc_selector.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "ycsb_query.h"
#include "tpcc_helper.h"
#include "tpcc.h"
#include "tpcc_query.h"

void CCSelector::init(){
    pstats=new uint64_t[g_total_shard_num];
    is_high_conflict=new bool[g_total_shard_num];
    memset(pstats,0,g_total_shard_num*sizeof(uint64_t));
    memset(is_high_conflict,false,g_total_shard_num*sizeof(bool));
}
CCSelector::~CCSelector(){
    delete [] pstats;
    delete [] is_high_conflict;
}
int CCSelector::get_best_cc(Message *msg){
#if WORKLOAD == YCSB
    auto req=((YCSBClientQueryMessage*)msg)->requests;
    for(uint64_t i=0;i<req.size();i++){
        auto shard = key_to_shard(req[i]->key);
        if(is_high_conflict[shard]){//if txn visits high contention shard, use CALVIN
            return CALVIN;
        }
    }
    return SILO;//if txn visits only low contention shards, use SILO
#elif WORKLOAD == TPCC
    auto tpcc_msg = (TPCCClientQueryMessage*)msg;
    uint64_t w_id = tpcc_msg->w_id;
	uint64_t d_id = tpcc_msg->d_id;
	uint64_t c_id = tpcc_msg->c_id;
	uint64_t d_w_id = tpcc_msg->d_w_id;
	uint64_t c_w_id = tpcc_msg->c_w_id;
	uint64_t c_d_id = tpcc_msg->c_d_id;
	char * c_last = tpcc_msg->c_last;
	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
    uint64_t key, shard;
	switch(tpcc_msg->txn_type) {
		case TPCC_PAYMENT:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			    // WH
                key = w_id;
                key += TPCCTableKey::WAREHOUSE_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
			    // Dist
			    key = distKey(d_id, d_w_id);
                key += TPCCTableKey::DISTRICT_OFFSET;
				shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
			}
			if(GET_NODE_ID(part_id_c_w) == g_node_id) {
			    // Cust
				if (tpcc_msg->by_last_name) {
				    key = custNPKey(c_last, c_d_id, c_w_id);
                    key += TPCCTableKey::CUST_BY_NAME_OFFSET;
                    shard = key_to_shard(key);
                    if(is_high_conflict[shard]){
                        return CALVIN;
                    }
				} else {
					key = custKey(c_id, c_d_id, c_w_id);
                    key += TPCCTableKey::CUST_BY_ID_OFFSET;
                    shard = key_to_shard(key);
                    if(is_high_conflict[shard]){
                        return CALVIN;
                    }
				}
			}
			break;
		case TPCC_NEW_ORDER:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			    // WH
                key = w_id;
                key += TPCCTableKey::WAREHOUSE_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
			    // Cust
                key = custKey(c_id, d_id, w_id);
                key += TPCCTableKey::CUST_BY_ID_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
                // Dist
                key = distKey(d_id, w_id);
                key += TPCCTableKey::DISTRICT_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
			}
            // Items
            for(uint64_t i = 0; i < tpcc_msg->ol_cnt; i++) {
                if (GET_NODE_ID(wh_to_part(tpcc_msg->items[i]->ol_supply_w_id)) != g_node_id) continue;
                // item
                key = tpcc_msg->items[i]->ol_i_id;
                key += TPCCTableKey::ITEM_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
                // stock
                key = stockKey(tpcc_msg->items[i]->ol_i_id, tpcc_msg->items[i]->ol_supply_w_id);
                key += TPCCTableKey::STOCK_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
            }
			break;
		default:
			assert(false);
	}
    return SILO;
#endif
}
void CCSelector::update_conflict_stats(uint64_t shard,uint64_t value){
    ATOM_ADD(pstats[shard],value);
}
void CCSelector::update_ccselector(){
#if WORKLOAD == TPCC
    for(uint64_t i = TPCCTableKey::CUST_BY_ID_START+TPCCTableKey::CUST_BY_ID_OFFSET; i < TPCCTableKey::CUST_BY_ID_END+TPCCTableKey::CUST_BY_ID_OFFSET; i++){
        auto shard = key_to_shard(i);
        //  for table Customer, 60% cases we use index Customer_by_ID, so we need some scaling up to restore true conflict value
        pstats[shard] = pstats[shard] / 0.6;
    }
    for(uint64_t i = TPCCTableKey::CUST_BY_NAME_START+TPCCTableKey::CUST_BY_NAME_OFFSET; i < TPCCTableKey::CUST_BY_NAME_END+TPCCTableKey::CUST_BY_NAME_OFFSET; i++){
        auto shard = key_to_shard(i);
        //  for table Customer, 40% cases we use index Customer_by_NAME, so we need some scaling up to restore true conflict value
        pstats[shard] = pstats[shard] / 0.4;
    }
#endif
    for(uint64_t i=0;;i++){//i equals to shard_number_in_node, refer to key_to_shard for more information
        uint64_t shard=i*g_node_cnt+g_node_id;
        if(shard>=g_total_shard_num){
            break;
        }
        if(pstats[shard]<g_lower_bound){
            is_high_conflict[shard]=false;
        }else if(pstats[shard]>g_upper_bound){
            is_high_conflict[shard]=true;
        }
    }
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

uint64_t CCSelector::get_total_conflict() {
    uint64_t sum = 0;
    for (uint64_t i = 0;; i++) {
        uint64_t shard=i*g_node_cnt+g_node_id;
        if(shard>=g_total_shard_num){
            break;
        }
        sum += pstats[shard];
    }
    return sum;
}

uint64_t CCSelector::get_highest_conflict() {
    return pstats[g_node_id];
}
