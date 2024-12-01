#include "global.h"
#include "helper.h"
#include "thread.h"
#include "checkpoint_thread.h"
#include "ycsb.h"
#include "index_hash.h"
#include "row.h"

void CheckpointThread::setup() {}

RC CheckpointThread::run() {
    tsetup();
    printf("Running CheckpointThread %ld\n",_thd_id);
    std::ofstream checkpoint_file;
    checkpoint_file.open("./checkpoint", std::ios::out | std::ios::binary);
    while (!simulation->is_half_done()) {
        sleep(1);
    }
    simulation->checkpoint_epoch = simulation->get_seq_epoch();
    while (!simulation->checkpoint_state) {
        usleep(5000);
    }
    uint64_t time = get_sys_clock() - simulation->run_starttime;
    printf("Checkpointing at %ld\n", time / BILLION);
    // retrieve all the data and write to disk
    assert(WORKLOAD == YCSB);
    YCSBWorkload *wl = (YCSBWorkload *)_wl;
    INDEX * index = wl->the_index;
    BucketHeader ** buckets = NULL;
    uint64_t bucket_num = index->get_buckets(buckets);
    for (uint64_t i = 0; i < bucket_num; i++) {
        BucketNode * node = buckets[0][i].first_node;
        while (node != NULL) {
            itemid_t * item = node->items;
            row_t * row = (row_t *)item->location;
            checkpoint_file.write((char *)row->stable_data, row->tuple_size);
            node = node->next;
        }
    }
    checkpoint_file.close();

    time = get_sys_clock() - simulation->run_starttime;
    printf("Checkpoint done at %ld\n", time / BILLION);
    return FINISH;
}