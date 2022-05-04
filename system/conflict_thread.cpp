#include "thread.h"
#include "conflict_thread.h"

RC ConflictThread::run() {
    RC rc = RCOK;

    while (!simulation->is_done()) {
        //
    }
    return rc;
}

void ConflictThread::setup() {}
