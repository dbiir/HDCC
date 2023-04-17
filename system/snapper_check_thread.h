#ifndef _SNAPPER_CHECK_THREAD_
#define _SNAPPER_CHECK_THREAD_

#if CC_ALG == SNAPPER
#include "global.h"
#include "thread.h"

class SnapperCheckThread : public Thread {
public:
  RC run();
  void setup();
};

#endif
#endif