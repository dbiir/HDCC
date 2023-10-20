#ifndef _CONFLICT_THREAD_
#define _CONFLICT_THREAD_

#include "global.h"
#include "cc_selector.h"
#include "message.h"

class ConflictThread : public Thread {
public:
  RC run();
  void setup();
};

#endif