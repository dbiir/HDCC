#ifndef CONFLICT_THREAD
#define CONFLICT_THREAD

#include "global.h"

class ConflictThread : public Thread {
public:
  RC run();
  void setup();
};

#endif