/*
 * Header

  Wicci Project C Code Header
  Utility Code for Debugging

 ** Copyright

  Copyright (c) 2005-2009 J. Greg Davidson.
  You may use this software under the terms of the
  GNU AFFERO GENERAL PUBLIC LICENSE
  as specified in the file LICENSE.md included with this distribution.
  All other use requires my permission in writing.
 *
 */

#define C_DEBUG_NO_PG
#include "debug-log.h"

int main() {
  // debug-log features
  debug_log_open("debug-test.log");
  AssertThat(debug_log_ != 0);
  // debug features
  _Static_assert(1 + 1 == 2, "debug features");
  DebugSetOn();
  // debug-log and debug features
  if ( DebugLevel > 0 )  WARN_OUT("This is your last warning.");
  if ( DebugLevel > 0 ) BUG_OUT("This should be an error.");
  return 0;
}
