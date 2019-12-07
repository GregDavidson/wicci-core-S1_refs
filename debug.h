/*
 * Header

	Wicci Project C Code Header
	Utility Code for Debugging and Initialization Management

 ** Copyright

	Copyright (c) 2005-2009 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.

 * Dependencies:
	PostgreSQL unless you define macro C_DEBUG_NO_PG
 */

#ifndef C_DEBUG_H
#define C_DEBUG_H

#include <stdio.h>
// maybe merge with debug-log.h ??
// #include "debug-log.h"

// Standard C now defines _Static_assert(cond, str)
#if 0
// generate compile error when cond is false:
#if 0
#define STATIC_ASSERT(cond) sizeof(char[1-2*!(cond)])
#else
#define STATIC_ASSERT(e) typedef \
	struct { int STATIC_ASSERT_NAME_(__LINE__): ((e) ? 1 : -1); } \
	STATIC_ASSERT_NAME_(__LINE__)[(e) ? 1 : -1]
#define STATIC_ASSERT_NAME_(line)	STATIC_ASSERT_NAME2_(line)
#define STATIC_ASSERT_NAME2_(line)	assertion_failed_at_##line
#endif
#endif

static int Debug_Level_ = 0;

static inline int
DebugLevel(void) {		// test if debugging 
	return Debug_Level_ > 0 ? Debug_Level_ : 0;
}

static inline int
DebugSetLevel(int new_level) {	// set new, return old 
	int old_level = Debug_Level_;
	Debug_Level_ = new_level;
	return old_level;
}

static inline int
DebugSetOn(void) {		// turn on, return level 
	if ( Debug_Level_ <= 0 )
		DebugSetLevel(Debug_Level_ ? -Debug_Level_ : 1);
	return Debug_Level_;
}

static inline int
DebugSetOff(void) {			// turn off, return level 
	if ( Debug_Level_ > 0 )
		DebugSetLevel( -Debug_Level_ );
	return Debug_Level_;
}

#endif
