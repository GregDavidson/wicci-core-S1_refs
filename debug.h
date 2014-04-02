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
#include "debug-log.h"

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

/* Define MODULE_TAG if you want a module-specific instance
	 of this debugging code.
*/

#ifndef MODULE_TAG
#define MODULE_TAG(x) x
#endif

int MODULE_TAG(Initialized_) = 0;	/*
	used by  those packages which care;
	see below for the functions on this variable
*/

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

#ifndef C_DEBUG_NO_PG

// FUNCTION_DEFINE is in spx.h

FUNCTION_DEFINE(MODULE_TAG(debug_level)) {	// ()  -> integer
	PG_RETURN_INT32( DebugLevel() );
}

FUNCTION_DEFINE(MODULE_TAG(debug_set)) {	// (integer) -> integer
	PG_RETURN_INT32( DebugSetLevel( PG_GETARG_INT32(0) ) );
}

FUNCTION_DEFINE(MODULE_TAG(debug)) {		// () -> boolean
	PG_RETURN_BOOL( DebugLevel() > 0 );
}

FUNCTION_DEFINE(MODULE_TAG(debug_on)) {		// () -> integer
	PG_RETURN_INT32( DebugSetOn( ) );
}

FUNCTION_DEFINE(MODULE_TAG(debug_off)) {	// () -> integer
	PG_RETURN_INT32( DebugSetOff( ) );
}

#endif

static inline int
Initialized(void) {		// is this package intialized? 
	return MODULE_TAG(Initialized_);
}

static inline void
Initialize(void) {		// record this package is initialized 
	 MODULE_TAG(Initialized_) = 1;
	 DEBUG_OUT( "Module Initialized" );
}

#endif
