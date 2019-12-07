/*
 * Header

	Wicci Project C Code Header
	Utility Code for Spx Debugging and Initialization Management

 ** Copyright

	Copyright (c) 2005-2019 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.

 * Dependencies:
	PostgreSQL unless you define macro C_DEBUG_NO_PG
 */

#ifndef C_DEBUG_SPX_H
#define C_DEBUG_SPX_H

#include "debug-log.h"

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
