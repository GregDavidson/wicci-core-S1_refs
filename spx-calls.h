/*
 * Header

  Wicci Project C Code Header
  Enhancement to PostgreSQL Server Programming Interface
  Call Chain System for Procedure Context and SPX State Management

 ** Copyright

  Copyright (c) 2005,2006 J. Greg Davidson.
  You may use this software under the terms of the
  GNU AFFERO GENERAL PUBLIC LICENSE
  as specified in the file LICENSE.md included with this distribution.
  All other use requires my permission in writing.
 *
 */

#ifndef C_SPX_CALLS_H
#define C_SPX_CALLS_H

#include <postgres.h>
#include "str.h"
#include "debug-log.h"

// * call_chain system

// ** call_chain system constructors

#if 0
// #ifdef _NO_CALL_CHAINS_

/* We currently need call chains to be able to tell if we're
in an SPI context, e.g. in CallAlloc and TmpAlloc.  Until we
find a workaround for that we have to keep call chains!!
*/

/* _NO_CALL_CHAINS_ had better not be defined until we
 (1)	finish coming up with do-nothing variants of all
	of the stuff below
 (2)	finish converting the do-something variants to
 	use the provided macros for the names of things
 (3)	make sure aplication code only uses the provided
	macros to do declare or use call chains
	- in particular, check JoinCallAccum(), JoinCalls()
	- and any occurrance of _CALL_-> or !_CALL_
*/

#define _CALLS_	void		// single parameter declaration
#define _CALL_				// pass calls as single arg

#define CALLS_				// first parameter declaration
#define CALL_				// call chain passed to us

#else

// for functions taking no other arguments:
#define _CALLS_	Calls call_link_
#define _CALL_		call_chain_

// for functions taking other arguments, pass calls first:
#define CALLS_		_CALLS_,
#define CALL_		_CALL_,

#endif

// ** call_chain system type definitions

typedef const struct call_chain *const Calls;

typedef struct call_state {
  int connected;		// SPI_connected?
} * const CallState;

/* A call_chain is a linked_list of function names
 * representing a chain of function calls.  It is used
 * to provide the function call context in the case of
 * an error or log message.
 */
typedef const struct call_chain *const CallChain;
struct call_chain {
  const char *const fname;
  CallChain parent;
  CallState state;
};

/* Some of the following macros use the GNU C extensions:
 *  ({ Statement Expressions })
 * , ## in variadic macros
 */

// Next two not for direct use

#if 0
#define CALL_LINK_BASE(link, base)					\
	struct call_chain call_node_ =					\
		( (struct call_chain) { __func__, link, base } ),	\
	*const _CALL_ = &call_node_
#else
#define CALL_LINK_BASE(link, base)					\
	struct call_chain call_node_ =					\
		( (struct call_chain) { __func__, link, base } ),	\
	*const _CALL_ = &call_node_;				\
	if ( DebugLevel() > 2 )  CALL_DEBUG_OUT("+")
#endif

#define CALL_LINK_CALLS(calls) CALL_LINK_BASE(calls, calls->state)

// One of next two at top of all fallible registered functions:

#define CALL_BASE()                                             		\
  struct call_state call_base = (struct call_state){false};	\
  CALL_LINK_BASE(0, &call_base)

#if 1
// can this be improved ???
#define CALL_CREFS(crefs)                                       		\
	struct call_state call_base = (struct call_state){false};	\
	CALL_LINK_BASE( crefs ? crefs->calls : 0, &call_base )
#else
// can this be improved ???
#define CALL_CREFS(crefs)                                       		\
	struct call_state call_base = (struct call_state){false};	\
	CALL_LINK_BASE(                                               	\
		crefs ? crefs->calls : 0,                      		\
		crefs? crefs->calls->state : &call_base		\
	)
#endif

// Use this at top of all fallible internal functions:
#define CALL_LINK() CALL_LINK_CALLS(call_link_)
// Use this at top of all lightweight internal functions:
#define CALLS_LINK() CallChain _CALL_ = call_link_

// ** call_chain system getters

TmpStrPtr JoinCalls(_CALLS_);  // join graph into a list 

#if 0
#define JOIN_CALL(call) ( JoinCalls( (_CALL_) ) )
#define CALL(calls) (										\
	(struct call_chain) { __func__, (calls), (calls) ? (calls)->state : 0 }	\
)
#define JOIN_CALLS(calls) ( JoinCalls( &CALL(_CALL_) ) )
#endif

// ** call_chain assertion macros

// Which of these could be Inline Functions??

#define AssertBy_(calls, bool_exp) AssertExpFunc(bool_exp, JoinCalls(calls))
#define AssertByMsg_( calls, bool_exp, ... )						\
	AssertExpFuncMsg(bool_exp, JoinCalls(calls), ## __VA_ARGS__)

#define CallAssert(bool_exp) AssertBy_(_CALL_, bool_exp)
#define CallAssertMsg(bool_exp, ... )							\
	AssertByMsg_(_CALL_, bool_exp, ## __VA_ARGS__)

// * initialization and SPI connection management

static inline int StackedSPX(_CALLS_) { CALLS_LINK(); return _CALL_->state->connected; }
static inline bool InSPX(_CALLS_) { CALLS_LINK(); return StackedSPX(_CALL_) > 0; }

static inline void AssertSPX(_CALLS_) { CALLS_LINK(); CallAssert( InSPX(_CALL_) ); }
static inline void AssertNotSPX(_CALLS_) { CALLS_LINK(); CallAssert( ! InSPX(_CALL_) );}

static inline int IncSPX(_CALLS_) { CALLS_LINK(); return ++_CALL_->state->connected; }
static inline int DecSPX(_CALLS_) {
  CALLS_LINK();
  AssertSPX(_CALL_);
  return --_CALL_->state->connected;
}

// ** More call_chain assertion convenience forms


#define CALL_WARN_OUT(format, ...)							\
	WARN_OUT("%s:\n\t" format, JoinCalls(_CALL_), ## __VA_ARGS__)

#if 0
#define CALLS_WARN_OUT(format, ...)							\
	WARN_OUT("%s: " format, JOIN_CALLS(_CALL_), ## __VA_ARGS__)
#endif

#define CALL_BUG_OUT(format, ...)							\
	BUG_OUT("%s:\n\t" format, JoinCalls(_CALL_), ## __VA_ARGS__)

#if 0
#define CALLS_BUG_OUT(format, ...)							\
	BUG_OUT("%s: " format, JOIN_CALLS(_CALL_), ## __VA_ARGS__)
#endif
		
#define CALL_DEBUG_OUT(format, ...)							\
	DEBUG_OUT("%s:\n\t" format, JoinCalls(_CALL_), ## __VA_ARGS__)

 #if 0
#define CALLS_DEBUG_OUT(format, ...)						\
	DEBUG_OUT("%s: " format, JOIN_CALLS(_CALL_), ## __VA_ARGS__)
#endif
		
#endif
