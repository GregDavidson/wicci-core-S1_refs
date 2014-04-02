/*
 * Header

	Wicci Project C Code Header
	Utility Code for Debugging Log

 ** Copyright

	Copyright (c) 2005,2006 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.

 * Dependencies:
	PostgreSQL unless you define macro C_DEBUG_NO_PG
	JoinCalls needed for CallAssert

 */

#ifndef C_DEBUG_LOG_H
#define C_DEBUG_LOG_H

#include <stdio.h>

extern FILE *debug_log_;

#define debug_out (debug_log_ ? debug_log_ : stderr)

static inline void debug_log_close(void) {
	if (debug_log_) fclose(debug_log_);
}

static inline FILE * debug_log_set(FILE *log) {
	debug_log_close();
	return debug_log_ = log;
}

static inline FILE *debug_log_open(const char *filename) {
	return debug_log_set( fopen(filename, "a") );
}

/* Some of the following macros use the GNU C extensions:
 *  ({ Statement Expressions })
 * , ## in variadic macros
 */

#ifdef C_DEBUG_NO_PG
#define EREPORT_LEVEL_MSG(level, ...)
#else
#define EREPORT_LEVEL_MSG(level, ...)				\
	ereport(level, ( errmsg( __VA_ARGS__ ) ) )
#endif

#define MSG_OUT(level, ...) ({         					\
	fprintf(debug_out, #level " " __VA_ARGS__);			\
	putc('\n', debug_out);							\
	fflush(debug_out);							\
	EREPORT_LEVEL_MSG(level, __VA_ARGS__ );	\
})

#define DEBUG_OUT(...) ({					\
			if ( DebugLevel() )						\
	MSG_OUT(NOTICE, __VA_ARGS__);		\
})

#define WARN_OUT(...) MSG_OUT(WARNING, __VA_ARGS__)

#define BUG_OUT(...) MSG_OUT(ERROR, __VA_ARGS__)

// "File %s, function %s, line %d: assertion %s failed!"

#define AssertExpFuncMore( bool_exp, func, more, ... ) ({	\
	if ( ! (bool_exp) ) BUG_OUT(						\
		"%s:%d:\n\tExpected %s in %s" more,				\
		__FILE__, __LINE__, #bool_exp, func, ## __VA_ARGS__	\
	);	  										\
})

#define AssertExpFunc(bool_exp, func)  AssertExpFuncMore(bool_exp, func, "!")
#define AssertExpFuncMsg( bool_exp, func, msg, ... ) 	\
 	AssertExpFuncMore(bool_exp, func, ", " msg, ## __VA_ARGS__)

#define AssertThat(bool_exp) AssertExpFunc(bool_exp, __func__)
#define AssertThatMsg( bool_exp, ... )			\
	AssertExpFuncMsg(bool_exp, __func__, ## __VA_ARGS__)

#endif
