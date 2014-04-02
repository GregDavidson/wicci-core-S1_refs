static const char spx_version[] = "$Id: spx-test.c,v 1.1 2006/06/28 01:34:16 greg Exp greg $";
/*
 * Header

  Wicci Project C Code
  Enhancement to PostgreSQL Server Programming Interface
  Module Unit Test Code

 ** Copyright

  Copyright (c) 2005,2006 J. Greg Davidson.
  You may use this software under the terms of the
  GNU AFFERO GENERAL PUBLIC LICENSE
  as specified in the file LICENSE.md included with this distribution.
  All other use requires my permission in writing.
 *
 */

#include "spx.h"
#define MODULE_TAG(name) spx_test##name
#include "debug.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

FUNCTION_DEFINE(spx_test_planned_queries) {
  CALL_BASE();
  const int level = StartSPX(_CALL_);
  static SpxPlans plan;
  if ( SpxPlanNull(plan) )
    plan = SpxPlanQuery(CALL_ "select 'hello world!'::text", 0, 0);
    //  Datum result = SpxQueryDatum(CALL_ plan, 0, 0);
  SpxText result = SpxQueryText(CALL_ plan, 0, 0);
  DEBUG_OUT( "spx_test_1: returned from SpxQueryDatum");
  FinishSPX(CALL_ level);
  DEBUG_OUT( "spx_test_1: returned from FinishSPX");
  //  PG_RETURN_DATUM(result);
  PG_RETURN_TEXT_P(result.varchar);
}

#if 0	// see pgsql/include/server/fmgr.h

typedef struct FmgrInfo {    // info from system catalog for fmgr call
  PGFunction fn_addr; // pointer to function or handler to be called
  Oid fn_oid;	      // OID of function (NOT of handler, if any)
  short fn_nargs;     // 0..FUNC_MAX_ARGS, or -1 if variable arg count
  bool fn_strict;     // function is "strict" (NULL in => NULL out)
  bool fn_retset;     // function returns a set
  unsigned char fn_stats;   // collect stats if track_functions > this
  void *fn_extra;	    // extra space for use by handler
  MemoryContext fn_mcxt;    // memory context to store fn_extra in
  fmNodePtr fn_expr;	    // expression parse tree for call, or NULL
} FmgrInfo;

typedef struct FunctionCallInfoData {
  FmgrInfo  *flinfo;	// ptr to lookup info used for this call
  fmNodePtr context;	// pass info about context of call
  fmNodePtr resultinfo;	// pass or return extra info about result
  bool isnull;		// function must set true if result is NULL
  short nargs;		// # arguments actually passed
  Datum arg[FUNC_MAX_ARGS];	// Arguments passed to function
  bool argnull[FUNC_MAX_ARGS];	// T if arg[i] is actually NULL
} FunctionCallInfoData, *FunctionCallInfo;

#define PG_FUNCTION_ARGS	FunctionCallInfo fcinfo

#endif

FUNCTION_DEFINE(spx_function_oid) {
  PG_RETURN_INT32(fcinfo->flinfo->fn_oid);
}

FUNCTION_DEFINE(spx_function_nargs) {
  PG_RETURN_INT32(fcinfo->flinfo->fn_nargs);
}
