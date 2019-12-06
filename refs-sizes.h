/*
 * Header

	Wicci Project
	PostgreSQL Server-side C Code Header File
	Ref Sizes

 ** Copyright

	Copyright (c) 2005,2006 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.
 *
 */

#ifndef REF_SIZES_H
#define REF_SIZES_H

#include <limits.h>

// * TAG and ID Macros and types

#ifndef __WORDSIZE
# if UINT_MAX == 65535U
#  define __WORDSIZE	16
# elif UINT_MAX == 4294967295U
#   define __WORDSIZE	32
# elif UINT_MAX == 18446744073709551615UL
#   define __WORDSIZE	64
# endif
#endif

/* static_assert(__WORDSIZE >= 32); !!!  */

#if __WORDSIZE == 32
#define REF_TAG_WIDTH 8
#elif  __WORDSIZE >= 48
#define REF_TAG_WIDTH 16
#elif  __WORDSIZE >= 80
#define REF_TAG_WIDTH 32
#endif

#define REF_TAG_OID INT4OID
#if __WORDSIZE >= 64
	#define REF_OID INT8OID
	#define REF_INFIX(pre, post) pre##64##post
	#define REF_ID_OID INT8OID
	#define ID_INFIX(pre, post) pre##64##post
#else
	#define REF_OID INT4OID
	#define REF_INFIX(pre, post) pre##32##post
	#define REF_ID_OID INT4OID
	#define ID_INFIX(pre, post) pre##32##post
#endif

#define REF_SUFFIX(root) REF_INFIX(root,)
#define ID_SUFFIX(root) ID_INFIX(root,)

#define GetArgTag(arg) PG_GETARG_INT32(arg)
#define TagGetDatum(tag) Int32GetDatum(tag)
#define RowColTagType(r, c, t, n) RowColInt32(CALL_  (r), (c), (t), (n))
#define RowColTag(r, c, t, n) RowColTypInt32(CALL_  (r), (c), (t), (n))
#define RowColTag0(r, c, t) RowColIfInt32(CALL_  (r), (c), (t), 0)
#define QueryTagType(p, a, t, n) SpxQueryTypeInt32(CALL_  (p), (a), (t), (n))
#define QueryTag(p, a, t, n) SpxQueryInt32(CALL_  (p), (a), (t), (n))
#define QueryTag0(p, a, t) SpxQueryIfInt32(CALL_  (p), (a), (t), 0)

#define GetArgId(arg) ID_SUFFIX(PG_GETARG_INT)(arg)
#define IdGetDatum(id) ID_INFIX(Int,GetDatum)(id)
#define RowColIdType(r,c,t,n) ID_SUFFIX(RowColInt)(CALL_ (r),(c),(t),(n))
#define RowColId(r,c,t,n) ID_SUFFIX(RowColTypInt)(CALL_ (r),(c),(t),(n))
#define RowColId0(r,c,t) ID_SUFFIX(RowColIfInt)(CALL_ (r),(c),(t),0)
#define QueryIdType(p,a,t,n) ID_SUFFIX(SpxQueryTypeInt)(CALL_ (p),(a),(t),(n))
#define QueryId(p,a,t,n) ID_SUFFIX(SpxQueryInt)(CALL_ (p),(a),(t),(n))
#define QueryId0(p,a,t) ID_SUFFIX(SpxQueryIfInt)(CALL_ (p),(a),(t),0)

#define UpdateIdType(p,a,t,n) ID_SUFFIX(SpxUpdateTypeInt)(CALL_ (p),(a),(t),(n))
#define UpdateId(p,a,t,n) ID_SUFFIX(SpxUpdateInt)(CALL_ (p),(a),(t),(n))
#define UpdateId0(p,a,t) ID_SUFFIX(SpxUpdateIfInt)(CALL_ (p),(a),(t),0)

#define GetArgRef(arg) REF_SUFFIX(PG_GETARG_INT)(arg)
#define DatumGetRef(x) REF_SUFFIX(DatumGetInt)(x)
#define RefGetDatum(ref) REF_INFIX(Int,GetDatum)(ref)
#define RowColRefType(r,c,t,n) REF_SUFFIX(RowColInt)(CALL_ (r),(c),(t),(n))
#define RowColRef(r,c,t,n) REF_SUFFIX(RowColTypInt)(CALL_ (r),(c),(t),(n))
#define QueryRefType(p,a,t,n) REF_SUFFIX(SpxQueryTypeInt)(CALL_ (p),(a),(t),(n))
#define QueryRef(p,a,t,n) REF_SUFFIX(SpxQueryInt)(CALL_ (p),(a),(t),(n))

#define UpdateRefType(p,a,t,n) REF_SUFFIX(SpxUpdateTypeInt)(CALL_ (p),(a),(t),(n))
#define UpdateRef(p,a,t,n) REF_SUFFIX(SpxUpdateInt)(CALL_ (p),(a),(t),(n))

#if __WORDSIZE >= 64
#define REF_MAX_ID ( (long long) (LONG_MAX >> REF_TAG_WIDTH) )
#else
#define REF_MAX_ID ( (long) (INT_MAX >> REF_TAG_WIDTH) )
#endif
#define REF_MIN_ID ( -REF_MAX_ID )

#define REF_MIN_TAG	1
#define REF_MAX_POSSIBLE_TAG ( (1U << REF_TAG_WIDTH) - 1)

#define REF_MAX_TAG (REF_MAX_POSSIBLE_TAG - 1)

typedef unsigned ref_tags;	// a tag mapping to a database relation 
#define REF_TAG_VAL(x) (x)
#define REF_TAG_FMT__ "%u"
#define REF_TAG_FMT_ FMT_lf_(tag,REF_TAG_FMT__)
#define REF_TAG_FMT FMT_LF(tag,REF_TAG_FMT__)
#if __WORDSIZE >= 64
	typedef long long refs;	// a tag plus an ID 
	#define REF_INT_FMT__ "%Ld"
	typedef long long ref_ids;	// the ID of a tuple of a database relation 
	#define REF_ID_FMT__ "%Ld"
#else
	typedef long refs;	// a tag plus an ID 
	#define REF_INT_FMT__ "%ld"
	typedef long ref_ids;	// the ID of a tuple of a database relation 
	#define REF_ID_FMT__ "%ld"
#endif


#define REF_INT_FMT_ FMT_LF_(ref, REF_INT_FMT__)
#define REF_INT_FMT FMT_LF(ref, REF_INT_FMT__)

#define REF_ID_FMT_ FMT_lf_(id, REF_ID_FMT__)
#define REF_ID_FMT FMT_LF(id, REF_ID_FMT__)

#define REF_FMT_ FMT_LF2_(ref, REF_TAG_FMT_, REF_ID_FMT_)
#define REF_FMT FMT_LF2(ref, REF_TAG_FMT_, REF_ID_FMT_)
#define REF_VAL(ref) RefTag(ref), RefId(ref)

#endif
