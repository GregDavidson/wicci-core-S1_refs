/* * Wicci Project Object References C Code Header

	Wicci Project
	PostgreSQL Server-side C Code Header File
	Typed/Tagged Object References

 ** Copyright

	Copyright (c) 2005 - 2019 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.
 *
 */

/* * Include Files */

#ifndef REFS_H
#define REFS_H

#include "spx.h"
#include "refs-sizes.h"

#include "this-tag.h"
#define THIS_TAG REF
#define ThisTag Ref
#define this_tag ref

/* * Method management */

/* ** methods */

typedef const struct typed_object_method {
	ref_tags tag;
	SpxProcs operation;
	SpxProcs method;
	SpxPlans plan;	// calls method with operation args
} *Toms;

typedef const struct tom_cache *RefTomCache;
// variable-sized structure
struct tom_cache {
	//	struct spx_ref_count ref_count;	// must be 1st field!!
	struct spx_caches spx_caches;
	int size;
	struct typed_object_method tom[0];
};

void RefTomCacheCheck(CALLS_ RefTomCache cache);

/* * Macros and Functions */

/* ** Method Management */

/* Load all of the Toms into new storage.  If there were any
	 previously loaded descriptions, transfer over any of the query
	 plans which are still valid and free the rest, then free the old
	 storage.
 */
int RefLoadToms(_CALLS_);	// returns operation count

/* Lookup and return the appropriate method for a given operation and
	 ref_tag.  Create the query plan if necessary.  Return NULL if there
	 are any problems.
 */
Toms MethodForOpTag(CALLS_ SpxProcs operation, ref_tags tag);

Datum CallMethod(
	CALLS_ Toms tom, Datum args[], bool *null_ret
);

FUNCTION_DECLARE(refs_base_init);	 // void -> cstring
FUNCTION_DECLARE(refs_load_toms); // void --> INT32
FUNCTION_DECLARE(refs_debug_toms); // void --> INT32
FUNCTION_DECLARE(ref_op_tag_to_method);  // regprocedure, ref_tags -> regprocedure?

static inline int RefsInitialized(void) {
	extern int refs_Initialized_; // utl-debug.h: MODULE_TAG(Initialized_)
	return refs_Initialized_;
}

static inline void RefsRequired(_CALLS_) {
	CALLS_LINK();
	SpxRequired(_CALL_);
	CallAssert(RefsInitialized());
}

/* * type crefs stuff */

/* C References (CRefs) are references to arbitrary C data structures
	 which can be passed through PostgreSQL but are opaque to it.

	 The crefs framework should be moved to Spx!!
*/

typedef const struct crefs * CRefs;

#define GetArgCrefs(i)  ( (CRefs) ID_SUFFIX(PG_GETARG_INT)(i) )
#define CrefsGetDatum(d) ID_INFIX(Int,GetDatum)(d)

FUNCTION_DECLARE(crefs_nil);		// () -> crefs

FUNCTION_DECLARE(crefs_in);		// cstring -> crefs
FUNCTION_DECLARE(crefs_out);		// crefs -> cstring

FUNCTION_DECLARE(crefs_calls);		// crefs -> text
// FUNCTION_DECLARE(crefs_env);		// crefs -> ctext[]
FUNCTION_DECLARE(crefs_node);		// crefs -> ref (doc_node_refs)
FUNCTION_DECLARE(crefs_parent);	// crefs -> ref (doc_node_refs)
FUNCTION_DECLARE(try_crefs_doc);	// crefs -> ref (doc_refs)
FUNCTION_DECLARE(crefs_indent);	// crefs -> integer
FUNCTION_DECLARE(ref_crefs_graft);	// ref, crefs -> refs

#define CREFS_FMT__ "%p"
#define CREFS_FMT SPX_FMT(crefs, CREFS_FMT__)
#define CREFS_VAL(crefs) (void *) (crefs)

#if 0
enum crefs_tags {
	first_crefs_tag = 0,
	after_crefs_tags
};
#define LAST_CREFS_TAG after_crefs_tags
#endif


/* * type ref stuff */

/* A Typed Object Reference (REF) is a one-word value
	 consisting of a type-tag and a row id.  The type-tag
	 is associated with a PostgreSQL domain and table.
 */

/* **  Low level Utility Functions: */

FUNCTION_DECLARE(refs_null);	// void ->  NULL

FUNCTION_DECLARE(refs_tag_width);	// void ->  integer

FUNCTION_DECLARE(refs_min_tag);	// void -> ref_tags
FUNCTION_DECLARE(refs_max_tag);	// void -> ref_tags

FUNCTION_DECLARE(refs_min_id);	// void -> ref_ids
FUNCTION_DECLARE(refs_max_id);	// void -> ref_ids

/* ** ref (de)construction: */

FUNCTION_DECLARE(ref_tag);		// ref -> ref_tags
FUNCTION_DECLARE(ref_id);		// ref -> ref_ids
FUNCTION_DECLARE(unchecked_ref_from_tag_id);	// ref_tags, ref_ids -> ref

/* ** Fundamental ref operations: */

/* *** SQL API: */

FUNCTION_DECLARE(call_in_method);	// (cstring) -> ref
/* Given an XML description of a desired object,
	 Returns a reference to such an object if possible, NULL otherwise.
	 The description will be matched to existing objects where possible,
	 otherwise new objects will be created with the described values
	 and associations.
	 Objects referenced only by class and ID or TAG and ID must exist.
	 Object referenced by ID and value will first try to find the
	 object by ID and value, ignoring env (eq-method), then will
	 try to find the object by value but with a NULL enviroment.
	 Operates as a transaction, so if it fails, nothing is created.
	 Dispatches object creation to appropriate class factories (get-methods).
*/

FUNCTION_DECLARE(call_out_method);	// ref -> cstring
// Should this convert all types to xml?
// Should this convert context-dependent types to xml?
// Right now, each type decides for itself!

/* * op method dispatch */

/* ** op method dispatch without wicci magic */

FUNCTION_DECLARE(call_text_method); // ref, env, crefs, ... -> Text
FUNCTION_DECLARE(call_scalar_method); // ref, env, crefs, ... -> Scalar Value

/* ** op method dispatch with wicci magic */

FUNCTION_DECLARE(ref_env_crefs_etc_text_op); // ref, env, crefs, ... -> Text
FUNCTION_DECLARE(ref_env_crefs_etc_scalar_op); // ref, env, crefs, ... -> Scalar Value

/* ** Calling an operation with altered crefs */

// oftd !!!
// (regprocedure,from ref[],to ref[],ref, ref,ref_env,crefs ...)-->text
FUNCTION_DECLARE(oftd_ref_env_crefs_etc_text_op);

// oftd !!!
// (regprocedure,from ref[],to ref[],ref, ref,ref_env,crefs ...)-->scalar
FUNCTION_DECLARE(oftd_ref_env_crefs_etc_scalar_op);

/* ** ref btree index comparison function */

FUNCTION_DECLARE(ref_cmp);	// (ref, ref) -> (-1, 0, +1)
/* Used for btree indexes.
	 In theory, hash indexes make more sense, but
	 (1) Until 8.4 hash indexes were inefficient
	 (2) btree indexes require lots of boiler-plate,
	 even though this one function is doing all the work!
	 (3) Switching to hash indexes might be better+++
*/

/* ** Utility Functions and macros: */

/* Convert a strange ref to a string, including the
 * calling function context and perhaps a message
 */
size_t RefReportLen(Calls call, refs ref);
StrPtr RefReport(Calls call, refs ref, ALLOCATOR_PTR(alloc) );
size_t RefReportMsgLen(Calls call, refs ref, Str msg);
StrPtr RefReportMsg(Calls call, refs ref, Str msg, ALLOCATOR_PTR(alloc) );

static inline refs RefTagId (ref_tags tag, ref_ids id) {
//  AssertThat(id >= REF_MIN_ID && id <= REF_MAX_ID );
//  AssertThat(tag <= REF_MAX_TAG);
	return (id << REF_TAG_WIDTH) | tag;
}

static inline ref_tags RefTag(refs v) {
	return v & ~(~0U <<  REF_TAG_WIDTH);
}

static inline ref_ids RefId(refs v) {
	return v >> REF_TAG_WIDTH;
}

static inline bool RefIsNil(refs v) {
	return v == 0 || RefTag(v) == 0 ||  RefId(v) == 0;
}

static inline refs RefTagNil(ref_tags tag) {
	return RefTagId(tag, 0);
}

static inline refs RefNil(void) {
	return RefTagNil(0);
}

/* ** tag macros, enums and declarations */

/* Tag enum identifiers have the pattern name##s_tag
 * Tag functions identifiers have the pattern  name##_tag
 * Tag functions return value of corresponding tag enumeration
 */

#define TAG_DECLARE(name)			\
	FUNCTION_DECLARE(name##_tag);

#define TAG_DEFINE(name)				\
	FUNCTION_DEFINE(name##_tag) {		\
		 return TagGetDatum(name##s_tag);	\
	}

#define C_TAG_ISA_(Name) Isa##Name##Tag
#define SQL_TAG_ISA_(name) isa_##name##_tag
#define C_REF_ISA_(Name) Isa##Name
#define SQL_REF_ISA_(name) isa_##name

#define TAG_RANGE(name, Name, first, after)			\
	inline static bool C_TAG_ISA_(Name)( ref_tags tag) {	\
		return  tag >= first && tag < after;			\
	}											\
	inline static bool C_REF_ISA_(Name)( refs tobj) {		\
		return C_TAG_ISA_(Name)(RefTag(tobj) );		\
	}											\
	FUNCTION_DECLARE(SQL_TAG_ISA_(name));			\
	FUNCTION_DECLARE(SQL_REF_ISA_(name));

#define DEF_TAG_PREDICATE(predicate, Predicate)	\
	FUNCTION_DEFINE(predicate) {					\
		PG_RETURN_BOOL( Predicate(GetArgTag(0)) );		\
}

#define DEF_REF_PREDICATE(predicate, Predicate)	\
	FUNCTION_DEFINE(predicate) {					\
		PG_RETURN_BOOL( Predicate(GetArgRef(0)) );		\
}

#define DEF_RANGE_PREDS(name, Name)			\
	DEF_TAG_PREDICATE(SQL_TAG_ISA_(name), C_TAG_ISA_(Name))	\
	DEF_REF_PREDICATE(SQL_REF_ISA_(name), C_REF_ISA_(Name))

/* * ref tags */

/* ** enum ref_tags */

/* It is best if tags of classes which share operations
	 are contiguous.  Ideally the tree of Ref types would
	 be reflected in nested ranges of enumerators.  After
	 the core stabilizes, the non-texty classes of ref_env
	 and array_ref shoiuld be moved here so as to come
	 before ref_names_tag.
 */

enum ref_base_tags {
	first_ref_tag = 0,
	ref_nils_tag=first_ref_tag,  	// function ref_nil()
	after_ref_base_tags
};

// moved here from RefName/ref_name.h
enum name_ref_tags {	
	first_name_ref_tag=after_ref_base_tags,
	ref_names_tag=first_name_ref_tag,	// type name_ref
	after_name_ref_tags
};

// moved here from RefEnv/ref_env.h
enum ref_env_tags {
	first_env_tag=after_name_ref_tags,
	ref_envs_tag=first_env_tag,	// type ref_env
	after_env_tags
};

enum ref1_tags {
	after_ref1_tags = after_env_tags
};

#define LAST_REF_TAG after_env_tags

/* ** TAG_DECLAREs */

TAG_DECLARE(ref_nil)						// TAG --> DATUM

/* ** TAG_RANGEs */

/* ** SpxTypeOids declarations */

// pgsql type oids corresponding to key Ref types
static inline SpxTypeOids RefTagIntType() { return Int32_Type; }
static inline SpxTypeOids RefIdIntType() { return ID_INFIX(Int,_Type); }
static inline SpxTypeOids RefIntType() { return REF_INFIX(Int,_Type); }

// consider this change:
#if 0
extern SpxTypes CRefs_Type, Refs_Type;
#else
extern SpxTypeOids CRefs_Type;
extern SpxTypeOids Ref_Tags_Type;
extern SpxTypeOids Refs_Type;
#endif


/* * Obsolete or postponable? */

#if 0

/* It seems that I don't really need any of the class
	 structures for the immediate purposes of the Wicci;
	 I'll leave the basic structure in case it winds up
	 being useful, but don't trust that any of this works!
 */

/* ** tom_dispatch */

/*
	The TomsArray is sorted by (operation, tag).

	The TomsOpsArray indexes the Toms array elements
	where new operations begin, plus one extra element pointing
	beyond, allow pairs of TomsOpsArray elements to delimit the
	slices of the TomsArray with the same operation.

	The TomsDispatchArray manages the slices of the
	TomsOpsArray which correspond to the operations
	of the Tocs.  Therefore the TomsDispatchArray is
	sorted by (tag, operation).

	The TocsArray is dense and indexed by tag.
 */

/* An operation function may have a static pointer to one of these;
	 if so, there needs to be a back-pointer to allow us to null-out
	 or update that static pointer should the Toms be reloaded.
	 There needs to be a TomDispatchArray
 */
typedef const struct tom_dispatch  *TomDispatch;
struct tom_dispatch {
	Toms *slice;			// slice[0] ... slice[1]
	TomDispatch ptr;		// pts to static member of op function
	// Could add a strategy enum to allow indexing
	// instead of binary search when the tags are dense.
};
#endif

/* * classes */

typedef const struct typed_object_class {
	ref_tags tag;
#if 0
	ClassOids table;		// ClassOids is history
	SpxTables table;		// SpxTables is a possible future
#else
	Oid table;
#endif
	SpxTypes type;
	SpxProcs out;	    // (.type) -> cstring|text ; used by .type output function
	SpxPlans out_plan;
	SpxProcs in;	    // (cstring|text) -> .type ; used by .type input function
	SpxPlans in_plan;
	int num_ops;
#if 0
	TomDispatch *struct typed_object_method methods[0];
#endif
} *Tocs, **TocsPtr;

// when would size != max_tag + 1?
typedef const struct toc_cache *RefTocCache;
// variable-sized structure
struct toc_cache {
	//	struct spx_ref_count ref_count;	// must be 1st field!!
	struct spx_caches spx_caches;
	RefTomCache tom_cache;
	int size;
	ref_tags max_tag;
#if 1
	// this is the end of the fixed-length fields
	struct typed_object_class toc[0];
#else
	// this is what's real but C doesn't do variable array sizes in structures
	//	struct typed_object_class toc[max_tag + 1];
	struct typed_object_class toc[max_tag + 1];
	Tocs by_type_tag[size];		// sorted by (1) type oid, (1) tag
	Tocs by_class_type[size];		// sorted by (1) class oid, (2) type oid
#endif
};

// return pointer to first struct
static inline Tocs toc_cache_toc_start(RefTocCache cache) {
	return cache->toc; 
}

// return pointer just past last struct
static inline Tocs toc_cache_toc_end(RefTocCache cache) {
	return toc_cache_toc_start(cache) + cache->max_tag + 1;
}

// return pointer to first struct pointer
static inline TocsPtr toc_cache_by_type_tag_start(RefTocCache cache) {
	return (TocsPtr) toc_cache_toc_end(cache);
}

static inline TocsPtr toc_cache_by_type_tag_end(RefTocCache cache) {
	return toc_cache_by_type_tag_start(cache) + cache->size;
}

static inline TocsPtr toc_cache_by_class_type_start(RefTocCache cache) {
	return toc_cache_by_type_tag_end(cache);
}

static inline TocsPtr toc_cache_by_class_type_end(RefTocCache cache) {
	return toc_cache_by_class_type_start(cache) + cache->size;
}

static inline char *toc_cache_end(RefTocCache cache) {
	return (char *) toc_cache_by_class_type_end(cache);
}

FUNCTION_DECLARE(refs_load_tocs);	// void -> tocs count INT32
FUNCTION_DECLARE(refs_table_type_to_tag); // regclass, regtype -> ref_tags
FUNCTION_DECLARE(refs_tag_to_type);				// tag_arg --> reftype OID
FUNCTION_DECLARE(refs_type_to_tag);				// reftype --> tag_arg INT32
FUNCTION_DECLARE(refs_debug_tocs_by_tag); // void --> tocs_count INT32
FUNCTION_DECLARE(refs_debug_tocs_by_type); // void --> tocs_count INT32

void RefTocCacheCheck(CALLS_ RefTocCache cache);

#ifndef REFS_C
#include "last-tag.h"
#define LAST_TAG REF
#define LastTag Ref
#define last_tag ref
#endif

#endif
