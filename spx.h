/* * Wicci Project Spx C Code Header
	Enhancement to PostgreSQL Server Programming Interface

 ** Copyright

	Copyright (c) 2005-2019 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.

	TO DO: Add a num_args argument to all of the functions
	taking arrays of argument types, and check it!

	Every public symbol should begin with one of:
	SPX_, Spx, spx_, RowCol
 */

#ifndef SPX_H
#define SPX_H

/* * Include Files */

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
// general Postgres declarations:
#include <postgres.h>
	// GetAttributeByName(), SPI_freeplan, etc.:
#include <executor/spi.h>
#include <utils/array.h>
// Need cstring <-> text utilities
// also inclues fmgr.h and other goodies:
#include <utils/builtins.h>

#include "str.h"	   // typedefs clarify '\0' terminated strings
#include "array.h"	   // macros for C arrays
#include "fmt.h"	   // help for printing size_t

#include "spx-calls.h"		// call chains for debugging

#include "this-tag.h"
#define THIS_TAG SPX
#define ThisTag Spx
#define this_tag spx

// * misc early

// align on 64-bit boundary
static inline int spx_aligned_size(int size) {
	const int align = 8 - 1;
	return (size + align) & ~align;
}

/* * Reference Counted Dynamic Allocation Support */

// There must be NO cycles in reference paths!

// variable-size, allocated only by spx_obj_alloc(size_t)
typedef const struct spx_obj_header {
	int count;		// when 0, object is garbage
	struct spx_obj_header *prev, *next; // for TheWheel
	char *end_ptr;		     // end of the complete object
	struct spx_obj_header *the_wheel; // a list we're part of
	char object[0];
} *SpxObj;
typedef struct spx_obj_header *SpxObjHdr;

/* Allocates object of given size along with a struct
spx_obj_header attached to TheWheel, returning pointer to the
object.*/
extern void *spx_obj_alloc(size_t size);

// Circular d-list of spx_obj_header managed heap objects
extern struct spx_obj_header TheWheel;

/* **** Usage of spx_obj_header 
grep -nwB 1 spx_obj_header {spx,ref}.{h,c}
*/
/* **** Usage of count
 rg '[.>]count' spx.h spx.c refs.h refs.c
spx.h functions
- SpxObjRefIncr
- SpxObjRefDecr
*/
/* **** Usage of prev or next
 rg '[.>]end_ptr' spx.h spx.c refs.h refs.c
spx.c functions
- SpxObjDetach
- SpxTryFreeOne
- SpxTryFreeSome
 */
/* **** Usage of end_ptr
rg '[.>]end_ptr' spx.h spx.c refs.h refs.c
spx.h: 
- spx_obj_in
- spx_obj_end
- spx_obj_alloc
spx.c:
- LoadProcs (near the end)
 */

// Return pointer to header under given object
// Object MUST have been allocated with spx_obj_alloc
static inline SpxObjHdr spx_obj_hdr(CALLS_ void *spx_obj) {
	CALLS_LINK();
	SpxObjHdr hdr = (SpxObjHdr) spx_obj - 1;
	CallAssert(hdr->the_wheel == &TheWheel);
	return hdr;
}

static inline char *spx_obj_end(CALLS_ void *spx_obj) {
	CALLS_LINK();
	SpxObjHdr hdr = spx_obj_hdr(CALL_ spx_obj);
	return hdr->end_ptr;
}

static inline bool SpxObjPtr_Valid(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
	SpxObjHdr hdr = spx_obj_hdr(CALL_ spx_obj);
	return (char *) ptr >= hdr->object && (char *) ptr <= hdr->end_ptr;
}

static inline void Assert_SpxObjPtr_Valid(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
	CallAssert( SpxObjPtr_Valid(CALL_ spx_obj, ptr) );
}

static inline bool SpxObjPtr_In(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
	Assert_SpxObjPtr_Valid(CALL_ spx_obj, ptr);
	SpxObjHdr hdr = spx_obj_hdr(CALL_ spx_obj);
	return  (char *) ptr >= hdr->object && (char *) ptr < hdr->end_ptr;
}

static inline void Assert_SpxObjPtr_In(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
	CallAssert( SpxObjPtr_In(CALL_ spx_obj, ptr) );
}

static inline bool SpxObjPtr_AtEnd(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
	Assert_SpxObjPtr_Valid(CALL_ spx_obj, ptr);
	SpxObjHdr hdr = spx_obj_hdr(CALL_ spx_obj);
	return hdr->end_ptr == (char *) ptr;
}

static inline void Assert_SpxObjPtr_AtEnd(CALLS_ void *spx_obj, void *ptr) {
	CALLS_LINK();
#if 1		// extra feedback if it bombs!!
	SpxObjHdr hdr = spx_obj_hdr(CALL_ spx_obj);
	if ( hdr->end_ptr != (char *) ptr )
		CALL_DEBUG_OUT( "object end is %p but pointer is %p",
										hdr->end_ptr, (char *) ptr );
#else	 // this was failing on LoadTypes but above was not??
	CallAssert( SpxObjPtr_AtEnd(CALL_ spx_obj, ptr ) );
#endif
}

/* I'm not seeing initialization complete!  Usage:
rg -w spx_obj_alloc
  * spx.c
LoadSchemaPath:573:		spx_obj_alloc( sizeof *sp + num_rows*sizeof *sp->path);
LoadSchemas:620:	const SchemaCachePtr cache = spx_obj_alloc(
LoadTypes:814:	const TypeCachePtr cache = spx_obj_alloc(
LoadProcs:1175:	const ProcCachePtr cache = spx_obj_alloc(
 * refs.c
LoadToms:171:		spx_obj_alloc(sizeof *cache + num_rows * sizeof *cache->tom);
LoadTocs:642:		spx_obj_alloc( toc_cache_end(&toc) - (char *) &toc );
 *  Are we, in fact, actually using TheWheel??
 * I'm not finding any calls to SpxTryFreeOne or SpxTryFreeSome??
 */

// free p or add it to TheWheel
void SpxTryFreeOne(CALLS_ SpxObjHdr p);

// free any objects on TheWheel that are no longer in use
void SpxTryFreeSome(_CALLS_);

#if 0
// increment reference count of given object
static inline int SpxObjRefIncr(SpxObjHdr p) {
	return p && ++p->count;
}
#define SPX_OBJ_REF_INCR(struct_ptr) ({			\
			SpxObjRefIncr(spx_obj_hdr(CALL_ (void *)struct_ptr));	\
			struct_ptr;																		\
})
#endif

// decrement ref count of object's header, try to free it
static inline int SpxObjRefDecr(CALLS_ SpxObjHdr p) {
	CALLS_LINK();
	// need new_count in case we deallocate *p
	int new_count = p->count;
	if (p->count > 0) {
		--new_count;
		p->count = new_count;;
		if (new_count == 0)
			SpxTryFreeOne(CALL_ p);
	} else
		CALL_WARN_OUT("%p zero ref count", p);
	return new_count;
}

/* Given an L-Value that's either NULL or is pointing to
   an spx object managed with an spx_obj_header: if the
   pointer is NULL, do nothing and return 0;  otherwise,
   call SpxObjRefDecr and - if it returns 0, NULL out the
   L-Value.  (0 becomes NULL in a C pointer context)
*/
#define SPX_OBJ_REF_DECR(struct_ptr) ({	\
			__typeof__(struct_ptr) *pp = &(struct_ptr), p = *pp;	\
			*pp = p && SpxObjRefDecr(CALL_ spx_obj_hdr(CALL_ (void *)p)) ? p : 0 ;							\
})

/* * Schema Cache */

/* schema and search_path functionality
	 consider hiding behind a functional interface
 */

typedef int SchemaIds;				 // our assigned IDs
typedef Oid SchemaOids;				 // PostgreSQL's assigned OIDs

// variable-size
typedef const struct spx_schema {
	SchemaIds id;
	SchemaOids oid;
#ifndef _STRIP_META_
	char name[0];									// '\0'-terminated & alignment padded
#endif
} *SpxSchemas;

static inline bool SchemaNull(SpxSchemas s) {return !s||!s->oid;}

// return pointer to next variable-length spx_schema structure
static inline SpxSchemas spx_schema_next(SpxSchemas schema) {
#ifndef _STRIP_META_
	size_t name_size = spx_aligned_size(strlen(schema->name)+1);
	return (SpxSchemas) ((char *) (schema + 1) + name_size);
#else
	return schema + 1;
#endif
}

#ifndef _STRIP_META_

#define SPX_OID_FMT__ "%d"
#define SPX_OID_FMT_ FMT_LF_(oid, SPX_OID_FMT__)
#define SPX_OID_FMT FMT_LF(oid, SPX_OID_FMT__)
#define SPX_OID_VAL(x) (x)

#define SPX_SCHEMA_FMT_ FMT_LF3_(schema, "%s", FMT_LF_(id, "%d"), SPX_OID_FMT_)
#define SPX_SCHEMA_FMT FMT_LF3(schema, "%s", FMT_LF_(id, "%d"), SPX_OID_FMT_)
#define SPX_SCHEMA_VAL(schema)	\
	StrOrQ((schema).name), (schema).id, SPX_OID_VAL((schema).oid)

#else

#define SPX_SCHEMA_FMT_ FMT_LF2_(schema, FMT_LF_(id, "%d"), SPX_OID_FMT_)
#define SPX_SCHEMA_FMT FMT_LF2(schema, FMT_LF_(id, "%d"), SPX_OID_FMT_)
#define SPX_SCHEMA_VAL(schema) (schema).id, SPX_OID_VAL((schema).oid)

#endif

#define MAX_SCHEMAS 1000	// make plenty big!!

/* * struct spx_schema_cache */
	
// variable-siz
typedef const struct spx_schema_cache {
	int min_id, max_id;
	int size;
#if 1				// not really
	SpxSchemas by_id[0];
#else			// really:
	SpxSchemas by_id[max_id+1];
	SpxSchemas by_name[size];
	SpxSchemas by_oid[size];
	struct spx_schema variable_length_schemas[size];
#endif
}*SpxSchemaCache;

// returns &by_name[0]
static inline SpxSchemas *
spx_schema_cache_by_name(SpxSchemaCache cache) {
	return (SpxSchemas *) cache->by_id + cache->max_id + 1;
}

// returns start of by_oid array
static inline SpxSchemas *
spx_schema_cache_by_oid(SpxSchemaCache cache) {
	return spx_schema_cache_by_name(cache) + cache->size;
}

// returns start of schema arena
static inline SpxSchemas
spx_schema_cache_schemas(SpxSchemaCache cache) {
	return (SpxSchemas) (spx_schema_cache_by_oid(cache) + cache->size);
}

static inline SpxSchemas
SpxSchemaById(CALLS_ int id) {
	CALLS_LINK();
	extern SpxSchemaCache Spx_Schema_Cache;
	CallAssert(
		id >= Spx_Schema_Cache->min_id
		&& id <= Spx_Schema_Cache->max_id
	);
	SpxSchemas s = Spx_Schema_Cache->by_id[id];
	return SchemaNull(s) ? 0 : s;
}

// SpxSchemas SchemaById(int id);
SpxSchemas SpxSchemaByOid(CALLS_ SchemaOids oid);
SpxSchemas SpxSchemaByName(CALLS_ StrPtr name);

void SpxSchemaCacheCheck(CALLS_ SpxSchemaCache cache);

/* * struct spx_schema_path */

// variable-size
typedef const struct spx_schema_path {
	SpxSchemaCache schema_cache;
	int size;
	SpxSchemas path[0];
}*SpxSchemaPath;;

void SpxSchemaPathCheck(CALLS_ SpxSchemaPath path);

int SpxLoadSchemaPath(_CALLS_);

/* * Stored Query Plans */

/* Creating Stored Query Plans is expensive, not least
	 because creating multiple plans cannot be batched.
	 A lazy strategy is recommended.  Ideally there would
	 be a Reference Counted Stored Query Plans Cache
	 which would allow them to be shared.  A start towards
	 that is inside the ifdefs below.
 */

/*  The most common use of SpxPlans in the Wicci is to store the plan
		used by an operation to call the appropriate method given the type
		of the arguments to the operation.  Almost always the number of
		arguments to the method are fixed and are the same as those of the
		operation.  Occasionally, however, the number of arguments is not
		the same.  As a safety check we record the number of arguments
		expected by the query.  At a later time we plan to make SpxPlans
		be a first-class datatype which independently store more of their
		meta-data.
 */
// was a variable-sized structure
typedef struct {
	void *plan;
	int num_args;	// for safety check
#if 0
	Oid arg_type_oids[0];
#if 0				// if C were really smart!
	char sq[0]l;			// follows arg_type_oids[num_args]
#endif
#endif
} SpxPlans, *SpxPlanPtr;
static inline bool SpxPlanNull(SpxPlans p) { return !p.plan; }
static inline bool SpxPlanNargs(SpxPlans p) { return p.num_args; }

static inline SpxPlans SpxNoPlan(void) {
	static SpxPlans plan0;  return plan0;
}

// Place holders for fancier plan management
#define SPX_MOVE_PLAN(to, from) ({		\
	__typeof__(from) *fromp = &(from);	\
	to = *fromp;					\
	fromp->plan = 0;				\
})
#define SPX_FREE_PLAN(p) ({			\
	__typeof__(p) *pp = &(p);			\
	if (!SpxPlanNull(*pp))				\
		SPI_freeplan((p).plan);			\
	pp->plan = 0;					\
})

#define SPX_PLAN_FMT_ FMT_LF2_(plan, "%p", FMT_lf_(nargs," %d"))
#define SPX_PLAN_FMT FMT_LF2(plan, "%p", FMT_lf_(nargs," %d"))
#define SPX_PLAN_VAL(qplan) ((qplan).plan), ((qplan).num_args)

/* * Text Datatype */

/* Automating memory for extended postgres objects:
	 (1) Wrap each kind of varlena in a structure
	 (2) Along with info on its allocation persistence
	 (3) And inline functions to manage it
	 (4) When it's text or varchar, add a null byte
	 whenever we store it somewhere, so we can
	 treat it's data area as a C string.
 */

/* We need something like this as a tag.  We could add a few
	 special values (with negative integer values) to the connection
	 levels, but we need to invalidate any storage the moment
	 we leave a given connection level - hmmm!
*/

enum spx_allocs {
	spx_auto,		// allocated in this function's stack frame
	spx_static,		// allocated statically - no worries!
	spx_buffer,	// sitting in a pg buffer, text is not null terminated
	spx_connect,		// will persist for the rest of this connection
	spx_session		// will persist for the rest of this session
};

// from include/c.h: typedef struct varlena text;
typedef struct {
	struct varlena *varchar;
#if 0
	enum spx_text_allocs alloc;
#endif
} SpxText;

static inline SpxText SpxTextNull(void) {
	return (SpxText) {0};
}

static inline bool SpxTextIsNull(SpxText t) { return !t.varchar; }

#if 0

/* Macros and functions defined and declared in
 * include/utils/builtins.h
 * seem to make all of this obsolete!
 * PostgreSQL still needs to document a wider SPI API!
 */

static inline SpxText SpxArgText(PG_FUNCTION_ARGS /*fcinfo*/, int arg_no) {
	return (SpxText){PG_GETARG_TEXT_P(arg_no)};
}
#define SPX_ArgText(arg_no) SpxArgText(fcinfo, (arg_no))

static inline SpxText SpxDatumText(Datum d) {
	return (SpxText){DatumGetTextP(d)};
}

extern const SpxText Spx_Null_Text;

#define SPX_RETURN_Text(textdata) do {			\
	SpxText td = (textdata);					\
	if (td.varchar) PG_RETURN_TEXT_P(td.varchar);	\
	PG_RETURN_NULL();					\
} while (0)

#if 0
// Oh well, it isn't used anymore anyway!
// We have a hidden allocator call here!!!
// and was CallAlloc
#define SPX_RETURN_StrText(str)					\
	SPX_RETURN_Text( SpxStrText( CALL_ (str) , palloc ) )
#endif

/* type conversion */

#if 0
SpxText SpxStrText( CALLS_ StrPtr s, ALLOCATOR_PTR(allocator) );
StrPtr SpxTextStr( CALLS_ SpxText td, ALLOCATOR_PTR(allocator) );
#endif

#endif

/* * Types */

/* In order to make queries and check the datatypes returned we need
	 access to the PostgreSQL oids assigned to the types of some
	 datatypes.  In general we can look this up at runtime, but this is
	 expensive, so we provide a handy datastructure to cashe the
	 result.  Some of these oids which are known statically are declared
	 in this section.
 */

/* SpxTypeOids are designed for types which are used so much
	 that they should be cached in static variables.  Some of them
	 can be initialized statically from macros in PostgreSQL headers.
	 The others can be put in lists of required types and initialized
	 with the package which requires them.

	 See the fancier type
	SpxTypes
	 See the handy constructor functions:
	SpxTypeOids SpxTypeOid(SpxTypes)
	SpxTypeOids SpxMkTypeOid(Oid, Str)
 */

/* Why can't we just use struct spx_type for this??? */

typedef struct spx_type_oids {
	Oid type_oid;
#ifndef _STRIP_META_
	StrPtr type_name;
#endif
	struct spx_type_oids *required;	// one seems to be enough
} SpxTypeOids;
static inline bool TypeOidsNull(SpxTypeOids x) { return !x.type_oid; }

static inline SpxTypeOids SpxMkTypeOid(Oid o, Str s) {
	return (struct spx_type_oids){o, s};
}

#ifndef _STRIP_META_

#define SPX_TYPEOID_FMT_ FMT_LF2_(type, "%s", SPX_OID_FMT_)
#define SPX_TYPEOID_FMT FMT_LF2(type, "%s", SPX_OID_FMT_)
#define SPX_TYPEOID_VAL(type)	\
	StrOrQ((type).type_name), SPX_OID_VAL( (type).type_oid )

#else

#define SPX_TYPEOID_FMT_ FMT_LF_(typeoid, SPX_OID_FMT__)
#define SPX_TYPEOID_FMT FMT_LF(typeoid, SPX_OID_FMT__)
#define SPX_TYPEOID_VAL(type)	\
	SPX_OID_VAL( (type).type_oid )

#endif

// Some key PostgreSQL TypeOids from "server/catalog/see pg_type.h"
extern const SpxTypeOids
	Int32_Type,  Int32_Array_Type,  Int64_Type,
	Float_Type, Float_Array_Type, Double_Type,
	Bool_Type, Text_Type,  Text_Array_Type,  XML_Type,
	Procedure_Type,  Class_Type, Type_Type, Type_Array_Type,
	CString_Type, Oid_Type,  Oid_Vector_Type, Name_Type,
	Unknown_Type, End_Type;			// oid==0, used to end lists

// Ensure additional required types in list are (re-)initialized.
void SpxRequireTypes(CALLS_ SpxTypeOids *types, bool reinit);

// Here's the newer type code:

#define MAX_TYPES 10000	// make plenty big!
// variable-sized structure
typedef const struct spx_type {
	Oid oid;
	SpxSchemas schema;
	int len;
	bool by_value;
#ifndef _STRIP_META_
	char name[0];
#endif
} *SpxTypes;

static inline SpxTypeOids SpxTypeOid(SpxTypes t) {
	return SpxMkTypeOid(t->oid, t->name);
}

// return pointer to next variable-length spx_type structure
static inline SpxTypes spx_type_next(SpxTypes type) {
#ifndef _STRIP_META_
	size_t name_size = spx_aligned_size(strlen(type->name)+1);
	return (SpxTypes) ((char *) (type + 1) + name_size);
#else
	return type + 1;
#endif
}

#ifndef _STRIP_META_
#define SPX_TYPE_FMT_ FMT_LF2_(type, "%s.%s", SPX_OID_FMT_)
#define SPX_TYPE_FMT FMT_LF2(type, "%s.%s", SPX_OID_FMT_)
#define SPX_TYPE_VAL(type)	\
	StrPFFOrQ((type),schema,name), StrPFOrQ((type),name),	\
	SPX_OID_VAL( PtrFieldOr0((type), oid) )
#else
#define SPX_TYPE_FMT_ FMT_LF_(type, "%d.%d")
#define SPX_TYPE_FMT FMT_LF(type, "%d.%d")
#define SPX_TYPE_VAL(type) Ptr2FieldsOr0((type),schema,id), PtrFieldOr0((type),oid)
#endif

typedef const struct spx_type_cache *SpxTypeCache;
// variable-sized structure
struct spx_type_cache {
	SpxSchemaCache schema_cache;
	SpxSchemaPath schema_path;	// needed?
	int size;
#if 1			// not really
	SpxTypes by_name[0];
#else			// really (but illegal in C):
	SpxTypes by_name[size];
	SpxTypes by_oid[size];
	struct spx_type variable_length_types[size];
#endif
};

static inline SpxTypes *
spx_type_cache_by_oid(SpxTypeCache cache) {
	return (SpxTypes *) cache->by_name + cache->size;
}

// returns start of type arena
static inline SpxTypes
spx_type_cache_types(SpxTypeCache cache) {
	return (SpxTypes) (spx_type_cache_by_oid(cache) + cache->size);
}

SpxTypes SpxTypeByOid(CALLS_ Oid oid);
SpxTypes SpxTypeByName(CALLS_ StrPtr name);
int32 SpxLoadTypes(_CALLS_); // returns count, frees any old types
void SpxTypeCacheCheck(CALLS_ SpxTypeCache cache);

/* * Procedure Cache */

#define MAX_PROCS 10000	// make plenty big!
#define SPX_PROC_MAX_ARGS (9)
// see StoredProcSql and LoadStoredProc before changing!
// variable-sized structure
typedef const struct spx_proc {
	Oid oid;
	SpxSchemas schema;
	/* reordering the next two lines causes the memory allocation
		 in LoadProc to be off at the end of the loop!!
 */
	bool readonly;		// NOT VOLATILE
	SpxTypes return_type;
	int min_args, max_args;
#if 1				// not really
	Oid arg_type_oids[0];
#else			// really:
	Oid arg_type_oids[max_args];
	SpxTypes arg_types[max_args];
#ifndef _STRIP_META_
	char name[ word_align( strlen(name)+1 ) ];
#endif
#endif
} *SpxProcs;

typedef struct spx_proc *SpxProcPtr; // prefer "SpxProcs" when possible!

static inline SpxTypes *spx_proc_arg_types(SpxProcs proc) {
	return (SpxTypes *) (proc->arg_type_oids + proc->max_args);
}

static inline char *spx_proc_name(SpxProcs proc) {
	return (char *) (spx_proc_arg_types(proc) + proc->max_args);
}

// SEE ALSO spx_proc_end in spx.c

#define SPX_PROC_OID_FMT " proc " SPX_OID_FMT
#define SPX_PROC_OID_VAL(oid) SPX_OID_VAL(oid)

#ifndef _STRIP_META_

#define SPX_PROC_FMT_ FMT_LF_(proc, "%s.%s-" SPX_OID_FMT__)
#define SPX_PROC_FMT FMT_LF(proc, "%s.%s-" SPX_OID_FMT__)
#define SPX_PROC_VAL(proc)							\
	(!(proc) ? "???" : !(proc)->schema ? "??" : StrOrQ((proc)->schema->name)) , \
	 StrOrQ(spx_proc_name((proc))), (!(proc) ? 0 : (proc)->oid)

#else

#define SPX_PROC_FMT_ FMT_LF_(proc, SPX_OID_FMT__ "." SPX_OID_FMT__ )
#define SPX_PROC_FMT FMT_LF(proc, SPX_OID_FMT__ "." SPX_OID_FMT__ )
// protect this one better against NULLs as in the one above!!
#define SPX_PROC_VAL(proc)							\
	(proc)->schema ? (proc)->schema->oid : -1,  (proc)->oid

#endif

typedef enum proc_init_result {
	proc_init_ok,
	proc_init_ptr_null, proc_init_oid_null,
	proc_init_plan_exists, proc_init_unknown_proc,
	num_proc_init_results
} ProcInitResult;

/* Returns length of name and signature string of procedure p.
	 Stores up to size bytes of that string into a non-null dst.  */
size_t SpxProcSig(CALLS_ char *dst, size_t size, SpxProcs p);

/* Construct an SQL command which calls the given procedure p with
	 arguments of the indicated arg_types and returns a result of type
	 ret_type.  Casts are applied where the types differ.  We assume
	 that the casts are possible.  */
size_t SpxCallStr(
	CALLS_ char *dst, size_t size, SpxProcs p,
	SpxTypeOids ret_type, const Oid arg_types[], int num_args
);
			 
/* Create and return a query plan to call the given proc given the
	 arg_types array.  Return a status code if given a return pointer.
 */
SpxPlans SpxProcTypesQueryPlan(
	CALLS_ SpxProcs proc, SpxTypeOids ret_type,
	const Oid arg_types[], int num_arg_types,
	ProcInitResult *status_return
);

static inline ProcInitResult SpxRequireQueryPlan(
	CALLS_ SpxProcs proc, SpxTypeOids ret_type,
	const Oid arg_types[], int num_arg_types,
	const SpxPlans *plan		// const deception!!
) {
	CALLS_LINK();
	ProcInitResult status;
	if ( ! SpxPlanNull(*plan) ) {
		CallAssertMsg( plan->num_args == num_arg_types,
		"plan->num_args: %d, num_arg_types: %d",
		plan->num_args, num_arg_types );
		return proc_init_ok;
	}
	*(SpxPlanPtr)plan = SpxProcTypesQueryPlan(
		CALL_ proc, ret_type, arg_types, num_arg_types, &status
	);
	return status;
}

typedef const struct spx_proc_cache *SpxProcCache;
// variable-sized structure
struct spx_proc_cache {
	SpxSchemaCache schema_cache;
	SpxSchemaPath schema_path;	// needed?
	SpxTypeCache type_cache;
	int size;
#if 1
	SpxProcs by_name[0];
#else		// actually:
	SpxProcs by_name[size];
	SpxProcs by_oid[size];
	struct spx_proc proc_heap[size irregular elements];
#endif
};

static inline SpxProcs *spx_proc_cache_by_oid(SpxProcCache cache) {
	return (SpxProcs *) cache->by_name + cache->size;
}
		
SpxProcs SpxProcByOid(CALLS_ Oid oid);
SpxProcs SpxProcByName(CALLS_ StrPtr name);
int SpxLoadProcs(_CALLS_);
void SpxProcCacheCheck(CALLS_ SpxProcCache cache);

/* * Cache Management */

typedef const struct spx_caches {
	SpxSchemaCache schema_cache;
	SpxSchemaPath schema_path;
	SpxTypeCache type_cache;
	SpxProcCache proc_cache;
} *SpxCaches;

struct spx_caches SpxCurrentCaches(void);
void SpxCheckCaches(CALLS_ SpxCaches caches);

/* * Errors */

// Warning: will break if codes change!!
enum query_codes {
				 Query_Error_Unknown = -12, // dummy start
			Query_Error_Typunknown = -11,
			 Query_Error_Nooutfunc = -10,
		 Query_Error_Noattribute = -9,
		 Query_Error_Transaction = -8,
		 Query_Error_Param = -7,
	Query_Error_Argument = -6, // SPI_ERROR_ARGUMENT = plan is invalid!
		Query_Error_Cursor = -5, //Not Used Anymore
		 Query_Error_Unconnected = -4,
			 Query_Error_Opunknown = -3,
			Query_Error_Copy = -2,
	 Query_Error_Connect = -1,
						Query_Bogus = 0, // Bogus
			 Query_Ok_Connect = 1,
				Query_Ok_Finish = 2,
				 Query_Ok_Fetch = 3,
			 Query_Ok_Utility = 4,
				Query_Ok_Select = 5,
			 Query_Ok_Selinto = 6,
				Query_Ok_Insert = 7,
				Query_Ok_Delete = 8,
				Query_Ok_Update = 9,
				Query_Ok_Cursor = 10,
		Query_Ok_Insert_Returning = 11,
		Query_Ok_Delete_Returning = 12,
		Query_Ok_Update_Returning = 13,
		 Query_Ok_Rewritten = 14,
			 Query_Ok_Unknown = 15, // dummy end
};

// Warning: not maintainable!!! will break if codes change!!!
struct spx_query_decodes {
	enum query_codes code;
	StrPtr name;
};

// Warning: not maintainable!!! will break if codes change!!!
static inline StrPtr SpxQueryDecode(enum query_codes code) {
	extern const struct spx_query_decodes Spx_Query_Decode[];
	return Spx_Query_Decode[
			code < Query_Error_Unknown ? Query_Error_Unknown
			: (code > Query_Ok_Unknown ? Query_Ok_Unknown
			: code - Query_Error_Unknown)
	].name;
}

/* * SPI Connection */

int StartSPX(_CALLS_);				// post::InSPX(calls)
int FinishSPX(CALLS_ int start_level);	// pre:InSPX(calls)

void SpxInitSPX(_CALLS_); // @pre( InSPX(calls) )
void SpxInit(_CALLS_);	  // @pre( not in SPX at this level )

static inline void SpxRequired(_CALLS_) {
	CALLS_LINK();
	extern int spx_Initialized_; // utl-debug.h: MODULE_TAG(Initialized_)
	CallAssert(spx_Initialized_);
}

 // @pre( !InSPX(calls) ), idempotent -- was Backwards!!! clients???
// we should check this!!!
static inline void RequireSpxSPX(_CALLS_) {
	CALLS_LINK();
	extern int spx_Initialized_; // utl-debug.h: MODULE_TAG(Initialized_)
	if ( ! spx_Initialized_ )
		SpxInitSPX(_CALL_);
}

// @pre( inSPX(calls) ), itempotent -- was Backwards!!! clients???
// we should check this!!!
static inline void RequireSpx(_CALLS_) {
	CALLS_LINK();
	extern int spx_Initialized_; // utl-debug.h: MODULE_TAG(Initialized_)
	if ( ! spx_Initialized_ )
		SpxInit(_CALL_);
}

/* * PostgreSQL Interface Functions */

#if 0

// From server/fmgr.h
typedef struct FmgrInfo {
	Oid	fn_oid;	// OID of function (NOT of handler, if any)
	short	fn_nargs;	// 0..FUNC_MAX_ARGS, or -1 if variable arg count
	bool	fn_strict;	// function is "strict" (NULL in => NULL out)
	bool	fn_retset;	// function returns a set
} FmgrInfo;

typedef struct FunctionCallInfoData {
	FmgrInfo   *flinfo;	// ptr to lookup info used for this call
	bool	isnull;	// function must set true if result is NULL
	short	nargs;	// # arguments actually passed
	Datum	arg[FUNC_MAX_ARGS];	// Arguments passed to function
	bool	argnull[FUNC_MAX_ARGS]; // T if arg[i] is actually NULL
} FunctionCallInfoData;

// PG_FUNCTION_ARGS:: FunctionCallInfoData fcinfo

// Routines in fmgr.c
Oid	fmgr_internal_function(const char *proname);
Oid	get_fn_expr_rettype(FmgrInfo *flinfo);
Oid	get_fn_expr_argtype(FmgrInfo *flinfo, int argnum);
Oid	get_call_expr_argtype(fmNodePtr expr, int argnum);
bool get_fn_expr_arg_stable(FmgrInfo *flinfo, int argnum);
bool get_call_expr_arg_stable(fmNodePtr expr, int argnum);

#endif

#define FUNCTION_DECLARE(f) Datum (f)(PG_FUNCTION_ARGS)
#define FUNCTION_DEFINE(f)	\
	PG_FUNCTION_INFO_V1(f);	\
	FUNCTION_DECLARE(f)

static inline int SpxFuncNargs(PG_FUNCTION_ARGS) { return PG_NARGS(); }

#if 0
// **	PgSQL 12.0 SPI API Change!!
// before and after we have in fmgr.h:
#define PG_FUNCTION_ARGS	FunctionCallInfo fcinfo
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;

// ***	Before PostgreSQL 12.0 we have in fmgr.h:
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

typedef struct FunctionCallInfoData {
	// ...
	Datum		arg[FUNC_MAX_ARGS]; /* Arguments passed to function */
	bool		argnull[FUNC_MAX_ARGS]; /* T if arg[i] is actually NULL */
// ...
}
// along with the macros in fmgr.h which we weren't using:
#define PG_ARGISNULL(n)  (fcinfo->argnull[n])
#define PG_GETARG_DATUM(n)	 (fcinfo->arg[n])

//***  With PostgreSQL 12.0 this changes to:

typedef struct FunctionCallInfoData *FunctionCallInfo;
	
typedef struct FunctionCallInfoBaseData {
	// ...
	NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
	// ...
} FunctionCallInfoBaseData;
// and we have the new data structure:
typedef struct NullableDatum {
	Datum		value;
	bool		isnull;
} NullableDatum;
// and the macros have changed to 											
#define PG_ARGISNULL(n)  (fcinfo->args[n].isnull)
#define PG_GETARG_DATUM(n)	 (fcinfo->args[n].value)
// So we'd best change the Wicci to use those macros!!!
#endif

#if 0
// *** We've been using this to pass arguments for queries
 static inline Datum * SpxFuncArgs(PG_FUNCTION_ARGS) { return fcinfo->arg; }
// But the SPI query functions require contiguous args, e.g.:

// Now we're going to have to copy them into temporary storage
// ***  How are we going to handle it?
// We could create space on the stack and copy everything there
// We could do things like
// /usr/local/src/postgresql-12.0/src/backend/executor/spi.c 2444
static ParamListInfo
_SPI_convert_params(int nargs, Oid *argtypes,
										Datum *Values, const char *Nulls);
// and
// /usr/local/src/postgresql-12.0/src/backend/nodes/params.c 31
ParamListInfo makeParamList(int numParams);

/*
We could find the undocumented but widely used function which allows directly calling a function given its procedure oid and use that!
- FunctionCallInvoke
// fmgr.h 167
#define FunctionCallInvoke(fcinfo)	((* (fcinfo)->flinfo->fn_addr) (fcinfo))
// check out: /usr/local/src/postgresql-12.0/src/backend/tcop/fastpath.c
// Oo, oo, check out:
// /usr/local/src/postgresql-12.0/src/backend/utils/fmgr/fmgr.c
// backend/utils/sort/sortsupport.c
// maybe also
// backend/utils/adt/rowtypes.c
// backend/utils/adt/arrayfuncs.c

We could get the PostgreSQL core team to add a stable version of the latter to the SPI API and then use it.

But we're set up to use stored procedures, so let's go with that for now!
*/
#endif

// Returns the number of null arguments
// !!args gets copies of non-null Datums into corresponding elements
// !!is_nulls gets 1/0 for is_null into corresponding elements of is_nulls if non-null
int SpxFuncArgs(PG_FUNCTION_ARGS, Datum args[], char is_nulls[]);


static inline bool SpxFuncStrict(PG_FUNCTION_ARGS) { return fcinfo->flinfo->fn_strict; }
static inline bool SpxFuncReturnsSet(PG_FUNCTION_ARGS) {
	return fcinfo->flinfo->fn_retset;
}
static inline bool SpxFuncArgNull(PG_FUNCTION_ARGS, int n) {
	return PG_ARGISNULL(n);
}
static inline Oid SpxFuncOid(PG_FUNCTION_ARGS) { return fcinfo->flinfo->fn_oid; }
static inline Oid SpxFuncArgType(PG_FUNCTION_ARGS, int n) {
	return get_fn_expr_argtype(fcinfo->flinfo, n);
}
static inline Oid SpxFuncReturnType(PG_FUNCTION_ARGS) {
	return get_fn_expr_rettype(fcinfo->flinfo);
}
static inline Datum SpxCopyPointerDatum(Datum d) {
	return (Datum) PG_DETOAST_DATUM_COPY(d);
}

#define SPX_FUNC_NUM_ARGS_IS(n)			\
	CallAssertMsg( SpxFuncNargs(fcinfo) == (n),	\
		"nargs=%d",  SpxFuncNargs(fcinfo) )

enum spx_check_levels {
	spx_check_none = -1,	// check does not apply
	spx_check_error,		// assert type, non-null
	spx_check_warn,		// warn on wrong type or null
	spx_check_break,		// just stop on wrong type or null
	num_spx_check_levels
};

bool SpxCheckArgCount(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	int min, int max
);

bool SpxCheckArgNonNull(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level, int arg
);

bool SpxCheckArgTypeOid(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	int arg, SpxTypeOids type, int *num_unknown_accum
);

// check nargs arguments, return number which checked OK
int SpxCheckArgsRegtypesMinMax(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels,
	const Oid *, int num, int min, int max
);

static inline int SpxCheckArgsRegtypes(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	const Oid types[], int num
) {
	CALLS_LINK();
	return SpxCheckArgsRegtypesMinMax(
		CALL_ fcinfo, level, types, num, num, num
	);
}

// check nargs arguments, return number which checked OK
int SpxCheckArgsMinMax(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels,
	int min, int max, SpxTypeOids, ...	// varargs ending in 0
);

// check nargs arguments, return number which checked OK
int SpxCheckArgs(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels,
	SpxTypeOids, ...	// varargs ending in 0
);

#define SPX_FUNC_ARG_TYPE_IS(arg_num, expected) ({	\
	int n = (arg_num);								\
	Oid actual = SpxFuncArgType(fcinfo, n);			\
	CallAssertMsg( actual == (expected).type_oid,		\
	 "arg %d type_oid %d", n, actual  );				\
})

/* * Allocator Functions */

/* MaxAllocationSize will help prevent memory problems, and
	 postpones having to deal with toasting tuples. It will be necessary
	 to revisit this when we move to real  websites!!! */
static const size_t MaxAllocationSize = 1024 * 1024;  // !!! prototype debugging !!!

/* MaxTextStrLen will help prevent memory problems, and
	 postpones having to deal with toasting tuples. It will be necessary
	 to revisit this when we move to real  websites!!! */
static const size_t MaxTextStrLen = 16 * 1024;  // !!! prototype debugging !!!

/* MaxTorNameSize will help prevent memory problems; note
	 that name_ref objects are "supposed" to be fairly small. */
static const size_t MaxTorNameSize = 40; // !!! prototype debugging !!!

/*
	There are three contexts for memory allocation:
	(1) Allocate for the Session - use malloc
	(2) Allocate in the current memory context - use palloc
	(3) If we're inside of an SPI_connect, allocate in the parent memory context
       - use SPI_palloc

	My current understanding is that functions being called from
	SQL should use palloc for allocating objects which they're
	returning by reference, and that the SQL call binding mechanism
	will copy those objects before ending that memory context.
  This seems to be all that we can do, as calling SPI_palloc when we
	haven't done an SPI_connect gives the error that we're not connected.
 */

// #define ALLOCATOR_FUNCTION(f) void *(f)(size_t size)
#define ALLOCATOR_FUNCTION(f) void *(f)(CALLS_ size_t size)
#define ALLOCATOR_PTR(fp) ALLOCATOR_FUNCTION(*fp)
// #define CALL_ALLOC(alloc, size) alloc(size)
#define CALL_ALLOC(alloc, size) alloc(_CALL_, size)

/* * Allocation Function Declarations */

/* These used to be defined here as static inline functions
 * but then they got instrumented and needed access to
 * debug_level() which is not available outside of
 * compilation units!!
 */

/* allocate storage in current SPI_context */
ALLOCATOR_FUNCTION(call_palloc);
/* allocate storage in SPI_context above us */
ALLOCATOR_FUNCTION(call_SPI_palloc);
/* allocate storage on the heap */
ALLOCATOR_FUNCTION(SessionAlloc);

void * CheckedAlloc(
	CALLS_ size_t size, size_t max_size, ALLOCATOR_PTR(alloc)
);
void * MemAlloc( CALLS_ size_t size, ALLOCATOR_PTR(alloc) );
char * StrAlloc( CALLS_ size_t size, ALLOCATOR_PTR(alloc) );

// NULL => NULL
// all other strings (including empty strings) will be reallocated
// (empty strings get a new 1-byte buffer for a '\0' byte
StrPtr NewStr( CALLS_ Str old, ALLOCATOR_PTR(alloc) );

/* * Utility Functions */

text *
CStringToText( CALLS_ UtilStr s, ALLOCATOR_PTR(alloc) );

UtilStr
TextToCString( CALLS_ const text *const t, ALLOCATOR_PTR(alloc) );

/* Warning!!!
 * Stored procedures
	- especially those held in static local variables of functions
	 will cause great trouble when some change in the database invalidates them.
 * Right now this is how we are doing things!
 * We can fix this with a slight redesign:
	(1) Make sure Query Plans contain their query string
	(2) Make sure query function gets passwd QueryPlans by pointer
	(3) On SPI_ERROR_ARGUMENT,
	(3a) recreate the plan
	(3b) reissue the query
 */

/* Create and save a Query Plan */
// Prefer (const Oid arg_types[]) if SPI API gets fixed
SpxPlans SpxPlanQuery(
	 CALLS_ StrPtr sql_str, const Oid arg_types[], int num_args );
// !!arg_types == !!num_args, not __attribute__((nonnull));

/* fragile variadic function:
	 Non-negative argument n
	 MUST be followed by that number of SpxTypeOids arguments!
 */
void SpxPlanNvargs(
	CALLS_ SpxPlanPtr plan, StrPtr sql_str,
	size_t n, SpxTypeOids arg0, ...
) __attribute__((nonnull));

static inline void SpxPlan0(
	CALLS_ SpxPlanPtr SavedPlan, StrPtr sql_str
) __attribute__((nonnull));
static inline void SpxPlan0(
	CALLS_ SpxPlanPtr SavedPlan, StrPtr sql_str
	) {
	CALLS_LINK();
	if  ( SpxPlanNull(*SavedPlan) )
		*SavedPlan = SpxPlanQuery(CALL_ sql_str, 0, 0);
}

static inline void SpxPlan1(
	CALLS_ SpxPlanPtr plan, StrPtr sql_str, SpxTypeOids arg_type
) __attribute__((nonnull));
static inline void SpxPlan1(
	CALLS_ SpxPlanPtr plan, StrPtr sql_str, SpxTypeOids arg_type
) {
	CALLS_LINK();
	if  ( SpxPlanNull(*plan) )
		*plan = SpxPlanQuery(CALL_ sql_str, &arg_type.type_oid, 1);
}

static inline void SpxPlan2(
	CALLS_ SpxPlanPtr plan, StrPtr sql_str,
	SpxTypeOids arg_type0, SpxTypeOids arg_type1
) __attribute__((nonnull));
static inline void SpxPlan2(
	CALLS_ SpxPlanPtr plan, StrPtr sql_str,
	SpxTypeOids arg_type0, SpxTypeOids arg_type1
) {
	CALLS_LINK();
	if  ( SpxPlanNull(*plan) ) {
		Oid arg_types[2];
		arg_types[0] = arg_type0.type_oid;
		arg_types[1] = arg_type1.type_oid;
		*plan = SpxPlanQuery(CALL_ sql_str, RAnLEN(arg_types));
	}
}

// For safety, every function taking a plan to execute with arguments should also
// take a parameter for how many arguments are being passed.  This will allow
// checking that there are at least as many arguments as are needed by the plan!!!

// execute a query plan returning status
int SpxAccessDB(CALLS_ SpxPlans plan, Datum args[], int nrows, bool readonly);

// Execute a read/write query plan
#define SpxUpdateDB(plan,args,nrows)  SpxAccessDB(CALL_ (plan),(args),(nrows), 0) 


/* Update Database Convenience Functions */


/* Execute a read-write query plan returning the result, if any */

Datum SpxUpdateDatum(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
Datum SpxUpdateTypedDatum(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);

Oid SpxUpdateOid(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
Oid SpxUpdateTypedOid(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int32 SpxUpdateInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
int64 SpxUpdateInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
int32 SpxUpdateTypedInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int64 SpxUpdateTypedInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int32 SpxUpdateIfInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids *, int32 or_else);
int64 SpxUpdateIfInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids *, int64 or_else);
#if 0
// These are fine but unused
bool SpxUpdateBool(CALLS_ SpxPlans, Datum args[], bool *null_ret);
StrPtr SpxUpdateStr(CALLS_ SpxPlans, Datum args[], ALLOCATOR_PTR(alloc));
SpxText SpxUpdateText(CALLS_ SpxPlans, Datum args[], ALLOCATOR_PTR(alloc));
#endif

// Execute a read-only query plan
#define SpxQueryDB(plan,args,nrows)  SpxAccessDB(CALL_ (plan),(args),(nrows), 1) 

/* result_types and NULL value detection coventions:

		type_ret parameters can be passed a NULL pointer and the
		result_type will not be returned.

		null_ret parameters can be passed a NULL pointer and the NULL
		condition becomes an error.

		The RowCol*Str and SpxQuery*Str functions will return a NULL string
		if the string being fetched is NULL.

		Warning: the SPI_ convention is that rows are 0-based, cols 1-based
		BUT: the spx_ convention is that both are 0-based.
*/

/* Fetch a field from the return set of a query plan;
	 The type-specific type-checking functions are preferred.
	 Passing a null pointer for an null_return means that
	 a error will be thrown on a null result.
*/
Datum RowColDatum(CALLS_ int row, int col, SpxTypeOids *type_ret, bool *null_ret);
Datum RowColTypedDatum(CALLS_ int row, int col, SpxTypeOids expected, bool *null_ret);
Oid RowColOid(CALLS_ int row, int col, SpxTypeOids *type_ret, bool *null_ret);
Oid RowColTypedOid(CALLS_ int row, int col, SpxTypeOids expected_type, bool *null_ret);
int32 RowColInt32(CALLS_ int row, int col, SpxTypeOids *type_ret, bool *null_ret);
int64 RowColInt64(CALLS_ int row, int col, SpxTypeOids *type_ret, bool *null_ret);
int32 RowColTypedInt32(CALLS_ int row, int col, SpxTypeOids expected, bool *null_ret);
int64 RowColTypedInt64(CALLS_ int row, int col, SpxTypeOids expected, bool *null_ret);
#if 0
int32 RowColIfTypeInt32(CALLS_ int row,int col,SpxTypeOids *type_ret, int32 or_else);
int64 RowColIfTypeInt64(CALLS_ int row, int col, SpxTypeOids *type_ret, int64 or_else);
#endif
bool RowColBool(CALLS_ int row, int col, bool *null_ret);
StrPtr RowColStrPtr(CALLS_ int row, int col);  // fragile!!! why???
StrPtr RowColStr(CALLS_ int row, int col, ALLOCATOR_PTR(alloc));
SpxText RowColText(CALLS_ int row, int col, ALLOCATOR_PTR(alloc));
size_t RowColTextLen(CALLS_ int row, int col, bool *is_null_ret);
size_t RowColTextCopy(
	CALLS_ int row, int col, char *buffer, size_t buffer_size, bool *is_null_ret
);

/* Execute a read-only query plan returning the result, if any */

Datum SpxQueryDatum(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
Datum SpxQuerydDatum(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
Oid SpxQueryOid(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
Oid SpxQueryTypedOid(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int32 SpxQueryInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
int64 SpxQueryInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids *type_ret, bool *null_ret);
int32 SpxQueryTypedInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int64 SpxQueryTypedInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids expected, bool *null_ret);
int32 SpxQueryIfInt32(CALLS_ SpxPlans, Datum args[], SpxTypeOids *, int32 or_else);
int64 SpxQueryIfInt64(CALLS_ SpxPlans, Datum args[], SpxTypeOids *, int64 or_else);
#if 0
// Not currently used
bool SpxQueryBool(CALLS_ SpxPlans, Datum args[], bool *null_ret);
#endif
StrPtr SpxQueryStr(CALLS_ SpxPlans, Datum args[], ALLOCATOR_PTR(alloc));
SpxText SpxQueryText(CALLS_ SpxPlans, Datum args[], ALLOCATOR_PTR(alloc));

/* Unplanned Queries */

#ifndef OID_FROM_TYPE_STR
#define OID_FROM_TYPE_STR(type_str) SpxQueryNameToType(NULL, type_str)
#endif


// String Support

bool is_entity_name(StrPtr s); //  possible entity name with possible schema
bool is_simple_name(StrPtr s); //  possible lowercase entity name
bool no_quotes(StrPtr s);      // free of single quotes

// StrPtr NewStr( Str old, ALLOCATOR_PTR(f) ); // copy str into new storage

/* concatenate strings into new storage */
StrPtr StrCat(CALLS_ ALLOCATOR_PTR(alloc), StrPtr s0, ...) __attribute__ ((sentinel));

/* concatenate string slices into new storage */
StrPtr StrCatSlices(CALLS_ ALLOCATOR_PTR(alloc), const StrPtr start, const StrPtr end, ...) __attribute__ ((sentinel));

// Array Support

/* Caution: this structure may change without notice:
 * DO NOT directly access any of its fields, use
 * the functions provided below!

 * Shouldn't
 	void *data
 * be
 	Datum *data
 * instead??
 */
enum { array_max_dims = 1 };
typedef struct array_info {
	ArrayType *array_type_ptr;
	void *data;  bool isnull;  int ndims;
	int *dims;	int *base;
	int dims_array[array_max_dims];
	int base_array[array_max_dims]; // index of first element
	Oid elem_type; int16 typlen;  bool typbyval;  char typalign;
#if 0
	char typdelim;  Oid typelem;  Oid typiofunc;  FmgrInfo proc;
#endif
} *SpxArrayInfo;

/* Use the following functions to get information
 * from a properly initialized SpxArrayInfo structure
 */

static inline ArrayType *SpxArrayTypePtr(SpxArrayInfo a) {
	return a->array_type_ptr;
}
static inline void *SpxArrayData(SpxArrayInfo a) {
	return a->data;
}
static inline bool SpxArrayIsNull(SpxArrayInfo a) {
 return a->isnull;
}
static inline size_t SpxArrayNDims(SpxArrayInfo a) {
	return a->ndims;
}
static inline size_t SpxArrayDims(SpxArrayInfo a) {
	return a->dims[0];
}
static inline size_t SpxArrayNItems(SpxArrayInfo a) {
 return ArrayGetNItems(a->ndims, a->dims);
}
static inline Oid SpxArrayElemType(SpxArrayInfo a) {
	return a->elem_type;
}

/* The following functions construct SpxArrayInfo structures.
 * Either pass in a SpxArrayInfo structure to use, reserving it
 * for this new use, and being sure it is NOT deallocated too soon
 * Or pass in NULL and a suitable allocator to be used to
 * allocate a new structure.
 */

// Return a description of a Null Postgres Array of given type
SpxArrayInfo SpxNullArray(
	CALLS_ SpxTypeOids, SpxArrayInfo,  ALLOCATOR_PTR(alloc)
);

// Return a description of an Empty Postgres Array of given type
SpxArrayInfo SpxEmptyArray(
	CALLS_ SpxTypeOids, SpxArrayInfo, ALLOCATOR_PTR(alloc)
);

/* Create and return a description of a new Postgres Array
	 of given type and contents */
SpxArrayInfo SpxNewArray(
	CALLS_ SpxTypeOids elem_type, int nelems, Datum *,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
);

/* Create and return a description of an existing Postgres Array
	 from the argument data and nullity. */
SpxArrayInfo SpxGetArray(
	CALLS_ ArrayType *, bool isnull, SpxArrayInfo, ALLOCATOR_PTR(alloc)
);

/* Major Functions */

// FUNCTION_DECLARE(spx_init);
FUNCTION_DECLARE(unsafe_spx_initialize);
FUNCTION_DECLARE(spx_collate_locale);
FUNCTION_DECLARE(unsafe_spx_load_schemas);
FUNCTION_DECLARE(spx_debug_schemas);
FUNCTION_DECLARE(unsafe_spx_load_schema_path);
FUNCTION_DECLARE(unsafe_spx_load_types);
FUNCTION_DECLARE(unsafe_spx_load_procs);
FUNCTION_DECLARE(spx_test_select);

#ifndef NO_TEST_FRAME

FUNCTION_DECLARE(spx_schema_by_id);
FUNCTION_DECLARE(spx_schema_by_oid);
FUNCTION_DECLARE(spx_schema_path_by_oid);
FUNCTION_DECLARE(spx_type_by_oid);
FUNCTION_DECLARE(spx_proc_by_oid);
FUNCTION_DECLARE(spx_proc_call_proc_str);

// array testing code
FUNCTION_DECLARE(spx_describe_array); // ANYARRAY -> nitems
FUNCTION_DECLARE(spx_item_to_singleton); // item -> [item]

// misc testing
FUNCTION_DECLARE(spx_test_planned_queries); // item -> [item]

#endif

#ifndef SPX_C
#include "last-tag.h"
#define LAST_TAG SPX
#define LastTag Spx
#define last_tag spx
#endif

#endif
