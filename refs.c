static const char refs_module_id[] = "$Id: ref.c,v 1.5 2007/07/24 04:27:48 greg Exp greg $";
/* * Wicci Project Object References C Code Module

	Wicci Project
	PostgreSQL Server-side C Code
	Typed/Tagged Object References

 ** Copyright

	Copyright (c) 2005-2019 J. Greg Davidson.
	Although it is my intention to make this code available under
	a Free Software license when it is ready, this code is
	currently not to be copied nor shown to anyone without
	my permission in writing.
 */

#define REFS_C
#include "refs.h"

#define MODULE_TAG(name) refs_##name
#include "debug-spx.h"

/* would like to get rid of this dependency
	 and right now it's needed for call_in_method:
 */
#include "catalog/pg_type.h"
#include <access/htup_details.h> // for BITMAPLEN

/* * Method Management */

/* ** method table */

/* The method table is a simple array sorted by
	(1) operation, (2) tag
	 for convenient binary search.
 */

MutableTomCaches Tom_Cache = 0;

extern void
RefTomCacheCheck(CALLS_ TomCaches cache) {
	CALLS_LINK();
	SpxCheckCaches(CALL_ &cache->spx_caches);
	CallAssert(MutableTomCache(cache) == Tom_Cache);
}

static inline void TomCacheCheck(_CALLS_) {
	CALLS_LINK();
	RefTomCacheCheck(CALL_ Tom_Cache);
}

static int
cmp_toms(const Toms a, const Toms b) {
	const int cmp_ops = a->operation->oid - b->operation->oid;
	return cmp_ops ? cmp_ops : a->tag - b->tag;
}

#if 0
static int
cmp_toms_ptrs(const TomsPtrs a, const TomsPtrs b) {
	return cmp_toms(*a, *b);
}
#endif

/* WHAT WE WANT AND DO NOT HAVE!!!!
	When there is no method available for a given operation
	and class, a fallback for that operation may be tried.
	--> When the fallback is not an operator, then it is the
	method to use.  <--  When the fallback IS an operator,
	and it also has no method for this class, IT may have
	a fallback, and so on.
 */
// Warning: within this module, result may be cast to TomPtr
static Toms GetTom(CALLS_ SpxProcs op, ref_tags tag) {
	CALL_LINK();
	struct typed_object_method target = {.tag = tag};
	struct typed_object_method fallback = {.tag = -1, .method = op};
	Toms tom, fallback_tom = &fallback;
	for (;;) {
		target.operation = fallback_tom->method;
		fallback_tom = 0;
		const Toms target_ptr = &target, tom_cache = Tom_Cache->tom;
		tom = SPX_BSEARCH(
			target_ptr, tom_cache, Tom_Cache->size,
			sizeof target, cmp_toms
		);
		if ( tom ) break;
		fallback.operation = target.operation;
		const Toms fallback_ptr = &fallback;
		fallback_tom = SPX_BSEARCH(
			fallback_ptr, tom_cache, Tom_Cache->size,
			sizeof fallback, cmp_toms
		);
		if ( !fallback_tom ) break;
		CALL_DEBUG_OUT(
				 "%s falling back to %s",
				 spx_proc_name(fallback.operation),
				 spx_proc_name(fallback_tom->method)
		);
	}
	return tom;
}

// Warning: within this module, result may be cast to TomPtr
static Toms SameOldTom(CALLS_ Toms new_tom) {
	CALLS_LINK();
	if ( !Tom_Cache ) return 0;
	Toms old_tom=GetTom(CALL_ new_tom->operation, new_tom->tag);
	if ( !old_tom ) return 0;
	return old_tom->method == new_tom->method ? old_tom : 0;
}

static void DebugTom(CALLS_ Toms tom) {
	CALL_LINK();
	if (!tom)
		CALL_DEBUG_OUT("tom NULL");
	else
		CALL_DEBUG_OUT(
	REF_TAG_FMT " op" SPX_PROC_FMT
		" method" SPX_PROC_FMT SPX_PLAN_FMT,
	REF_TAG_VAL(tom->tag),
	SPX_PROC_VAL(tom->operation),
	SPX_PROC_VAL(tom->method),
	SPX_PLAN_VAL(tom->plan)
	);
}

FUNCTION_DEFINE(refs_debug_toms) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	if ( !Tom_Cache ) {
		CALL_DEBUG_OUT("Tom_Cache NULL");
		PG_RETURN_NULL();
	}
	const Toms start = Tom_Cache->tom;
	const Toms end = start + Tom_Cache->size;
	CALL_DEBUG_OUT("Found %d TOMs!\n", Tom_Cache->size);
	for (Toms p = start; p < end ; p++ )
		DebugTom(CALL_ p);
	PG_RETURN_INT32( Tom_Cache->size );
}

FUNCTION_DEFINE(refs_op_tag_to_method) {
	enum {op_arg, tag_arg, num_args};
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const Oid op_oid = PG_GETARG_OID(op_arg);
	const SpxProcs op = SpxProcByOid(CALL_ op_oid);
	const ref_tags tag = GetArgTag(tag_arg);
	const Toms tom = GetTom(CALL_ op, tag);
	if (!tom) {
		CALL_DEBUG_OUT(
	"null tom" SPX_PROC_FMT REF_TAG_FMT,
	SPX_PROC_VAL(op),REF_TAG_VAL(tag)
		);
		PG_RETURN_NULL();
	}
	DebugTom(CALL_ tom);
	PG_RETURN_OID(tom->method->oid);
}

static TomCaches LoadToms(_CALLS_) {
	enum {max_toms = MAX_PROCS * 2}; // not rigorous!!
	static char select[] =
		"SELECT DISTINCT tag_, operation_, method_"
		" FROM s1_refs.tag_operation_method_view"
		" ORDER BY operation_, tag_";
	enum tom_fields { tag_, operation_, method_ };
	CALL_LINK();
	CALL_DEBUG_OUT("==> LoadToms");
	static SpxPlans plan;
	SpxPlan0( CALL_ &plan, select );
	const int num_rows = SpxQueryDB(plan, NULL, MAX_PROCS);
	int num_ops = 0;
	SpxProcs last_op = 0;
	MutableTomCaches cache =
		spx_obj_alloc(sizeof *cache + num_rows * sizeof *cache->tom);
	cache->size = num_rows;
	cache->spx_caches = SpxCurrentCaches();
	MutableToms p = cache->tom, pp = p;
	for (; p < cache->tom + num_rows; pp = p++) {
		int row = p - cache->tom;
		p->tag = RowColTypedInt32(CALL_ row, tag_, RefTagIntType(), 0);
		// tag is either in TOC or is -1 to indicate an op fallback
		p->operation = SpxProcByOid(
			CALL_ RowColTypedOid(CALL_ row, operation_, Procedure_Type, 0)
		);
		if (last_op && last_op != p->operation)
			++num_ops;
		p->method = SpxProcByOid(
			CALL_ RowColTypedOid(CALL_ row, method_, Procedure_Type, 0)
		);
		CallAssertMsg( cmp_toms(pp, p) <= 0,
			 "pp " SPX_PROC_FMT REF_TAG_FMT
			 "p " SPX_PROC_FMT REF_TAG_FMT,
			 SPX_PROC_VAL(pp->operation), REF_TAG_VAL(pp->tag),
			 SPX_PROC_VAL(p->operation), REF_TAG_VAL(p->tag) );
		const MutableToms old_tom = MutableTom(SameOldTom(CALL_ p));
		if ( old_tom && !SpxPlanNull(old_tom->plan) )
			SPX_MOVE_PLAN(p->plan, old_tom->plan);
	}
	Assert_SpxObjPtr_AtEnd( CALL_ cache, p );
	CALL_DEBUG_OUT("<== LoadToms");
	return cache;
}

extern int RefLoadToms(_CALLS_) {
	CALLS_LINK();
	const TomCaches cache = LoadToms(_CALL_);
	if ( Tom_Cache )
		for ( MutableToms p = Tom_Cache->tom
			; p < Tom_Cache->tom + Tom_Cache->size ; p++ )
			SPX_FREE_PLAN(p->plan);
	SPX_OBJ_REF_DECR(Tom_Cache);
	//	Tom_Cache = SPX_OBJ_REF_INCR(cache);
	Tom_Cache = MutableTomCache(cache);
	return cache->size;
}

FUNCTION_DEFINE(unsafe_refs_load_toms) {	// () -> integer (count)
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_refs_load_toms();");
	SpxRequired(_CALL_);
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int count = RefLoadToms(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32( count );
}

/* ** method plans */

/* Make a plan to call the specified procedure with arguments
	 of the given types except for the first argument.
	 Assumes: (1) First argument is the ref type the proc wants
	 (2) Any other arguments are compatible with what the proc wants,
	 e.g. either the same or implicitly convertable.  */
static inline SpxPlans RefMakeQueryPlan(
	CALLS_ SpxProcs proc2call, SpxTypeOids ret_type,
	const Oid arg_types[], int num_arg_types // available args
) {
	CALL_LINK();
	const int num_args = num_arg_types >
		proc2call->max_args ? proc2call->max_args : num_arg_types;
	Oid args[num_args];
	args[0] = proc2call->arg_type_oids[0]; // expected ref type
	for (int i = 1; i < num_args; i++)
		args[i] = arg_types[i]; // compatible other types
	ProcInitResult status;
	const SpxPlans plan = SpxProcTypesQueryPlan(
		CALL_ proc2call, ret_type, args, num_args, &status
	);
	CallAssertMsg(
		status == proc_init_ok, "status: %s", SpxQueryDecode(status)
	);
	return plan;
}

static inline void RefRequireQueryPlan(
	CALLS_ SpxProcs proc2call, SpxTypeOids ret_type,
	const Oid arg_types[], int num_arg_types, // caller's arg types
	const SpxPlans *plan_ptr		// const deception!!
) {
	CALLS_LINK();
	if ( SpxPlanNull(*plan_ptr) )
		*SpxMutablePlan(plan_ptr) =
			RefMakeQueryPlan( CALL_ proc2call, ret_type, arg_types, num_arg_types );
	else
		CallAssertMsg( num_arg_types >= proc2call->min_args,
			"plan->num_args: %d, num_arg_types: %d",
			plan_ptr->num_args, num_arg_types
		);
}


// Possibility of implementing inheritance here??
static Toms MethodForOpTagSpx(
	CALLS_ SpxProcs op, ref_tags tag,
	const Oid arg_types[], int num_args_available
) {
	CALL_LINK();
	const MutableToms m = MutableTom(GetTom(CALL_ op, tag)); // const to mutable !!
	if (!m) return 0;		// inheritance here ??
	RefRequireQueryPlan(
		CALL_ m->method, SpxTypeOid(op->return_type),
		arg_types, num_args_available, &m->plan
	);
	return m;
}

static void TomDebug(
	 CALLS_ SpxProcs op, refs ref, int num_args, Toms tom
) {
	CALLS_LINK();
	char op_sig[ 1 + SpxProcSig(CALL_ 0, 0, op) ];
	SpxProcSig(CALL_ op_sig, sizeof op_sig, op);
	if ( !tom ) {
		CALL_WARN_OUT(
			"No method for op %s, " REF_FMT ", %d args!", op_sig, REF_VAL(ref), num_args
		);
		return;
	}
	char method_sig[ 1 + SpxProcSig(CALL_ 0, 0, tom->method) ];
	SpxProcSig(CALL_ method_sig, sizeof method_sig, tom->method);
	CALL_DEBUG_OUT(
		"op %s/%d\n\tmethod %s/%d\n\t" REF_FMT,
		op_sig, num_args, method_sig, tom->plan.num_args, REF_VAL(ref)
	);
}

static inline Toms MethodForOpRefSpx(
	CALLS_ SpxProcs op, refs ref, int num_args
) {
	CALLS_LINK();
	const Toms tom = MethodForOpTagSpx(
		CALL_ op, RefTag(ref), op->arg_type_oids, num_args
	);
	if ( !tom || DebugLevel() > 1 )
		TomDebug(CALL_ op, ref, num_args, tom);
	return tom;
}

Datum CallMethod(CALLS_ Toms tom, Datum args[], bool *null_ret) {
	CALL_LINK();
	return (
		tom->method->readonly
		? SpxQueryDatum
		: SpxUpdateDatum
	) (	CALL_ tom->plan, args, 0, null_ret  );
}

// Should we let someone know if either or both pointers are NULL??
static inline void NoValue(SpxText *text_ret, bool *null_ret) {
	if (text_ret) *text_ret = SpxTextNull();
	if (null_ret) *null_ret = true;
}

// Typed Object Method to Value
static void TomToValue(
	CALLS_ Toms tom, Datum *const args, int num_args, // we dispatch this
	SpxText *text_ret,		// to either return text_ret and is_null_ret
	bool *is_null_ret,		// required!
	Datum *datum_ret				// or datum_ret and is_null_ret
) {
	CALL_LINK();
	AssertSPX( _CALL_ );
	CallAssert( is_null_ret );
	CallAssert( !!text_ret != !!datum_ret );
	if (!tom) {
		NoValue( text_ret, is_null_ret );
		CALL_WARN_OUT("No TOM");
	}	else if ( text_ret ) {
		SpxText text = SpxQueryText(CALL_ tom->plan, args, is_null_ret);
		if (*is_null_ret)
			NoValue( text_ret, is_null_ret );
		else
#if 0
		*text_ret = SpxCopyText( CALL_ text, call_SPI_palloc );
#else
		*text_ret = text;
#endif
	}
	else
		*datum_ret = CallMethod(CALL_ tom, args, is_null_ret);
}

/* * op method dispatch without wicci magic */

static void RefEtcToValue(
	CALLS_
	PG_FUNCTION_ARGS /*fcinfo*/,		// we dispatch this
	SpxText *text_ret,	// to either return this and null_ret
	bool *null_ret,			// required!
  Datum *datum_ret		// or this and null_ret
) {
	enum {ref_arg};								// one required argument
	CALL_LINK();
	RefsRequired(_CALL_);
	const int level = StartSPX(_CALL_);
	const int num_args = SpxFuncNargs(fcinfo);
	CallAssert(null_ret);
	CallAssert(num_args > ref_arg );
	CallAssert(!SpxFuncArgNull(fcinfo, ref_arg));
	CallAssert(!SpxFuncReturnsSet(fcinfo));
	const refs ref = GetArgRef(0);
	// the op function is the function that called us
	const SpxProcs op = SpxProcByOid(CALL_ SpxFuncOid(fcinfo));
	// next 3 assertions difficult to violate since we're here
	CallAssertMsg( op, "no operation proc for " SPX_PROC_OID_FMT,
		 SPX_PROC_OID_VAL(SpxFuncOid(fcinfo)) );
	CallAssert(num_args >= op->min_args );
	CallAssert(num_args <= op->max_args );
	// What method implements this operation?
	const Toms tom = MethodForOpRefSpx(CALL_ op, ref, num_args);
	CallAssertMsg( tom, "no method proc for " SPX_PROC_OID_FMT,
		 SPX_PROC_OID_VAL(SpxFuncOid(fcinfo)) );
	Datum args[num_args];
	(void) SpxFuncArgs(fcinfo, args, 0);
	TomToValue(
		 CALL_ tom, args, SpxFuncNargs(fcinfo),
		 text_ret, null_ret, datum_ret
	);
	// Paranoid debugging!!!
  if ( DebugLevel() && text_ret ) {
		TEXT_BUFFER_TMP_STR(text_ret->varchar, str);
		CALL_DEBUG_OUT("pre-text: %s", *str ? str : "NULL");
	}
	FinishSPX(CALL_ level);
  if ( DebugLevel() && text_ret ) {
		TEXT_BUFFER_TMP_STR(text_ret->varchar, str);
		CALL_DEBUG_OUT("post-text: %s", *str ? str : "NULL");
	}
}

/* Handles most text-returning operations, with exceptions:
 * type output
 * operations taking a crefs argument for wicci magic
 * SIGNATURE: refs, ... -> NULL/cstring
 * Extra args must be part of SQL method function definition
*/
FUNCTION_DEFINE(call_text_method) {
	SpxText value;
	bool is_null;
	CALL_BASE();
	RefEtcToValue(CALL_ fcinfo, &value, &is_null, 0);		//  RefsRequired
	if (is_null)
		PG_RETURN_NULL();
	CallAssert(value.varchar);
	PG_RETURN_TEXT_P(value.varchar);
}

/* Handles most scalar-returning operations, with exceptions:
 * type input
 * SIGNATURE: refs -> NULL/Datum
 * Extra args must be part of SQL method function definition
 * Datum must represent a "Scalar" value, i.e.
 * a non-pointer, non-composite value that fits in one word
 * (Maybe 2-words on a 32-bit system??)
*/
FUNCTION_DEFINE(call_scalar_method) {
	bool is_null;
	Datum value;
	CALL_BASE();
	RefEtcToValue(CALL_ fcinfo, 0, &is_null, &value);	// RefsRequired
	if ( is_null )
		PG_RETURN_NULL();
	PG_RETURN_DATUM(value);
}

/* * Class Management */

MutableTocCaches Toc_Cache = 0;

/* Classes are cached in a single heap allocation:

	ref_count: reference counting mechanism
	schema_cache: which was current when this was loaded
	schema_path: which was current when this was loaded
	type_cache: which was current when this was loaded
	proc_cache: which was current when this was loaded
	tom_cache: which was current when this was loaded
	max_tag: the number of items in the cache
	size: the number of items in the cache
	toc: array of struct typed_object_class instances

 * Would additional indices be useful?
 * The whole thing can be freed by free(ref_count).
 */

extern void RefTocCacheCheck(CALLS_ TocCaches cache) {
	CALLS_LINK();
	SpxCheckCaches(CALL_ &cache->spx_caches);
	RefTomCacheCheck(CALL_ cache->tom_cache);
	CallAssert(cache == Toc_Cache);
}

static inline void TocCacheCheck(_CALLS_) {
	CALLS_LINK();
	RefTocCacheCheck(CALL_ Toc_Cache);
}

// Warning: within this module, result may be cast to MutableTocs
static inline Tocs GetToc(CALLS_ ref_tags tag) {
	CALLS_LINK();
	CallAssertMsg(
		tag >= 0 && tag <= Toc_Cache->max_tag, REF_TAG_FMT, tag
	);
	return &Toc_Cache->toc[tag];
}

static inline int cmp_tocs_by_class_type(const Tocs a, const Tocs b) {
	const int diff = a->table - b->table;
	return diff ? diff : a->type->oid - b->type->oid;
}

static int cmp_tocs_ptrs_by_class_type(const TocsPtrs a, const TocsPtrs b) {
	return cmp_tocs_by_class_type(*a, *b);
}

static inline int cmp_tocs_by_type_tag(const Tocs a, const Tocs b) {
	const int diff = a->type->oid - b->type->oid;
	return diff ? diff : a->tag - b->tag;
}

static int cmp_tocs_ptrs_by_type_tag(const TocsPtrs a, const TocsPtrs b) {
	return cmp_tocs_by_type_tag(*a, *b);
}

static int cmp_tocs_by_type(const Tocs a, const Tocs b) {
	return a->type->oid - b->type->oid;
}

static int cmp_tocs_ptrs_by_type(const TocsPtrs a, const TocsPtrs b) {
	return cmp_tocs_by_type(*a, *b);
}

/* returns first toc (lowest tag value) of given type */
static Tocs FirstTocByType(CALLS_ SpxTypes type) {
	struct typed_object_class target = {.type = type};
	const struct typed_object_class *const target_ptr = &target;
	const TocsPtrs start = toc_cache_by_type_tag_start(Toc_Cache), target_pp = &target_ptr;
	const Tocs *p = SPX_BSEARCH(
		target_pp, start, Toc_Cache->size, sizeof *start,
		cmp_tocs_ptrs_by_type
	);
	if (!p) return 0;
	const Tocs *pp;
	for ( pp=p ; p>start && (*--p)->type == (*pp)->type ; pp=p )
		;
	return *pp;
}

static void DebugToc(CALLS_ Tocs toc) {
	CALLS_LINK();
	if (!toc)
		CALL_DEBUG_OUT("toc NULL");
	else
		CALL_DEBUG_OUT(
	REF_TAG_FMT SPX_TYPE_FMT
	" out" SPX_PROC_FMT SPX_PLAN_FMT
	" in" SPX_PROC_FMT SPX_PLAN_FMT
	" num_ops %d",
	REF_TAG_VAL(toc->tag),	 SPX_TYPE_VAL(toc->type),
	SPX_PROC_VAL(toc->out), SPX_PLAN_VAL(toc->out_plan),
	SPX_PROC_VAL(toc->in),	SPX_PLAN_VAL(toc->in_plan),
	toc->num_ops
	);
}

FUNCTION_DEFINE(refs_tag_to_type) {
	enum {tag_arg, num_args};
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const ref_tags tag = GetArgTag(tag_arg);
	const Tocs toc = GetToc(CALL_ tag);
	CallAssertMsg(toc, REF_TAG_FMT, REF_TAG_VAL(tag));
	DebugToc(CALL_ toc);
	PG_RETURN_OID(toc->type->oid);
}

FUNCTION_DEFINE(refs_type_to_tag) {
	enum {type_arg, num_args};
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const Oid ret_type_oid = PG_GETARG_OID(type_arg);
	const SpxTypes ret_type=SpxTypeByOid(CALL_ ret_type_oid);
	CallAssertMsg(ret_type, SPX_OID_FMT, SPX_OID_VAL(ret_type_oid));
	const Tocs toc = FirstTocByType(CALL_ ret_type);
	CallAssertMsg(toc, SPX_TYPE_FMT, SPX_TYPE_VAL(ret_type));
	DebugToc(CALL_ toc);
	PG_RETURN_INT32(toc->tag);
}

static Tocs TocByTableType(CALLS_ Oid table, SpxTypes type) {
	const struct typed_object_class
		target = {.table = table, .type = type}, *const target_ptr = &target;
	const TocsPtrs start = toc_cache_by_class_type_start(Toc_Cache), target_pp = &target_ptr;
	const Tocs *const p = SPX_BSEARCH(
		target_pp, start,
		Toc_Cache->size,
		sizeof *start,
		cmp_tocs_ptrs_by_class_type
	);
	return p ? *p : 0;
}

FUNCTION_DEFINE(refs_table_type_to_tag) {
	enum {table_arg, type_arg, num_args};
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const Oid table_oid = PG_GETARG_OID(type_arg);
	const Oid type_oid = PG_GETARG_OID(type_arg);
	const SpxTypes type=SpxTypeByOid(CALL_ type_oid);
	CallAssertMsg(type, SPX_OID_FMT, SPX_OID_VAL(type_oid));
	const Tocs toc = TocByTableType(CALL_ table_oid, type);
	CallAssertMsg(toc,
		SPX_OID_FMT SPX_TYPE_FMT,
		SPX_OID_VAL(table_oid), SPX_TYPE_VAL(type) );
	DebugToc(CALL_ toc);
	PG_RETURN_INT32(toc->tag);
}

FUNCTION_DEFINE(refs_debug_tocs_by_tag) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	if ( !Toc_Cache ) {
		CALL_DEBUG_OUT("Toc_Cache NULL");
		PG_RETURN_INT32( -1 );
	}
	int count = 0;
	const Tocs start = Toc_Cache->toc;
	const Tocs end = start + Toc_Cache->max_tag + 1;
	for (Tocs p = start; p < end ; p++ )
		if (p) {
			++count;
			DebugToc(CALL_ p);
		}
	if (count != Toc_Cache->size)
		CALL_WARN_OUT(
	"count %d Toc_Cache->size %d",
	 count, Toc_Cache->size
		);
	PG_RETURN_INT32( count );
}

FUNCTION_DEFINE(refs_debug_tocs_by_type) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	if ( !Toc_Cache ) {
		CALL_DEBUG_OUT("Toc_Cache NULL");
		PG_RETURN_INT32( -1 );
	}
	TocsPtrs start = toc_cache_by_type_tag_start(Toc_Cache);
	TocsPtrs end = toc_cache_by_type_tag_end(Toc_Cache);
	for (TocsPtrs p = start; p < end ; p++ )
		DebugToc(CALL_ *p);
	PG_RETURN_INT32( Toc_Cache->size );
}

static TocCaches LoadTocs(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("==> LoadTocs");
	static char select[] =
		"SELECT DISTINCT tag_, class_, type_, out_, in_, numops_, maxtag_"
		" FROM s1_refs.tag_class_type_out_in_numops_maxtag_view"
		" ORDER BY tag_";
	enum toc_fields {
		tag_, class_,  type_,  out_, in_, numops_, maxtag_
	};
	// make the query plan
	static SpxPlans plan;
	SpxPlan0( CALL_ &plan, select );
	const struct toc_cache toc = {
		.size = SpxQueryDB(plan, NULL, REF_MAX_TAG),
		.max_tag = RowColTypedInt32(CALL_ 0, maxtag_, Ref_Tags_Type, 0)
	};
	if (toc.size != toc.max_tag + 1) {
		CALL_WARN_OUT(
									"toc.size: %d max_tag+1:  %d",
									 toc.size, toc.max_tag+1
									);
	}

	const int num_tags = toc.max_tag + 1; // since 0 is a legal tag
	CallAssertMsg( toc.size > 0 && toc.size <= num_tags,
		 "toc.size: %d, num_tags: %d", toc.size, num_tags );
	// CALL_DEBUG_OUT("toc.size %d", toc.size);

	// make room for the results
	// how might this go wrong if size != max_tag + 1 ???
	MutableTocCaches cache =
		spx_obj_alloc( toc_cache_end(&toc) - (char *) &toc );
	CallAssert(cache);
	cache->size = toc.size;
	cache->max_tag = toc.max_tag;
	cache->spx_caches = SpxCurrentCaches();
	cache->tom_cache = Tom_Cache;
	Tocs *by_type = TocsMutablePtr(toc_cache_by_type_tag_start(cache));
	Tocs *by_class = TocsMutablePtr(toc_cache_by_class_type_start(cache));
	
	Assert_SpxObjPtr_AtEnd( CALL_ cache, toc_cache_by_class_type_end(cache) );
	for (int row = 0; row < toc.size; row++) {  // load rows
		const int tag = RowColTypedInt32(CALL_ row, tag_, Ref_Tags_Type, 0);
		CallAssert(tag >= 0 && tag <= toc.max_tag);
		Assert_SpxObjPtr_In(CALL_ cache, by_type);
		Assert_SpxObjPtr_In(CALL_ cache, by_class);
		Tocs const_toc = *by_class++ = *by_type++ = &cache->toc[tag];
		MutableTocs toc = MutableToc(const_toc);
		Assert_SpxObjPtr_In(CALL_ cache, toc);
		toc->tag = tag;
		// CALL_DEBUG_OUT("row %d toc tag %d", row, toc->tag);
		toc->table = RowColTypedOid(CALL_ row, class_, Class_Type, 0);
		toc->type = SpxTypeByOid(CALL_
			RowColTypedOid(CALL_ row, type_, Type_Type, 0)
		);
		bool no_out_proc;
		const Oid out_proc =
			RowColTypedOid(CALL_ row, out_, Procedure_Type, &no_out_proc);
		toc->out = no_out_proc ? 0 : SpxProcByOid(CALL_ out_proc);
		bool no_in_proc;
		const Oid in_proc =
			RowColTypedOid(CALL_ row, in_, Procedure_Type, &no_in_proc);
		toc->in = no_in_proc ? 0 : SpxProcByOid(CALL_ in_proc);
		toc->num_ops = RowColTypedInt32(CALL_ row, numops_, Int32_Type, 0);
	}
	Assert_SpxObjPtr_AtEnd( CALL_ cache, by_class );
	SPX_QSORT(
		toc_cache_by_type_tag_start(cache), TocsMutablePtr, cache->size,
		sizeof *toc_cache_by_type_tag_start(cache), cmp_tocs_ptrs_by_type_tag
	);
	SPX_QSORT(
		toc_cache_by_class_type_start(cache), TocsMutablePtr, cache->size,
		sizeof *toc_cache_by_class_type_start(cache), cmp_tocs_ptrs_by_class_type
	);
	CALL_DEBUG_OUT("<== LoadTocs");
	return cache;
}

extern int RefLoadTocs(_CALLS_) {
	CALLS_LINK();
	const TocCaches cache = LoadTocs(_CALL_);
	SPX_OBJ_REF_DECR(Toc_Cache);
	//	Toc_Cache = SPX_OBJ_REF_INCR(cache);
	Toc_Cache = MutableTocCache(cache);
	return cache->size;
}

FUNCTION_DEFINE(unsafe_refs_load_tocs) {	// () -> integer (count)
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_refs_load_tocs();");
	SpxRequired(_CALL_);
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int count = RefLoadTocs(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32( count );
}

/* * type crefs */

/* To do: Generalize crefs so that all crefs data is accessed
 through a vector indexed by crefs_tags, then the crefs framework
can be moved to Spx!!

calls --> Spx
env --> RefEnv
from_to_len, from_,  to_ --> RefTree
node, parent, indent --> RefTree
*/

#if 0
struct spx_crefs {
	int num_crefs;
	void *cref[num_crefs];	// NULL or point to structures
};

// cref[spx_crefs_tag] would point to:
struct spx_calls_crefs {
	Calls calls;			// function call chain
};

// etc.
#endif

// consider this change:
#if 0
SpxTypes CRefs_Type, Refs_Type;
#else
SpxTypeOids CRefs_Type = {0, "crefs", NULL};
SpxTypeOids Ref_Tags_Type = {0, "ref_tags", &CRefs_Type};
SpxTypeOids Refs_Type = {0, "refs", &Ref_Tags_Type};
SpxTypeOids *const Refs_Type_List = &Refs_Type;
#endif

// crefs expresses the total context for an operation
struct crefs {
	Calls calls;			// function call chain
	size_t from_to_len;	// from_, to_ array lengths
	refs *from_, *to_;		// wicci substitution arrays
	refs *env;			// array of environment contexts
	refs node;			// current node being rendered
	refs doc;				// current document being rendered
	CRefs parent;	// when rendering hierarchical structure
	int indent;   // when rendering hierarchical structure - not yet used!
	void **extras;		// module-specific extras
};

FUNCTION_DEFINE(crefs_nil) {		// () -> crefs
	AssertThat(!SpxFuncNargs(fcinfo));
	return CrefsGetDatum(0);			// int32
}

// intentionally incomplete
FUNCTION_DEFINE(crefs_in) {		// cstring -> crefs
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CString_Type.type_oid);
	return CrefsGetDatum(0);			// int32
}

static StrPtr CRefsOut(CRefs crefs, bool isnull) {
	if ( isnull ) return "crefs(null)";
	if ( crefs == 0 ) return "crefs(0)";
	return JoinCalls(crefs->calls);
}

FUNCTION_DEFINE(crefs_out) {		// crefs -> cstring
	CALL_BASE();
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	UtilStr s = NewStr( CALL_ CRefsOut(GetArgCrefs(0), PG_ARGISNULL(0)), call_palloc );
	if (s)
		PG_RETURN_CSTRING(
				NewStr( CALL_ CRefsOut(GetArgCrefs(0), PG_ARGISNULL(0)), call_palloc )
		);
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(crefs_calls) {		// crefs -> text
	CALL_BASE();
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	StrPtr s = CRefsOut( GetArgCrefs(0), PG_ARGISNULL(0) );
	if (s)
		PG_RETURN_CSTRING(s);
	PG_RETURN_NULL(); // Currently can't happen!!
}

#if 0
FUNCTION_DEFINE(crefs_env) {		// crefs -> ctext[]
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
}
#endif

FUNCTION_DEFINE(crefs_node) {		// crefs -> ref (doc_node_refs)
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	CRefs cref = GetArgCrefs(0);
	if ( cref && cref->node )
		return RefGetDatum(cref->node);
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(try_crefs_doc) {		// crefs -> ref (doc_refs)
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	CRefs cref = GetArgCrefs(0);
	if ( cref && cref->doc )
		return RefGetDatum(cref->doc);
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(crefs_parent) {	// crefs -> ref (doc_node_refs)
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	CRefs cref = GetArgCrefs(0);
	if ( cref && cref->parent )
		return RefGetDatum(cref->parent);
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(crefs_indent) {	// crefs -> integer
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	AssertThat(SpxFuncArgType(fcinfo, 0) == CRefs_Type.type_oid);
	CRefs cref = GetArgCrefs(0);
	PG_RETURN_INT32(cref ? cref->indent : 0);
}


// To Do: Move all ChangeArray, Graft and Tree related code to RefTrees !!

#define FMT_2REFS REF_FMT " -> " REF_FMT
#define VAL_2REFS(r1, r2) REF_VAL(r1), REF_VAL(r2)

// later: use a hash table!!
static refs RefCrefsGraft(CALLS_ refs ref, CRefs crefs) { // wicci magic
	CALL_LINK();
	if ( crefs && crefs->from_to_len ) {
		CALL_DEBUG_OUT(
		 "considering " C_SIZE_FMT__ "  subs for" REF_FMT,
		 C_SIZE_VAL(crefs->from_to_len), REF_VAL(ref)
		);
		refs *const from_end = crefs->from_ + crefs->from_to_len;
		for ( refs *fp = crefs->from_ ; fp < from_end ; fp++ )
			if ( ref == *fp ) {
	const refs new_ref = crefs->to_[fp - crefs->from_];
				CALL_DEBUG_OUT(
		"wicci_magic: " FMT_2REFS, VAL_2REFS(*fp,new_ref)
	);
				return new_ref;
			}
	}
	return ref;
}

FUNCTION_DEFINE(ref_crefs_graft) {
	enum {ref_arg, crefs_arg, num_args};
	CALL_BASE();
	RefsRequired(_CALL_);		// --> RefTreeRequired!!
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const refs ref1 = GetArgRef(ref_arg);
	const CRefs crefs = GetArgCrefs(crefs_arg);
	const refs ref2 = RefCrefsGraft(CALL_ ref1, crefs);
	if ( ref1 == ref2 )
		PG_RETURN_NULL();
	return RefGetDatum(ref2);
}

/* ** tags */

FUNCTION_DEFINE(refs_base_init) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	RequireSpxSPX(_CALL_);
	SpxRequireTypes(CALL_ Refs_Type_List, false);
	RefLoadToms(_CALL_);
	RefLoadTocs(_CALL_);
	FinishSPX(CALL_ level);
	Initialize();
	PG_RETURN_CSTRING( NewStr(CALL_ refs_module_id, call_palloc) );
}

FUNCTION_DEFINE(refs_initialized) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	CALL_DEBUG_OUT("%d", MODULE_TAG(Initialized_));
	PG_RETURN_BOOL(MODULE_TAG(Initialized_));
}

/* Unsafe: Only call AFTER you've called
	 unsafe_refs_load_toms()
	 unsafe_refs_load_tocs()
 */
FUNCTION_DEFINE(unsafe_refs_initialize) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	Initialize();
	PG_RETURN_CSTRING( NewStr(CALL_ refs_module_id, call_palloc) );
}

/* * Low level Utility Functions */

FUNCTION_DEFINE(refs_tag_width) {	// void -> int32
	AssertThat(!SpxFuncNargs(fcinfo));
	PG_RETURN_INT32(REF_TAG_WIDTH);
}

FUNCTION_DEFINE(refs_min_tag) {	// void -> ref_tags
	AssertThat(!SpxFuncNargs(fcinfo));
	return TagGetDatum( REF_MIN_TAG );
}
FUNCTION_DEFINE(refs_max_tag) {	// void -> ref_tags
	AssertThat(!SpxFuncNargs(fcinfo));
	return TagGetDatum( REF_MAX_TAG );
}

FUNCTION_DEFINE(refs_min_id) {	// void -> ref_ids
	AssertThat(!SpxFuncNargs(fcinfo));
	return IdGetDatum( REF_MIN_ID );
}
FUNCTION_DEFINE(refs_max_id) {	// void -> ref_ids
	AssertThat(!SpxFuncNargs(fcinfo));
	return IdGetDatum( REF_MAX_ID );
}

/* ref (de)construction */

/* ** ref_tag(ref) ->  ref_tags */
FUNCTION_DEFINE(ref_tag) {
	AssertThat(SpxFuncNargs(fcinfo) == 1);
// Should I warn if the tag is unknown???
	return TagGetDatum( RefTag( GetArgRef(0) ) );
}

/* ** ref_id(ref) -> ref_ids	-- not a tag function! */
FUNCTION_DEFINE(ref_id) {
	AssertThat(SpxFuncNargs(fcinfo) == 1);
// Should I warn if the tag is unknown???
	return IdGetDatum( RefId( GetArgRef(0) ) );
}

/* ** unchecked_ref_from_tag_id(ref_tags, ref_ids) -> ref */
FUNCTION_DEFINE(unchecked_ref_from_tag_id) {
	enum {tag_arg, id_arg, num_args};
	AssertThat(SpxFuncNargs(fcinfo) == num_args);
	const int tag_as_int = GetArgTag(tag_arg);
	AssertThat(tag_as_int >= 0 && tag_as_int <= REF_MAX_TAG);
	const ref_tags tag = tag_as_int;
	const ref_ids  id =  GetArgId(id_arg);
	AssertThat(id >= REF_MIN_ID && id <= REF_MAX_ID);
	const refs ref = RefTagId(tag, id);
#if 0
	DEBUG_OUT(
		"%s: " REF_TAG_FMT REF_ID_FMT REF_FMT,
		__func__, tag, id, ref
	);
#endif
	return RefGetDatum( ref );
}

/* * Fundamental ref type operations: */

TAG_DEFINE(ref_nil)						// TAG --> DATUM

/* When the refs package is NOT initialized	we will accept input as
		"()" for NULL and
		"(REF_TAG_FMT__  REF_ID_FMT__) otherwise.
* This should allow normal PostgreSQL practices to backup and
   restore databases containing refs.
 * Be sure this is consistent with what call_out_method prints
   in the same situation!!
	
 call_in_method contains a lot of low-level pgsql dependencies!!
*/
FUNCTION_DEFINE(call_in_method) { // ** cstring -> some ref type
	enum {str_arg, type_arg, typemod_arg, num_op_args};
	CALL_BASE();
	CallAssert(SpxFuncNargs(fcinfo) > str_arg);
	const StrPtr str = PG_GETARG_CSTRING(str_arg);
	if ( !RefsInitialized() ) {
		// check if input is in our simple numeric format
		if ( str[0] == '(' ) {
			if ( str[1] == ')' && str[2] == '\0' )
				PG_RETURN_NULL();
			ref_tags tag;
			ref_ids id;
			const int n = sscanf(
										 str, "(" REF_TAG_FMT__ " " REF_ID_FMT__ ")",
										 &tag, &id
			);
			if (n == 2)
				return RefGetDatum( RefTagId(tag, id) );
		}
	}
	static Oid argtypes[] = { CSTRINGOID, OIDOID, INT4OID };
	SpxCheckArgsRegtypes(
		CALL_ fcinfo, spx_check_error, RAnLEN(argtypes)
	);
	const int type_mod=PG_GETARG_INT32(typemod_arg);
	if (type_mod != -1)
		CALL_DEBUG_OUT("type_mod %d", type_mod);
	const Oid ret_type_oid = PG_GETARG_OID(type_arg);
	const SpxTypes ret_type=SpxTypeByOid(CALL_ ret_type_oid);
	CallAssertMsg(ret_type, SPX_OID_FMT, SPX_OID_VAL(ret_type_oid));
	const Tocs toc = FirstTocByType(CALL_ ret_type);
	CallAssertMsg(toc, SPX_TYPE_FMT, SPX_TYPE_VAL(ret_type));
	if (! toc->in ) {
		CALL_WARN_OUT(
			"No input method for" SPX_TYPE_FMT, SPX_TYPE_VAL(ret_type)
		);
		PG_RETURN_NULL();
	}
	const int level = StartSPX(_CALL_);
	const int num_method_args = toc->in->max_args < num_op_args
		? toc->in->max_args : num_op_args;
	CallAssert(num_method_args >= toc->in->min_args);
	CallAssert(proc_init_ok == SpxRequireQueryPlan(
		CALL_ toc->in, SpxTypeOid(ret_type),
		argtypes, num_method_args, &toc->in_plan
	) );
	bool is_null;
	SpxTypeOids result_type;
	const 	int num_args = SpxFuncNargs(fcinfo);
	Datum args[num_args];
	(void) SpxFuncArgs(fcinfo, args, 0);
	const refs result = UpdateRefType(
		toc->in_plan, args, &result_type, &is_null
	);
	FinishSPX(CALL_ level);
	if ( result_type.type_oid != ret_type->oid )
		CALL_WARN_OUT(
			"Expected type " SPX_TYPE_FMT
 			" != Result type " SPX_TYPEOID_FMT
			" for %s",
			SPX_TYPE_VAL(ret_type), SPX_TYPEOID_VAL(result_type), str
		);
	if (is_null ) {
		CALL_WARN_OUT(
			"Failed input method for type " SPX_TYPE_FMT " value %s",
			SPX_TYPE_VAL(ret_type), str
		);
		PG_RETURN_NULL();
	}
	return RefGetDatum(result);
}

/* When the refs package is NOT initialized,  call_out_method will
   output refs as "()" when NULL and "(<tag> <id>)" otherwise.
 * This should allow backup and restore of refs databases in a
   numeric format.
 * Be sure this is consistent with what call_in_method reads
   in the same situation!!

 * RefOut does not return NULL for Null references, because
   it upsets PostgreSQL; when we did, this is what happened:
   select text_ref_nil(); ==>
   ERROR:  function 140870 [text_ref_out]  returned NULL
 * I'm not clear on why this is an error, although various notions
   come to mind,  e.g. it's an output function, cstring has no NULL, etc.
* So we throw a more informative exception!
*/

FUNCTION_DEFINE(call_out_method) {	//  ref -> cstring
	enum {ref_arg, num_args};
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const refs ref = GetArgRef(ref_arg);
	if ( !RefsInitialized() ) {
		// output in a portable numeric format
		if ( SpxFuncArgNull(fcinfo, ref_arg) )
			PG_RETURN_CSTRING( NewStr(CALL_ "()", call_palloc) );
		char buf[20];
		sprintf(buf, "(" REF_TAG_FMT__ " " REF_ID_FMT__ ")", REF_VAL(ref));
		PG_RETURN_CSTRING( NewStr(CALL_ buf, call_palloc) );
	}
	SpxRequired(_CALL_);
	SPX_FUNC_NUM_ARGS_IS(num_args);
	const Tocs toc = GetToc(CALL_ RefTag(ref));
	CallAssertMsg(toc, REF_FMT, REF_VAL(ref));
	CallAssertMsg(toc->out, REF_FMT, REF_VAL(ref));
	const int level = StartSPX(_CALL_);
	RefRequireQueryPlan(
		CALL_ toc->out, CString_Type,
		&Refs_Type.type_oid, 1, &toc->out_plan
	);
	Datum args[num_args];
	(void) SpxFuncArgs(fcinfo, args, 0);
	UtilStr s = SpxQueryStr(
		CALL_ toc->out_plan, args, call_SPI_palloc
	);
	FinishSPX(CALL_ level);
	if (s)
		PG_RETURN_CSTRING(s);
	EREPORT_LEVEL_MSG(
		NOTICE, "%s: " REF_FMT " --> NULL",
		__func__, REF_VAL(ref)
	);
	PG_RETURN_NULL();
}

/* ** op method dispatch with wicci magic */

enum {ref_arg, ref_env_arg, crefs_arg, num_args_with_crefs};

static CRefs FunctionGetCrefs(PG_FUNCTION_ARGS /*fcinfo*/, int arg) {
	if ( TypeOidsNull(CRefs_Type) ) {
		CALL_BASE();
		SpxRequireTypes(CALL_ Refs_Type_List, false);
	}
	AssertThat(! TypeOidsNull(CRefs_Type));
	AssertThat(SpxFuncNargs(fcinfo) > arg);
	AssertThat(SpxFuncArgType(fcinfo, arg) == CRefs_Type.type_oid);
	return GetArgCrefs(arg);
}

static Toms OpCrefsArgsToTom(
	CALLS_ const Oid op_oid, const CRefs crefs,
	Datum args[], const Datum op_args[], const int num_args
) {
	CALL_LINK();
	const SpxProcs op = SpxProcByOid(CALL_ op_oid);
	CallAssertMsg(op, SPX_OID_FMT, SPX_OID_VAL(op_oid));
	CallAssertMsg(
	num_args > ref_arg,	" num_args %d ref_arg %d", num_args, ref_arg
	 );
	const refs ref = DatumGetRef(op_args[ref_arg]);
	for ( int i = 0; i < num_args; i++)
		args[i] = (i == crefs_arg)
			? CrefsGetDatum(crefs) // right ???
			: op_args[i];
	return MethodForOpRefSpx(CALL_ op, ref, num_args);
}

static void RefEnvCrefsEtcToValue(
	PG_FUNCTION_ARGS /*fcinfo*/,		// we use this
	SpxText *text_ret,		    		// to either return this
	bool *null_ret, Datum *datum_ret	// or these
) {
	const CRefs crefs = FunctionGetCrefs(fcinfo, crefs_arg);
	CALL_CREFS(crefs);	// spx-calls.h - threads call context thru crefs!
	RefsRequired(_CALL_);
	//	CallAssert(SpxFuncStrict(fcinfo));
	CallAssert(!SpxFuncReturnsSet(fcinfo));
	const int level = StartSPX(_CALL_);
	// New boiler plate, check that it's really ok!!
	int num_args =  SpxFuncNargs(fcinfo);
	Datum args[num_args];
	Datum op_args[num_args];
	(void) SpxFuncArgs(fcinfo, op_args, 0);
	const Toms tom = OpCrefsArgsToTom(
		CALL_ SpxFuncOid(fcinfo), crefs,
		args, op_args, num_args
	);
	TomToValue(
		 CALL_ tom, op_args, num_args,
		 text_ret, null_ret, datum_ret
	);
	FinishSPX(CALL_ level);
}

FUNCTION_DEFINE(ref_env_crefs_etc_text_op) {
	SpxText value;
	RefEnvCrefsEtcToValue(fcinfo, &value, 0, 0); //  RefsRequired
	// SPX_RETURN_Text(value);
	PG_RETURN_TEXT_P(value.varchar);
}

FUNCTION_DEFINE(ref_env_crefs_etc_scalar_op) {
	bool is_null;
	Datum value;
	RefEnvCrefsEtcToValue(fcinfo, 0, &is_null, &value);	// RefsRequired
	if ( is_null )
		PG_RETURN_NULL();
	return value;
}

/* * Calling an operation with altered crefs */

// It might be better to move the opt_arg_ to the end of the oft_args
 
/* A general approach might be:
	 op_crefsl_ref_env_crefs_etc where
	 op = operator's regprocedure
	 crefsl = ( crefs-code, crefs-args ... ) ... crefs-done
	 ref_env_crefs_etc = arguments for the method
 */

// Sufficient for now: Allow extending the changeset:

// Change Arrays are O(n).  Improve later with hash table!!

static void DebugChangeArrays(CALLS_ refs *fp, refs *tp, int len)  {
	CALLS_LINK();
	for ( int i = 0; i < len; i++ ) {
		const refs f = *fp++, t = *tp++;
		CALL_DEBUG_OUT( "crefs[%d]: " FMT_2REFS, i, VAL_2REFS(f,t) );
	}
}

static inline int ArrayArgLength(ArrayType *const a, const int dim) {
	return ARR_NDIM(a) <= dim ? 0 : ARR_DIMS(a)[dim];
}

static int CheckChangeArray( CALLS_ 
	 PG_FUNCTION_ARGS /*fcinfo*/, const int arg, Str name
) {
	CALL_LINK();
	CallAssert( !PG_ARGISNULL(arg) );
	ArrayType *const a = PG_GETARG_ARRAYTYPE_P(arg);
	if ( !ARR_NDIM(a) )
	  return 0;	       // empty arrays have no dim in pgsql!!!
	CallAssertMsg(ARR_NDIM(a) == 1, "%s %d", name, ARR_NDIM(a));
	CallAssert(ARR_ELEMTYPE(a) == Refs_Type.type_oid);
	const int len = ARR_DIMS(a)[0];
	if ( ARR_HASNULL(a) ) {
		const bits8 *const bitmap = ARR_NULLBITMAP(a);
for (const bits8 *bits=bitmap; bits<bitmap+BITMAPLEN(len); bits++)
			CallAssertMsg(
	!*bits, "%s byte %d = %u", name, (int) (bits-bitmap), *bits
		 );
	}
	return len;
}

static inline void CheckChangeArrays( CALLS_ 
	 PG_FUNCTION_ARGS /*fcinfo*/, const int from_, const int to_
) {
	CALLS_LINK();
	const int from_len = CheckChangeArray(CALL_ fcinfo, from_, "from");
	const int to_len = CheckChangeArray(CALL_ fcinfo, to_, "to");
	CallAssertMsg(
	from_len == to_len,
	"from_len %d, to_len %d", from_len, to_len
	);
}

// later: use a hash table!!
static int MergeChangeArrays(
	CALLS_ refs *from, refs *to, CRefs crefs0,
	PG_FUNCTION_ARGS /*fcinfo*/, const int from_arg, const int to_arg
) {
	CALL_LINK();
	const int old_size = crefs0 ? crefs0->from_to_len : 0;
	if ( old_size ) {
		if (from) memcpy(from, crefs0->from_, old_size * sizeof *from);
		if (to) memcpy(to, crefs0->to_, old_size * sizeof *to);
	}
	ArrayType *const from_ = PG_GETARG_ARRAYTYPE_P(from_arg);
	ArrayType *const to_ = PG_GETARG_ARRAYTYPE_P(to_arg);
	const int new_size = ArrayArgLength(from_, 0);
	if (from) memcpy(
		from+old_size, ARR_DATA_PTR(from_), new_size * sizeof *from
	);
	if (to) memcpy(
		to+old_size, ARR_DATA_PTR(to_), new_size * sizeof *to
	);
	return old_size + new_size;
}

// oftd !!!
static void OftdRefEnvCrefsEtcToValue(
	PG_FUNCTION_ARGS /*fcinfo*/,		// we use this
	SpxText *text_ret,				// to either return this
	bool *null_ret, Datum *datum_ret	// or these
) {
	enum oftd_args {
		 op_arg_, froms_arg_, tos_arg_, doc_arg_, num_oft_args
	};
	const refs doc = GetArgRef(doc_arg_);
	const refs ref_node = GetArgRef(ref_arg + num_oft_args);
	const CRefs crefs0
		= FunctionGetCrefs(fcinfo, crefs_arg + num_oft_args);
	CALL_CREFS(crefs0);	// spx-calls.h - threads call context thru crefs!
	RefsRequired(_CALL_);
	//	CallAssert(SpxFuncStrict(fcinfo));
	CallAssert(!SpxFuncReturnsSet(fcinfo));
	CallAssert( SpxFuncArgType(fcinfo, op_arg_)==Procedure_Type.type_oid );
	const int num_args = SpxFuncNargs(fcinfo) - num_oft_args;
	CallAssert(SpxFuncNargs(fcinfo) > num_oft_args + ref_arg);
	CallAssert( text_ret || (null_ret && datum_ret) );
	CheckChangeArrays(CALL_ fcinfo, froms_arg_, tos_arg_);
	const int new_arrays_len = MergeChangeArrays(
	CALL_ 0, 0, crefs0, fcinfo, froms_arg_, tos_arg_
	);
	CALL_DEBUG_OUT("new_arrays_len %d", new_arrays_len);
	refs new_from[new_arrays_len], new_to[new_arrays_len];
	MergeChangeArrays(
	CALL_ new_from, new_to, crefs0, fcinfo, froms_arg_, tos_arg_
	);
	DebugChangeArrays(CALL_ new_from, new_to, new_arrays_len);
	const struct crefs crefs = (const struct crefs) {
		CALL_
		new_arrays_len, new_from, new_to,
		crefs0 ? crefs0->env : 0,
		ref_node,	// .node = ref parameter
		//	crefs0 ? crefs0->node : 0, // .parent = .node of our parent
		doc ? doc : (crefs0 ? crefs0->doc : 0),	// document ref
		crefs0 ? crefs0 : 0, // .parent = crefs of our parent
		crefs0 ? crefs0->indent + 1 : 0,
		crefs0 ? crefs0->extras : 0
	};
	Datum args[num_args];
	const int level = StartSPX(_CALL_);
	const Oid op = PG_GETARG_OID(op_arg_);
	// New boilerplate - check that it's really correct!
	int num_op_args =  SpxFuncNargs(fcinfo);
	Datum op_args[num_op_args];
	(void) SpxFuncArgs(fcinfo, op_args, 0);
	// Looks like I don't really need to have copied all the arguments??
	// go through this 
	const Toms tom = OpCrefsArgsToTom(
		CALL_ op, &crefs, args, op_args+num_oft_args, num_args
	);
	TomToValue(
		CALL_ tom, args, num_args, text_ret, null_ret, datum_ret
	);
	FinishSPX(CALL_ level);
}

// oftd !!!
FUNCTION_DEFINE(oftd_ref_env_crefs_etc_text_op) {
	SpxText value;
	OftdRefEnvCrefsEtcToValue(fcinfo, &value, 0, 0); //  RefsRequired
	// SPX_RETURN_Text(value);
	PG_RETURN_TEXT_P(value.varchar);
}

// oftd !!!
FUNCTION_DEFINE(oftd_ref_env_crefs_etc_scalar_op) {
	bool is_null;
	Datum value;
	OftdRefEnvCrefsEtcToValue(fcinfo, 0, &is_null, &value); // RefsRequired
	if ( is_null )	PG_RETURN_NULL();
	return value;
}

/* * Comparison operators */

/* Of course, we could just compare them as
 * integer values.  I'd eventually like to move
 * to hash indices and skip this btree nonsense.
 */
inline static int RefCmp(refs a, refs b) {
	const ref_tags a_tag = RefTag(a);
	const ref_tags b_tag = RefTag(b);
	const int tag_sign = (a_tag > b_tag) - (a_tag < b_tag);
	if (tag_sign)
		return tag_sign;
	const ref_ids a_id = RefId(a);
	const ref_ids b_id = RefId(b);
	return (a_id > b_id) - (a_id < b_id);
}

FUNCTION_DEFINE(ref_cmp) {
	PG_RETURN_INT32( RefCmp( GetArgRef(0), GetArgRef(1) ) );
}

/* GREG: README:

 * Postponed:

	 - Slice-based dispatching.

 */
