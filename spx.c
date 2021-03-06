static const char spx_version[] = "$Id: spx.c,v 1.3 2007/07/24 04:27:47 greg Exp greg $";
/* * Wicci Project Spx C Code Module

	Wicci Project C Code
	Enhancement to PostgreSQL Server Programming Interface

 ** Copyright

	Copyright (c) 2005-2019 J. Greg Davidson.
	You may use this software under the terms of the
	GNU AFFERO GENERAL PUBLIC LICENSE
	as specified in the file LICENSE.md included with this distribution.
	All other use requires my permission in writing.
 *
 * FIX: "SELECT *" would be better with explicit fields!
 *
 */

#include "spx.h"
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#define MODULE_TAG(name) spx_##name
#include "debug-spx.h"
#include "catalog/pg_type.h"
// #include <fmgr.h>		// for argument/result macros // in utils/builtins.h in spx.h
#include <ctype.h>
#include <locale.h>
#include <libpq/pqformat.h>	//  for send/recv functions

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* * Reference Counted Dynamic Allocation Support */

/* The Idea of TheWheel

	 This was intended as a simple Garbage Collection System
	 for Cached Data that might need to be updated even though
	 a transaction was still using it.  Given that this Cached
	 Data is local to a Database Connection, it may not really
	 be needed.  I've refactored the mechanism for tracking the use
	 of these caches to be less obtrusive (it's tucked under
	 the allocated objects) and I'm disabling the automatic
	 freeing of this memory when the reference count goes to 0.

	 A good time to throw away the old caches would be when we
	 acquire a connection for reuse and have not yet submitted
	 a new request.  At that time we could legitimately throw
	 away all existing caches and build new ones.  If we judge
	 this sufficient, we can throw away this whole mechanism.

	 This assumes that all we're doing is processing Wicci Web
	 requests.  Perhaps the scope of the Wicci Database will
	 expand to where we'll be doing more with a single
	 connection.  How would we ideally like our cache
	 management to work?

	 - jgd - 20 November 2019
 */

// objects that are not current but can't yet be freed go on TheWheel
// was static - maybe can be again??  Otherwise s/The/SpxObjHdrs/ !!!
struct spx_obj_header TheWheel = {1, &TheWheel, &TheWheel};

/* Allocate and return fresh storage of given size above
	 a header of type struct spx_obj_header linked to TheWheel
 */
void *spx_obj_alloc(size_t size) {
	// allocate storage for object on top of header
	SpxMutableObjHdrs header = calloc(size + sizeof *header, 1);
	// indicate object in use by 1 client
	header->count = 1;
	// attach new storage to TheWheel
	header->next = TheWheel.next;
	header->prev = &TheWheel;
	TheWheel.next = TheWheel.next->prev = header;
	// record how far object actually extends
	header->end_ptr = (char *) header + sizeof *header + size;
	// mark object as part of TheWheel to distinguish from any
	// other pointers
	header->the_wheel = &TheWheel;
	// return pointer to object to our client who is
	// blissfully unaware of header lurking just below it!
	return header->object;
}

// free object if reference count is now zero
// NOTE: We're currently not actually freeing anything!!
extern void SpxTryFreeOne(CALLS_ SpxMutableObjHdrs p) {
	CALL_LINK();
	if (p == 0) {
		CALL_WARN_OUT("Null Object!");
		return;
	}
	CallAssert(p->count >= 0);
	CallAssert(p->the_wheel == &TheWheel);
	if ( p->count == 0 ) {
#if 1
		CALL_WARN_OUT("Declining to deallocate %p", p);
#else
		// Detach the Heap Object from TheWheel
		p->prev->next = p->next;
		p->next->prev = p->prev;
		p->prev = p->next = 0;
		// free the storage
		free(p);
#endif
	}
}

// free any objects on TheWheel that are no longer in use
// When do we imagine we might need this??
extern void SpxTryFreeSome(_CALLS_) {
	CALL_LINK();
	// SpxMutableObjHdrs p, next;
	CALL_WARN_OUT("Declining to FreeSome");
#if 0
	for (SpxMutableObjHdrs next, p = TheWheel.next; p != &TheWheel; p = next) {
		next = p->next;
		SpxTryFreeOne(CALL_ p);
	}
#endif
}

/* * function call checking */

int SpxFuncArgs(PG_FUNCTION_ARGS, Datum arg[], char is_null[]) {
	int null_count = 0, nargs = PG_NARGS(), i;
	for (i = 0; i < nargs; i++) {
		if ( PG_ARGISNULL(i) ) {
			++null_count;
			if ( is_null ) is_null[i] = 1;
		} else {
			if ( arg ) arg[i] = PG_GETARG_DATUM(i);
			if ( is_null ) is_null[i] = 0;
		}
	}
	return null_count;
}

extern bool SpxCheckArgNonNull(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level, int arg
) {
	CALL_LINK();
	if ( SpxFuncArgNull(fcinfo, arg) )
		switch (level) {
		case spx_check_error: CALL_BUG_OUT("arg %d null", arg);
		case spx_check_warn: CALL_WARN_OUT("arg %d null", arg);
		case spx_check_break: return false;
		default:  CALL_BUG_OUT("illegal level %d", level);
		}
	return true;
}

extern bool SpxCheckArgCount(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	int min, int max	// max = -1 if none
) {
	CALL_LINK();
	const int num = SpxFuncNargs(fcinfo);
	if ( ( min != spx_check_none && num < min )
		|| ( max != spx_check_none && num > max ) )
		switch (level) {
		case spx_check_error:
			CALL_BUG_OUT("num %d min %d max %d", num, min, max);
		case spx_check_warn:
			CALL_WARN_OUT("num %d min %d max %d", num, min, max);
		case spx_check_break: return false;
		default:  CALL_BUG_OUT("illegal level %d", level);
		}
	if ( !SpxFuncStrict(fcinfo) && min != spx_check_none )
		for (int arg = 0; arg < min; arg++)
			if ( !SpxCheckArgNonNull(CALL_ fcinfo, level, arg) )
	switch (level) {
	case spx_check_error:
		CALL_BUG_OUT("arg %d null", arg);
	case spx_check_warn:
		CALL_WARN_OUT("arg %d null", arg);
	case spx_check_break: return false;
	default:  CALL_BUG_OUT("illegal level %d", level);
	}
	return true;
}

static void UnknownReport(CALLS_ int num) {
	if (num > 0) {
		CALLS_LINK();
		CALL_DEBUG_OUT(" expected %d arg types", num);
	}
}

extern bool SpxCheckArgTypeOid(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	int arg, SpxTypeOids expected, int *num_unknown
) {
	CALL_LINK();
	const Oid actual = SpxFuncArgType(fcinfo, arg);
	if ( actual == InvalidOid ) {
		if (num_unknown) ++*num_unknown;
		return true;						// not really true !!
	}
	if ( actual != expected.type_oid )
		switch (level) {
		case spx_check_error: CALL_BUG_OUT(
	FMT_LF(arg, "%d") FMT_LF(actual, SPX_OID_FMT_)
	FMT_LF(expected, SPX_TYPEOID_FMT_),
				arg, SPX_OID_VAL(actual), SPX_TYPEOID_VAL(expected)
			);
		case spx_check_warn: CALL_WARN_OUT(
	FMT_LF(arg, "%d") FMT_LF(actual, SPX_OID_FMT_)
	FMT_LF(expected, SPX_TYPEOID_FMT_),
				arg, SPX_OID_VAL(actual), SPX_TYPEOID_VAL(expected)
			);
		case spx_check_break: return false;
		default:  CALL_BUG_OUT("illegal level %d", level);
		}
	return true;
}

extern int SpxCheckArgsRegtypesMinMax(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	const Oid* types, const int num_types, int min, int max
) {
	CALL_LINK();
	if ( !SpxCheckArgCount(CALL_ fcinfo, level, min, max) )
		return 0;
	const int num_args = SpxFuncNargs(fcinfo);
	int arg = 0, unknown = 0;
	for (; arg < num_args && arg < num_types; arg++) {
		const struct spx_type_oids type_oid = {types[arg], 0};
		if ( !SpxCheckArgTypeOid(CALL_ fcinfo, level, arg, type_oid, &unknown) )
			return arg;
	}
	UnknownReport(CALL_ unknown);
	return arg;
}

extern int SpxCheckArgsMinMax(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	int min, int max, SpxTypeOids arg_type, ...
) {
	CALL_LINK();
	if ( !SpxCheckArgCount(CALL_ fcinfo, level, min, max) )
		return 0;
	int arg = 0, unknown = 0;
	va_list typeargs;
	va_start(typeargs, arg_type);
	for (;arg_type.type_oid ; arg_type=va_arg(typeargs, SpxTypeOids) ){
		if ( !SpxCheckArgTypeOid(CALL_ fcinfo, level, arg, arg_type, &unknown) )
			return arg;
		arg++;
	}
	va_end(typeargs);
	UnknownReport(CALL_ unknown);
	return arg;
}

extern int SpxCheckArgs(
	CALLS_ PG_FUNCTION_ARGS, enum spx_check_levels level,
	SpxTypeOids arg_type, ...
) {
	CALL_LINK();
	int arg = 0, unknown = 0;
	va_list typeargs;
	va_start(typeargs, arg_type);
	for (;arg_type.type_oid ; arg_type=va_arg(typeargs, SpxTypeOids) ){
		if ( !SpxCheckArgTypeOid(CALL_ fcinfo, level, arg, arg_type, &unknown) )
			return arg;
		arg++;
	}
	va_end(typeargs);
	UnknownReport(CALL_ unknown);
	return SpxCheckArgCount(CALL_ fcinfo, level, arg, arg) ? arg : 0;
}

/* * enumerations */

_Static_assert(Query_Error_Unknown + 1 ==  SPI_ERROR_TYPUNKNOWN, "TYPUNKNOWN");
_Static_assert(Query_Ok_Unknown - 1 == SPI_OK_REWRITTEN, "OK_REWRITTEN");
_Static_assert(Query_Bogus == 0, "!Query_Bogus");
#define SAME_CODE(x, y) _Static_assert((x) == (y), "SAME_CODE")
SAME_CODE(Query_Error_Typunknown, SPI_ERROR_TYPUNKNOWN);
SAME_CODE(Query_Error_Nooutfunc, SPI_ERROR_NOOUTFUNC);
SAME_CODE(Query_Error_Noattribute, SPI_ERROR_NOATTRIBUTE);
SAME_CODE(Query_Error_Transaction, SPI_ERROR_TRANSACTION);
SAME_CODE(Query_Error_Param, SPI_ERROR_PARAM);
SAME_CODE(Query_Error_Argument, SPI_ERROR_ARGUMENT);
SAME_CODE(Query_Error_Cursor, SPI_ERROR_CURSOR);
SAME_CODE(Query_Error_Unconnected, SPI_ERROR_UNCONNECTED);
SAME_CODE(Query_Error_Opunknown, SPI_ERROR_OPUNKNOWN);
SAME_CODE(Query_Error_Copy, SPI_ERROR_COPY);
SAME_CODE(Query_Error_Connect, SPI_ERROR_CONNECT);
SAME_CODE(Query_Ok_Connect, SPI_OK_CONNECT);
SAME_CODE(Query_Ok_Finish, SPI_OK_FINISH);
SAME_CODE(Query_Ok_Fetch, SPI_OK_FETCH);
SAME_CODE(Query_Ok_Utility, SPI_OK_UTILITY);
SAME_CODE(Query_Ok_Select, SPI_OK_SELECT);
SAME_CODE(Query_Ok_Selinto, SPI_OK_SELINTO);
SAME_CODE(Query_Ok_Insert, SPI_OK_INSERT);
SAME_CODE(Query_Ok_Delete, SPI_OK_DELETE);
SAME_CODE(Query_Ok_Update, SPI_OK_UPDATE);
SAME_CODE(Query_Ok_Cursor, SPI_OK_CURSOR);
SAME_CODE(Query_Ok_Insert_Returning, SPI_OK_INSERT_RETURNING);
SAME_CODE(Query_Ok_Delete_Returning, SPI_OK_DELETE_RETURNING);
SAME_CODE(Query_Ok_Update_Returning, SPI_OK_UPDATE_RETURNING);
SAME_CODE(Query_Ok_Rewritten, SPI_OK_REWRITTEN);

const struct spx_query_decodes Spx_Query_Decode[] = {
	{ Query_Error_Unknown, "Error_Unknown" },
	{ Query_Error_Typunknown, "Error_Typunknown" },
	{ Query_Error_Nooutfunc, "Error_Nooutfunc" },
	{ Query_Error_Noattribute, "Error_Noattribute" },
	{ Query_Error_Transaction, "Error_Transaction" },
	{ Query_Error_Param, "Error_Param" },
	{ Query_Error_Argument, "Error_Argument: Invalid Plan" },
	{ Query_Error_Cursor, "Error_Cursor" },
	{ Query_Error_Unconnected, "Error_Unconnected" },
	{ Query_Error_Opunknown, "Error_Opunknown" },
	{ Query_Error_Copy, "Error_Copy" },
	{ Query_Error_Connect, "Error_Connect" },
	{ Query_Bogus, "Query_Bogus: No code 0" },
	{ Query_Ok_Connect, "Ok_Connect" },
	{ Query_Ok_Finish, "Ok_Finish" },
	{ Query_Ok_Fetch, "Ok_Fetch" },
	{ Query_Ok_Utility, "Ok_Utility" },
	{ Query_Ok_Select, "Ok_Select" },
	{ Query_Ok_Selinto, "Ok_Selinto" },
	{ Query_Ok_Insert, "Ok_Insert" },
	{ Query_Ok_Delete, "Ok_Delete" },
	{ Query_Ok_Update, "Ok_Update" },
	{ Query_Ok_Cursor, "Ok_Cursor" },
	{ Query_Ok_Insert_Returning, "Ok_Insert_Returning" },
	{ Query_Ok_Delete_Returning, "Ok_Delete_Returning" },
	{ Query_Ok_Update_Returning, "Ok_Update_Returning" },
	{ Query_Ok_Rewritten, "Ok_Rewritten" },
	{ Query_Ok_Unknown, "Ok_Unknown" } // dummy end
};

/* * SpxTypes */

/* Some key statically known PostgreSQL core datatypes;
 * initialization is easy as these are given in
 *	"server/catalog/see pg_type.h"
 */
const SpxTypeOids
	Int32_Type = { INT4OID, "integer" },
	Int32_Array_Type = { INT4ARRAYOID, "integer[]" },
	Int64_Type = { INT8OID, "bigint" },
	Float_Type = { FLOAT4OID, "real" },
	Float_Array_Type = { FLOAT4ARRAYOID, "real[]" },
	Double_Type = { FLOAT8OID, "double precision" },
	Oid_Type = { OIDOID, "oid" },
	Oid_Vector_Type = { OIDVECTOROID, "oidvector" },
	Bool_Type = { BOOLOID, "bool" },
	Name_Type = { NAMEOID, "name" },
	Procedure_Type = { REGPROCEDUREOID, "regprocedure" },
	Class_Type = { REGCLASSOID, "regclass" },
	Type_Type={ REGTYPEOID, "regtype" },
	Type_Array_Type={ REGTYPEARRAYOID, "regtype[]" },
	CString_Type = { CSTRINGOID, "cstring" },
	Text_Type = { TEXTOID, "text" },
	Text_Array_Type = { TEXTARRAYOID, "text[]" },
	XML_Type = { XMLOID, "xml" },
	Unknown_Type = { UNKNOWNOID, "unknown" },
	End_Type = { 0, "end" };

/* Other important datatypes have to be resolved with a query.
	 These should be in our type cache.  For convenience, a whole
	 list of them can be resolved with one call to SpxRequireTypes.
 */

extern void SpxRequireTypes(CALLS_ SpxTypeOids *types, bool reinit) {
	CALL_LINK();
	for ( ; types; types = types->required )
		if ( reinit || !types->type_oid ) {
			const SpxTypes ourtype = SpxTypeByName(CALL_ types->type_name);
			if (! ourtype)
					CALL_BUG_OUT("No type %s", types->type_name);
			types->type_oid = ourtype->oid;
		}
}

/* * Initialization */

const SpxText Spx_Null_Text = { 0 };

// int  spx_init__ = 0;
int spx_connected__ = 0;

/* * Spx Schemas */

/* Schemas are cached in a single heap allocation.
		First, the contents of the declared fields:

	ref_count: reference counting mechanism
	min_id, max_id: the minimum and maximum ids in use
	size: the number of items in the cache
	by_name: points to index array sorted by name
	by_oid: points to index array sorted by oid
	by_id: index array sorted by id

		Second, three arrays of pointers serving as index arrays:
		 
	by_id[0..max_id] : sorted by our ids, some may be NULL
	by_name[0..size-1] : sorted by name
	by_oid[0..size-1] : sorted by oid

	 Third, a private heap of size variable-sized structures:
	struct spx_schema
	+ room for its schema_name
	+ extra bytes to align on a word boundary

	The whole thing can be freed by freeing the ref_count.
*/

/* Schema Paths are cached in a single heap allocation.

		 ref_count: reference counting mechanism
		 schema_cache: the schema_cache current when we built this path
		 size: the number of schemas in the path
		 path[0..size-1]: an array of pointers into the schema_cache

	The whole thing can be freed by freeing the ref_count.
*/

static size_t SchemaNameDelim(
	char *const dst, size_t const size,
	const SpxSchemas s, const Str name, const char d
) {
#if 0
	// crashes server!!!
	const size_t len = snprintf(dst, size, "%s.%s%c", s->name, name, d);
#else
	const size_t len = snprintf(dst, size, "%s%c", name, d);
#endif
	return len;
}

static size_t AddSchemaNameDelim(
	CALLS_ char **const dst, size_t *const size,
	const SpxSchemas s, const Str name, const char d
) {
	CALL_LINK();
	const size_t len = SchemaNameDelim(*dst, *size, s, name, d);
	if (*dst) {
		if (len < *size) {
			*dst += len; *size -= len;
			CallAssert( **dst == '\0' );
		} else {
			CALL_WARN_OUT(
					C_SIZE_FMT(len) C_SIZE_FMT(*size),
					C_SIZE_VAL(len), C_SIZE_VAL(*size)
			);
			*dst = 0;
		}
	}
	return len;
}

extern size_t SpxProcSig(CALLS_ char *dst, size_t size, SpxProcs p) {
	CALL_LINK();
	size_t total = AddSchemaNameDelim(
	CALL_ &dst, &size, p->schema, spx_proc_name(p), '('
	);
	const SpxTypes *tp=spx_proc_arg_types(p), *const end=tp+p->max_args;
	CallAssert(tp);
	for ( ; tp + 1 < end ; tp++ ) {
		CallAssert(*tp);
		CallAssert((*tp)->schema);
		CallAssert((*tp)->name);
		total += AddSchemaNameDelim(
	CALL_ &dst, &size, (*tp)->schema, (*tp)->name, ','
		);
	}
	if ( tp >= end ) {
		CALL_DEBUG_OUT("void function");
		total += snprintf(dst, size, ")");
	}
	else
		total += AddSchemaNameDelim(	// the last argument
	CALL_ &dst, &size, (*tp)->schema, (*tp)->name, ')'
		);
	return total;
}

/* The last fixed-size field in a proc cache is by_oid[size]
 * followed immedately by the variable-length proc structures. */
static inline SpxProcs procs_cache_procs(SpxProcCache cache) {
	return (SpxProcs) (spx_proc_cache_by_oid(cache) + cache->size);
}

/* p points to a fully initialized struct spx_proc
 * the last field of which is its '\0'-terminated name
 * plus spx_align_size, of total size name_size
 * the next structure should begin after that name field
 * we also put LOTS of checking and debugging code here
*/
static SpxProcs spx_proc_end(CALLS_ SpxProcs p, int name_size) {
 CALL_LINK();
	CallAssert( name_size % sizeof (int) == 0 );
	const int len = strlen( spx_proc_name(p) ) + 1;
	CallAssert( len <= name_size );
	CallAssert( spx_aligned_size(len) == name_size );
	if ( DebugLevel() > 1 ) {
		char buf[ 1 + SpxProcSig(CALL_ 0, 0, p) ];
		CallAssert( sizeof buf == 1 + SpxProcSig(CALL_ RAnLEN(buf), p) );
		CALL_DEBUG_OUT("passed %s", buf );
	}
	return (SpxProcs) (spx_proc_name(p) + name_size);
}

SpxSchemaCaches Spx_Schema_Cache;
SpxSchemaPaths Spx_Schema_Path;

static int cmp_name_with_schema(
	const StrPtr key, const SpxSchemasPtrs spp
) {
	return strcoll(key, (*spp)->name);
}

#if 0
static int cmp_schemas_by_name(const SpxSchemasPtrs a, const SpxSchemasPtrs b) {
	return strcoll((*a)->name, (*b)->name);
}
#endif

static int cmp_schemas_by_oid(const SpxSchemasPtrs a, const SpxSchemasPtrs b) {
	return (*a)->oid - (*b)->oid;
}

extern void SpxSchemaCacheCheck(CALLS_ SpxSchemaCaches cache) {
	CALLS_LINK();
	CallAssert(cache == Spx_Schema_Cache);
}

extern void SpxSchemaPathCheck(CALLS_ SpxSchemaPaths path) {
	CALLS_LINK();
	SpxSchemaCacheCheck(CALL_ path->schema_cache);
	CallAssert(path == Spx_Schema_Path);
}

static inline void SchemaPathCheck(_CALLS_) {
	CALLS_LINK();
	SpxSchemaPathCheck(CALL_ Spx_Schema_Path);
}

static SpxSchemaPaths LoadSchemaPath(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("==> LoadSchemaPath");
	static SpxPlans plan;
	SpxPlan0( CALL_ &plan, "SELECT id::int4 FROM our_existing_namespaces"	);
	enum {id_, name_, oid_ };
	const int num_rows = SpxQueryDB(plan, NULL, MAX_SCHEMAS);
	const SpxMutableSchemaPaths sp =
		spx_obj_alloc( sizeof *sp + num_rows * sizeof *sp->path);
	sp->size = num_rows;
	sp->schema_cache = Spx_Schema_Cache;
	CallAssert(num_rows <= sp->schema_cache->size);
	SpxSchemasMutablePtrs path_ptr = sp->path;
	int row;
	for (row = 0; row < num_rows; row++) {
		const int id = RowColTypedInt32(CALL_ row, id_, Int32_Type, NULL);
		CALL_DEBUG_OUT("row %d schema id %d", row, id);
		CallAssertMsg( id >= sp->schema_cache->min_id,
			"id %d, min_id %d", id, sp->schema_cache->min_id );
		CallAssertMsg( id <= sp->schema_cache->max_id,
			"id %d > max_id %d", id, sp->schema_cache->max_id );
		const SpxSchemas s = SpxSchemaById(CALL_ id);
		CallAssertMsg(s, "No schema with id %d", id);
		CallAssertMsg(s->oid, "No schema oid for id %d", id);
		CALL_DEBUG_OUT("row %d schema %d %d %s", row, s->id, s->oid, s->name);
		*path_ptr++ = s;
	}
	Assert_SpxObjPtr_AtEnd(CALL_ sp, path_ptr );
	CALL_DEBUG_OUT("<== LoadSchemaPath");
	return sp;
}

extern int SpxLoadSchemaPath(_CALLS_) {
	CALL_LINK();
	const SpxSchemaPaths p = LoadSchemaPath(_CALL_);
	SPX_OBJ_REF_DECR(Spx_Schema_Path);
	Spx_Schema_Path = p;
	return p->size;
}

static SpxSchemaCaches LoadSchemas(_CALLS_) {
	enum schema_fields {	id_, name_, oid_ };
	static const char select_schemas[] =
		"SELECT id::int4, schema_name::text, oid"
		" FROM our_namespaces_by_name";
	CALL_LINK();
	CALL_DEBUG_OUT("==> LoadSchemas");
	static SpxPlans plan;
	SpxPlan0( CALL_ &plan, select_schemas);
	const int num_rows = SpxQueryDB(plan, NULL, MAX_SCHEMAS);
	int min_id = RowColTypedInt32(CALL_ 0, id_, Int32_Type, NULL);
	int max_id = 0;
	size_t sum_text = 0;
	int row;
	for (row = 0; row < num_rows; row++) {
		const int id =  RowColTypedInt32(CALL_ row, id_, Int32_Type, NULL);
		const int name_len = RowColTextLen(CALL_ row, name_, NULL);
		if (id < min_id) min_id = id;
		if (id > max_id) max_id = id;
		sum_text += spx_aligned_size(name_len+1);
	}
	const int by_id_len = max_id + 1;
	const SpxMutableSchemaCaches cache = spx_obj_alloc(
	sizeof *cache
		+ by_id_len * sizeof *cache->by_id		// schema ptr
		+ 2 * num_rows * sizeof *cache->by_id	// schema ptr
		+ num_rows * sizeof **cache->by_id		// schema structure
		+ sum_text
	);
	CallAssert(cache);
	cache->min_id = min_id;
	cache->max_id = max_id;
	cache->size = num_rows;
	SpxSchemasMutablePtrs name_p = SpxSchemasMutablePtr(spx_schema_cache_by_name(cache));
	SpxSchemasMutablePtrs oid_p = SpxSchemasMutablePtr(spx_schema_cache_by_oid(cache));
	SpxMutableSchemas p = SpxMutableSchema(spx_schema_cache_schemas(cache));
	size_t room_left = sum_text;
	for (row = 0; row < num_rows; row++) {
		p->id = RowColTypedInt32(CALL_ row, id_, Int32_Type, NULL);
		CallAssertMsg( p->id >= 0 && p->id <= max_id,
			"row %d bad schema id %d", row, p->id );
		bool oid_vey_is_null;
		p->oid = RowColTypedOid(CALL_ row, oid_, Oid_Type, &oid_vey_is_null);
		const int name_len = RowColTextCopy(CALL_ row, name_, p->name, room_left, NULL);
		if (oid_vey_is_null) {
			CALL_WARN_OUT("Schema id %d, OID VEY, %s!", p->id, p->name);
			p->oid = 0;
		} else
			CALL_DEBUG_OUT("Schema id %d, Oid %d, %s!", p->id, p->oid, p->name);
		CallAssert(name_len < room_left);
		room_left -= spx_aligned_size(name_len+1);
		cache->by_id[p->id] = *name_p++ =  *oid_p++ = p;
		p = SpxMutableSchema(spx_schema_next(p));
	}
	// Check our arithmetic on the allocation and incrementing:
	Assert_SpxObjPtr_AtEnd(CALL_ cache, p);
	if ( room_left != 0 )
		CALL_WARN_OUT("room_left: %zu", room_left);
	CALL_DEBUG_OUT("About to qsort the Schema Cache by_oid");
	SPX_QSORT( spx_schema_cache_by_oid(cache), SpxSchemasMutablePtr, cache->size,
						 sizeof (SpxSchemas),	cmp_schemas_by_oid );
	CALL_DEBUG_OUT("<== LoadSchemas");
	return cache;
}

extern int SpxLoadSchemas(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("Before LoadSchemas");
	const SpxSchemaCaches cache = LoadSchemas(_CALL_);
	SPX_OBJ_REF_DECR(Spx_Schema_Cache);
	//	Spx_Schema_Cache = SPX_OBJ_REF_INCR(cache);
	Spx_Schema_Cache = cache;
	return cache->size;
}

#if 0
SpxSchemas SchemaById(int id) {
	const struct spx_schema target = {.id = id}, *target_ptr = &target;
	SpxSchemas *p = SPX_BSEARCH(
	&target_ptr,
	Spx_Schemas_By_Id,
	Schema_Array_Len,
	sizeof *Spx_Schemas_By_Id,
	cmp_schemas_by_id
	);
	return p ? *p : 0;
}
#endif

static SpxSchemas SchemaByOid(
	CALLS_ Oid oid, SpxSchemaCaches cache
) {
	const struct spx_schema target = {.oid = oid}, *const target_ptr = &target;
	const SpxSchemasPtrs target_pp = &target_ptr, p = SPX_BSEARCH(
	target_pp,
	spx_schema_cache_by_oid(cache),
	cache->size,
	sizeof (SpxSchemas),
	cmp_schemas_by_oid
	);
	return p ? *p : 0;
}

extern SpxSchemas SpxSchemaByOid(CALLS_ Oid oid) {
	CALLS_LINK();
	return SchemaByOid(CALL_ oid, Spx_Schema_Cache);
}

static SpxSchemas SchemaByName(
	CALLS_ StrPtr name, SpxSchemaCaches cache
) {
	const SpxSchemasPtrs p = SPX_BSEARCH(
	name,
	spx_schema_cache_by_name(cache),
	cache->size,
	sizeof (SpxSchemaCaches),
	cmp_name_with_schema
	);
	return p ? *p : 0;
}

extern SpxSchemas SpxSchemaByName(CALLS_ StrPtr name) {
	CALLS_LINK();
	return SchemaByName(CALL_ name, Spx_Schema_Cache);
}

// For debugging only!!
FUNCTION_DEFINE(unsafe_spx_load_schemas) {
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_spx_load_schemas();");
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int32 num_schemas = SpxLoadSchemas(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32(num_schemas);
}

// For debugging only!!
FUNCTION_DEFINE(unsafe_spx_load_schema_path) {
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_spx_load_schema_path();");
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int32 num_schemas = SpxLoadSchemaPath(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32(num_schemas);
}

/* * Spx Types */

/* Types are cached in a single heap allocation.
		First, the contents of the declared fields:

	ref_count: reference counting mechanism
	schema_cache: which was current when this was loaded
	schema_path: which was current when this was loaded
	size: the number of items in the cache
	by_oid: points to index array sorted by oid
	by_name: points to index array sorted by name

		Second, the arrays of pointers serving as index arrays:
		 
	by_oid[0..size-1] : sorted by oid
	by_name[0..size-1] : sorted by name

	 Third, a private heap of size variable-sized structures:
	struct spx_type
	+ room for its name
	+ extra bytes to align on a word boundary

	The whole thing can be freed by freeing the ref_count.
*/

SpxTypeCache Spx_Type_Cache;
typedef struct spx_type_cache *TypeCachePtr;

static int cmp_name_with_type(const StrPtr a, const SpxTypesPtrs b) {
	return strcoll(a, (*b)->name);
}

#if 0
static int cmp_types_by_name(const StrPtr a, const SpxTypesPtrs b) {
	return strcoll((*a)->name, (*b)->name);
}
#endif

static int cmp_types_by_oid(const SpxTypesPtrs a, const SpxTypesPtrs b) {
	return (*a)->oid - (*b)->oid;
}

extern void SpxTypeCacheCheck(CALLS_ SpxTypeCache cache) {
	CALLS_LINK();
	SpxSchemaCacheCheck(CALL_ cache->schema_cache);
	SpxSchemaPathCheck(CALL_ cache->schema_path);
	CallAssert(cache == Spx_Type_Cache);
}

static inline void TypeCacheCheck(_CALLS_) {
	CALLS_LINK();
	SpxTypeCacheCheck(CALL_ Spx_Type_Cache);
}

// Compare with LoadSchemas above
static SpxTypeCache LoadTypes(_CALLS_) {
 CALL_LINK();
	CALL_DEBUG_OUT("==> LoadTypes");
	static SpxPlans plan;
	SpxPlan0( CALL_ &plan, "SELECT DISTINCT"
		" oid_, name_::text, schema_id_::int4, typlen_::int4, typbyval_"
		" FROM s1_refs.type_view__ ORDER BY name_" );
	enum { oid_, name_, schema_id_, typlen_, typbyval_ };
	const int num_rows = SpxQueryDB(plan, NULL, MAX_TYPES);
	size_t sum_text = 0;
	int row;
	for (row = 0; row < num_rows; row++) {
		const int name_len = RowColTextLen(CALL_ row, name_, NULL);
		sum_text += spx_aligned_size(name_len + 1);
	}
	SpxMutableTypes p;			// We have 3 arrays of size TypePtrs all pointing into our arena
	const TypeCachePtr cache = spx_obj_alloc(
		sizeof *cache	+	num_rows * (3 * sizeof p + sizeof *p) + sum_text
	);
	CallAssert(cache );
	cache->schema_cache = Spx_Schema_Cache;
	cache->schema_path = Spx_Schema_Path;
	cache->size = num_rows;
	p = SpxMutableType(spx_type_cache_types(cache));
	SpxTypes *by_name = cache->by_name,
		*by_oid = SpxTypesMutablePtr(spx_type_cache_by_oid(cache));
	size_t room_left = sum_text;
	for (row = 0; row < num_rows; row++) {
		bool oid_vey_is_null;
		p->oid = RowColTypedOid(CALL_ row, oid_, Type_Type, &oid_vey_is_null);
		CallAssert(p->schema = SpxSchemaById(
			 CALL_ RowColTypedInt32(CALL_ row, schema_id_, Int32_Type, 0)
		) );
		p->len = RowColTypedInt32(CALL_ row, typlen_, Int32_Type, NULL);
		p->by_value = RowColBool(CALL_ row, typbyval_, NULL);
		const int name_len = RowColTextCopy(CALL_ row, name_, p->name, room_left, NULL);
		if (oid_vey_is_null)
			CALL_BUG_OUT(
				"Type: Row %d, Oid Vey, schema %d %d %s, len %d, byval %d, %s",
				row, p->schema->id, p->schema->oid, p->schema->name,
				p->len, p->by_value, p->name
			);
		else
			CALL_DEBUG_OUT(
				"Type: Row %d, Oid %d, schema %d %d %s, len %d, byval %d, %s",
				row, p->oid, p->schema->id, p->schema->oid, p->schema->name,
				p->len, p->by_value, p->name
		);
		CallAssert(name_len < room_left);
		room_left -= spx_aligned_size(name_len+1);
		*by_name++ = *by_oid++ = p;
		p = SpxMutableType(spx_type_next(p));
	}
	// Check our arithmetic on the allocation and incrementing:
	Assert_SpxObjPtr_AtEnd( CALL_ cache, p );
	if ( room_left != 0 )
		CALL_WARN_OUT("room_left: %zu", room_left);
	CALL_DEBUG_OUT("about to qsort %d rows by oid", num_rows);
	SPX_QSORT( spx_type_cache_by_oid(cache), SpxTypesMutablePtr, cache->size,
						 sizeof *spx_type_cache_by_oid(cache), cmp_types_by_oid );
	CALL_DEBUG_OUT("<== LoadTypes");
	return cache;
}

extern int SpxLoadTypes(_CALLS_) {
	CALL_LINK();
	const SpxTypeCache cache = LoadTypes(_CALL_);
	SPX_OBJ_REF_DECR(Spx_Type_Cache);
	//	Spx_Type_Cache = SPX_OBJ_REF_INCR(cache);
	Spx_Type_Cache = cache;
	return cache->size;
}

extern SpxTypes TypeByOid(CALLS_ Oid oid, SpxTypeCache cache) {
	const struct spx_type target = {.oid = oid}, *const target_ptr = &target;
	const SpxTypesPtrs target_pp = &target_ptr, p = SPX_BSEARCH(
		target_pp, spx_type_cache_by_oid(cache), cache->size,
		sizeof *spx_type_cache_by_oid(cache), cmp_types_by_oid
	);
	return p ? *p : 0;
}

extern SpxTypes SpxTypeByOid(CALLS_ Oid oid) {
	CALLS_LINK();
	TypeCacheCheck(_CALL_);
	return TypeByOid(CALL_ oid, Spx_Type_Cache);
}

static SpxTypes TypeByName(CALLS_ StrPtr name, SpxTypeCache cache) {
	SpxTypesPtrs p = SPX_BSEARCH(
	name, cache->by_name+0, cache->size,
	sizeof *cache->by_name, cmp_name_with_type
	);
	return p ? *p : 0;
}

extern SpxTypes SpxTypeByName(CALLS_ StrPtr name) {
	CALLS_LINK();
	TypeCacheCheck(_CALL_);
	return TypeByName(CALL_ name, Spx_Type_Cache);
}

// For debugging only!!
FUNCTION_DEFINE(unsafe_spx_load_types) {
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_spx_load_types();");
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int32 load_count = SpxLoadTypes(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32(load_count);
}

/* * Spx Procedures */

/* Procs are cached in a single heap allocation.
 * First, the contents of the declared fields:

	ref_count: reference counting mechanism
	schema_cache: which was current when this was loaded
	schema_path: which was current when this was loaded
	type_cache: which was current when this was loaded
	size: the number of items in the cache
	by_oid: points to index array sorted by oid
	by_name: points to index array sorted by name

		Second, the arrays of pointers serving as index arrays:
		 
	by_name[0..size-1] : sorted by name
	by_oid[0..size-1] : sorted by oid

	 Third, a private heap of size variable-sized structures:
	struct spx_proc
	+ room for its name
	+ extra bytes to align on a word boundary

	The whole thing can be freed by freeing the ref_count.
*/

SpxProcCache Spx_Proc_Cache;
typedef struct spx_proc_cache *ProcCachePtr;

static int cmp_name_with_proc(const StrPtr a, const SpxProcsPtrs b) {
	return strcoll(a, spx_proc_name(*b));
}

#if 0
static int cmp_procs_by_name(const SpxProcsPtrs a, const SpxProcsPtrs b) {
	return strcoll(spx_proc_name(*a), spx_proc_name(*b));
}
#endif

static int cmp_procs_by_oid(const SpxProcsPtrs a, const SpxProcsPtrs b) {
	return (*a)->oid - (*b)->oid;
}

static inline void dst_size_accum_len(
	char **dst, size_t *size, size_t *accum, size_t len
) {
	if (*dst) *dst += len;
	if (*size > len) *size -= len;
	*accum += len;
}

/* When a cast is necessary and cannot be done using the
	 PosgreSQL postfix-cast operator :: because of a space in
	 the destination type name, generate the beginning of a
	 standard SQL CAST expression.
*/
static size_t SpxCallStrCastPre(
	char *dst, size_t size,
	Oid from_oid,SpxTypeOids to
) {
	if ( from_oid != to.type_oid && strchr(to.type_name, ' ') )
		return snprintf(dst, size, "CAST( ");
	return 0;
}

/* Generate a cast if necessary, preferring the PostgreSQL
	 postfix-cast operator :: if there are no spaces in the
	 destination type name.
*/
static size_t SpxCallStrCastPost(
	char *dst, size_t size,
	Oid from_oid,SpxTypeOids to
) {
	if ( from_oid == to.type_oid )
		return 0;
	return snprintf(
	dst, size,
	strchr(to.type_name, ' ') ? "%s AS %s )" : "%s::%s",
	from_oid == CString_Type.type_oid
	&& (
		to.type_oid == Float_Type.type_oid
		|| to.type_oid == Double_Type.type_oid
	)
	? "::text" : "",	// PostgreSQL(8.4.2) can't cast cstring to floating
	to.type_name
	);
}

/* Watch out for PostgreSQL limitation on casts:
ERROR:	42846: cannot cast type cstring to double precision
IN:		SELECT s3_more.get_float_ref(CAST( $1 AS double precision ))
FROM:	SELECT '1234.56'::float_refs;
LOCATION:  SpxPlanQuery, spx.c:1528
FIX:	Cast to text first - see "SpxCallStrCastPost" above
 */

/* Construct an SQL command which calls the given procedure p with
	 arguments of the indicated arg_types and returns a result of type
	 ret_type.  Casts are applied where the types differ.  We assume
	 that the casts are possible.  */
extern size_t SpxCallStr(
	CALLS_ char *dst, size_t size, SpxProcs p,
	SpxTypeOids ret_type, const Oid arg_types[], int num_args
) {
	CALL_LINK();
	CallAssert(num_args >= p->min_args);
	CallAssert(num_args <= p->max_args);
	size_t accum = 0;
	dst_size_accum_len( &dst, &size, &accum, snprintf(dst, size, "SELECT ") );
	dst_size_accum_len( &dst, &size, &accum, SpxCallStrCastPre(
		 dst, size, p->return_type->oid, ret_type
	)  );
	dst_size_accum_len( &dst, &size, &accum, snprintf(
		 dst, size, "%s.%s(",
		 p->schema->name, spx_proc_name(p)
	)  );
	for (int i=0, n=1; i < num_args; i=n++) {
		dst_size_accum_len( &dst, &size, &accum, SpxCallStrCastPre(
			 dst, size, arg_types[i], SpxTypeOid(spx_proc_arg_types(p)[i])
		)  );
		dst_size_accum_len( &dst, &size, &accum, snprintf(
			dst, size, "$%d", n
		) );
		dst_size_accum_len( &dst, &size, &accum, SpxCallStrCastPost(
			 dst, size, arg_types[i], SpxTypeOid(spx_proc_arg_types(p)[i])
		)  );
		dst_size_accum_len( &dst, &size, &accum, snprintf(
			dst, size, "%s", n < num_args ? "," : ")"
		) );
	}    
	dst_size_accum_len( &dst, &size, &accum, SpxCallStrCastPost(
		 dst, size, p->return_type->oid, ret_type
	)  );
	return accum;
}

FUNCTION_DEFINE(spx_proc_call_proc_str) {
	enum {caller_arg, callee_arg, num_args};
	CALL_BASE();
	SpxCheckArgs(	CALL_ fcinfo, 0, Procedure_Type, Procedure_Type, End_Type );
	const SpxProcs caller = SpxProcByOid(CALL_ PG_GETARG_OID(caller_arg) );
	CallAssert(caller);
	const SpxProcs callee = SpxProcByOid(CALL_ PG_GETARG_OID(callee_arg) );
	CallAssert(callee);
	const int num_call_args = caller->max_args > callee->max_args
		? callee->max_args : caller->max_args;
	char buf[1+SpxCallStr(
												CALL_ 0, 0, callee, SpxTypeOid(caller->return_type),
												caller->arg_type_oids, num_call_args
	)];
	SpxCallStr(
						 CALL_ buf, sizeof buf, callee,
						 SpxTypeOid(caller->return_type),
						 caller->arg_type_oids, num_call_args
	);
	PG_RETURN_CSTRING( NewStr(CALL_ buf, call_palloc) );
}

extern SpxPlans SpxProcTypesQueryPlan(
	CALLS_ SpxProcs p,  SpxTypeOids ret_type,
	const Oid arg_types[], int num_args,
	ProcInitResult *status
) {
	CALL_LINK();
	CallAssert(p->oid);
	CallAssert(p->schema);
	CallAssert(p->schema->name);
	// CallAssert(p->return_type); // what about void??
	const size_t buf_len = 1+SpxCallStr(
		CALL_ 0, 0, p, ret_type, arg_types, num_args
	);
	char buf[buf_len];
	CallAssert(buf_len == 1+SpxCallStr(
		CALL_ buf, buf_len, p, ret_type, arg_types, num_args
	) );
	const SpxPlans plan =  SpxPlanQuery( CALL_ buf, arg_types, num_args );
	CallAssert(!SpxPlanNull(plan) || status);
	if (status) *status = !SpxPlanNull(plan) ? proc_init_ok : proc_init_ptr_null;
	return plan;
}

#if 0
static SpxProcs SameOldProc(CALLS_ SpxProcs new_proc) {
	SpxProcs old_proc = SpxProcByOid(CALL_  new_proc->oid);
	if (!old_proc) return NULL;
	if (old_proc->schema != new_proc->schema) return NULL;
	if (old_proc->return_type != new_proc->return_type) return NULL;
	if (old_proc->max_args != new_proc->max_args) return NULL;
#ifndef _STRIP_META_
	if ( strcoll(old_proc->name, spx_proc_name(new_proc)) != 0 )
		return NULL;
#endif
	int n;
	for (n = 0; n < old_proc->max_args; n++)
		if (old_proc->arg_type_oids[n] != new_proc->arg_type_oids[n] )
			return NULL;
	return old_proc;
}
#endif
	
static int RowColOidVector(
	CALLS_ const int row, const int col, Oid *dst, const int max_len
) {
	CALL_LINK();
	const Datum d = RowColTypedDatum(CALL_ row, col, Oid_Vector_Type, 0);
	ArrayType *const atp = DatumGetArrayTypeP(d);
	CallAssert(ARR_NDIM(atp) == 1);
	CallAssert(!ARR_HASNULL(atp));
	CallAssert(ARR_ELEMTYPE(atp) == Oid_Type.type_oid);
	CallAssert(ARR_LBOUND(atp)[0] == 0);
	const int dim = ARR_DIMS(atp)[0];
	CallAssert(dim <= max_len);
	const Oid *const oids = (Oid *) ARR_DATA_PTR(atp), *oidp = oids;
	while (oidp < oids + dim) {
		CALL_DEBUG_OUT(
	"%d: " SPX_OID_FMT, (int) (oidp - oids), SPX_OID_VAL(*oidp) );
		*dst++ = *oidp++;
	}
	return dim;
}

extern void SpxProcCacheCheck(CALLS_ SpxProcCache cache) {
	CALLS_LINK();
	SpxSchemaCacheCheck(CALL_ cache->schema_cache);
	SpxSchemaPathCheck(CALL_ cache->schema_path);
	SpxTypeCacheCheck(CALL_ cache->type_cache);
	CallAssert(cache == Spx_Proc_Cache);
}

static inline void ProcCacheCheck(_CALLS_) {
	CALLS_LINK();
	SpxProcCacheCheck(CALL_ Spx_Proc_Cache);
}

extern void SpxCheckCaches(CALLS_ SpxCaches caches) {
	CALLS_LINK();
	SpxSchemaCacheCheck(CALL_ caches->schema_cache);
	SpxSchemaPathCheck(CALL_ caches->schema_path);
	SpxTypeCacheCheck(CALL_ caches->type_cache);
	SpxProcCacheCheck(CALL_ caches->proc_cache);
}

static SpxProcCache LoadProcs(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("==> LoadProcs");
	static SpxPlans plan;
	SpxPlan0(	CALL_ &plan, "SELECT DISTINCT"
			" oid_, name_, schema_id::integer, rettype_,"
			" readonly_, minargs_, maxargs_, argtypes_,"
			" name_size_, sum_text_, sum_nargs_"
	" FROM s1_refs.proc_view" " ORDER BY name_"			);
	enum	{	oid_, name_, schema_id_, rettype_,
			readonly_, minargs_, maxargs_, argtypes_,
			name_size_, sum_text_, sum_nargs_		};
	//  const int prior_debug_level = DebugSetLevel(3);
	const int num_rows = SpxQueryDB(plan, NULL, MAX_PROCS);
	const int sum_text = RowColTypedInt64(CALL_ 0, sum_text_, Int64_Type, 0);
	const int sum_nargs=RowColTypedInt64(CALL_ 0, sum_nargs_, Int64_Type, 0);
	int name_size_accum = 0, max_args_accum = 0;
	SpxMutableProcs p;
	const ProcCachePtr cache = spx_obj_alloc(
	sizeof *cache
	+ num_rows * ( 2 * sizeof *cache->by_name + sizeof *p )
	+ sum_nargs * (sizeof *p->arg_type_oids + sizeof (SpxTypes *) )
	+ sum_text  );
	CallAssert(cache );
	cache->schema_cache = Spx_Schema_Cache;
	cache->schema_path = Spx_Schema_Path;
	cache->type_cache = Spx_Type_Cache;
	cache->size = num_rows;
	SpxProcs
		*by_name = SpxMutableProcsPtr(cache->by_name),
		*by_oid = SpxMutableProcsPtr(spx_proc_cache_by_oid(cache));
	p = SpxMutableProc(procs_cache_procs(cache));
	for (int row = 0; row < num_rows; row++) {
		p->oid = RowColTypedOid(CALL_ row, oid_, Procedure_Type, 0);
		CallAssert( p->schema = SpxSchemaById( CALL_
	RowColTypedInt32(CALL_ row, schema_id_, Int32_Type, 0)  ) );
		CallAssert( p->return_type = SpxTypeByOid( CALL_
	RowColTypedOid(CALL_ row, rettype_, Type_Type, 0)  ) );
		p->readonly = RowColBool(CALL_ row, readonly_, NULL);
		p->min_args = RowColTypedInt32(CALL_ row, minargs_, Int32_Type, NULL);
		p->max_args = RowColTypedInt32(CALL_ row, maxargs_, Int32_Type, NULL);
		CallAssert( ( max_args_accum += p->max_args ) <= sum_nargs );
		const int name_size = RowColTypedInt32(CALL_ row, name_size_, Int32_Type, NULL);
		CallAssert( ( name_size_accum += name_size ) <= sum_text );
		// Tmp -> Session Yes???
		strcpy( spx_proc_name(p), RowColStr(CALL_ row, name_, SessionAlloc) );
		const int num_arg_types = RowColOidVector(
				 CALL_ row, argtypes_, p->arg_type_oids, p->max_args );
		CallAssert(num_arg_types == p->max_args);
		for (int n = 0; n < p->max_args; n++)
			CallAssert( SpxTypesMutablePtr(spx_proc_arg_types(p))[n] =
		SpxTypeByOid(CALL_ p->arg_type_oids[n]) );
		*by_name++ =  *by_oid++ = p;
		p = SpxMutableProc(spx_proc_end(CALL_ p, name_size)); // includes checking & debugging
	}
	CallAssertMsg(
								SpxObjPtr_AtEnd(CALL_ cache, p),
								C_PTRDIFF_FMT__ ";"
								FMT_lf(max_args_accum,"%d") FMT_lf(sum_nargs,"%d")  ";"
								FMT_lf(name_size_accum,"%d") FMT_lf(sum_text,"%d"),
								C_PTRDIFF2_VAL(spx_obj_end(CALL_ cache), p),
								max_args_accum, sum_nargs,
								name_size_accum, sum_text
	);
	CallAssertMsg( max_args_accum == sum_nargs,
	FMT_lf(max_args_accum,"%d") FMT_lf(sum_nargs,"%d"),
	max_args_accum, sum_nargs );
	CallAssertMsg(name_size_accum == sum_text,
	FMT_lf(name_size_accum,"%d") FMT_lf(sum_text,"%d"),
	name_size_accum, sum_text );
	SPX_QSORT( spx_proc_cache_by_oid(cache), SpxMutableProcsPtr, cache->size,
						 sizeof (SpxProcs), cmp_procs_by_oid );
	//  DebugSetLevel(prior_debug_level);
	CALL_DEBUG_OUT("<== LoadProcs");
	return cache;
}

extern int SpxLoadProcs(_CALLS_) {
	CALL_LINK();
	const SpxProcCache cache = LoadProcs(_CALL_);
	SPX_OBJ_REF_DECR(Spx_Proc_Cache);
	//	Spx_Proc_Cache = SPX_OBJ_REF_INCR(cache);
	Spx_Proc_Cache = cache;
	return cache->size;
}

static SpxProcs ProcByOid(CALLS_ Oid oid, SpxProcCache cache) {
	const struct spx_proc target = {.oid = oid}, *const target_ptr = &target;
	const SpxProcsPtrs target_pp = &target_ptr, p = SPX_BSEARCH(
	target_pp, spx_proc_cache_by_oid(cache), cache->size,
	sizeof (SpxProcs), cmp_procs_by_oid
	);
	return p ? *p : 0;
}

extern SpxProcs SpxProcByOid(CALLS_ Oid oid) {
	CALLS_LINK();
	ProcCacheCheck(_CALL_);
	return ProcByOid(CALL_ oid, Spx_Proc_Cache);
}

static SpxProcs ProcByName(CALLS_ StrPtr name, SpxProcCache cache) {
	SpxProcsPtrs p = SPX_BSEARCH(
		name, cache->by_name + 0, cache->size,
		sizeof *cache->by_name, cmp_name_with_proc
	);
	return p ? *p : 0;
}

extern SpxProcs SpxProcByName(CALLS_ StrPtr name) {
	CALLS_LINK();
	ProcCacheCheck(_CALL_);
	return ProcByName(CALL_ name, Spx_Proc_Cache);
}

// For debugging only!!
FUNCTION_DEFINE(unsafe_spx_load_procs) {
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_spx_load_procs();");
	SPX_FUNC_NUM_ARGS_IS(0);
	const int level = StartSPX(_CALL_);
	const int32 load_count = SpxLoadProcs(_CALL_);
	FinishSPX(CALL_ level);
	PG_RETURN_INT32(load_count);
}

// For debugging only!!
// https://www.postgresql.org/docs/12/spi-examples.html
FUNCTION_DEFINE(spx_test_select) {
    char *command;
    int cnt;
    int ret;
    uint64 proc;

    /* Convert given text object to a C string */
    command = text_to_cstring(PG_GETARG_TEXT_PP(0));
    cnt = PG_GETARG_INT32(1);

    SPI_connect();
    ret = SPI_exec(command, cnt);
    proc = SPI_processed;
    /*
     * Print any returned rows with elog(INFO).
     */
    if (ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        char buf[8192];
        uint64 j;

        for (j = 0; j < proc; j++) {
            HeapTuple tuple = tuptable->vals[j];
            int i;

            for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
                snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), " %s%s",
                        SPI_getvalue(tuple, tupdesc, i),
                        (i == tupdesc->natts) ? " " : " |");
            elog(INFO, "spx_test_select: %s", buf);
        }
    }
    SPI_finish();
    pfree(command);
    PG_RETURN_INT64(proc);
}

/* * General Initialization */

extern struct spx_caches SpxCurrentCaches(void) {
	return (struct spx_caches){
		Spx_Schema_Cache, Spx_Schema_Path,
		Spx_Type_Cache, Spx_Proc_Cache
	};
}

// @pre( InSPX(_CALL_) )
// we should check this!!!
extern void SpxInitSPX(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("==>");
	CallAssert( setlocale(LC_COLLATE, 0) );
	CALL_DEBUG_OUT("~~> SpxLoadSchemas");
	SpxLoadSchemas(_CALL_);
	CALL_DEBUG_OUT("~~> SpxLoadSchemaPath");
	SpxLoadSchemaPath(_CALL_);
	CALL_DEBUG_OUT("~~> SpxLoadTypes");
	SpxLoadTypes(_CALL_);
	CALL_DEBUG_OUT("~~> SpxLoadProcs");
	SpxLoadProcs(_CALL_);
	Initialize();
	CALL_DEBUG_OUT("<==");
}

// @pre( ! InSPX(_CALL_) at this level )
// we should check this!!!
extern void SpxInit(_CALLS_) {
	CALL_LINK();
	CALL_DEBUG_OUT("+StartSPX(_CALL_);");
	const int level = StartSPX(_CALL_);
	// Shouldn't this be conditional on NOT ALREADY BEING INITIALIZED???
	SpxInitSPX(_CALL_);
	FinishSPX(CALL_ level);
}

FUNCTION_DEFINE(spx_init) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	SpxInit(_CALL_);
	// CallAssert(spx_version); // always true
	PG_RETURN_CSTRING( NewStr(CALL_ spx_version, call_palloc) );
}

FUNCTION_DEFINE(spx_initialized) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	CALL_DEBUG_OUT("%d", MODULE_TAG(Initialized_));
	PG_RETURN_BOOL(MODULE_TAG(Initialized_));
}

// For debugging only!!
/* Only call this AFTER ensuring that all
 spx initializations have been done, i.e.
* spx_collate_locale()
* unsafe_spx_load_schemas()
* spx_debug_schemas()
* unsafe_spx_load_schema_path()
*/
FUNCTION_DEFINE(unsafe_spx_initialize) {
	CALL_BASE();
	CALL_DEBUG_OUT("+unsafe_spx_initialize();");
	SPX_FUNC_NUM_ARGS_IS(0);
	// CallAssert(spx_version); // always true
	Initialize();
	PG_RETURN_CSTRING( NewStr(CALL_ spx_version, call_palloc) );
}

// For debugging only!!
FUNCTION_DEFINE(spx_collate_locale) {
	CALL_BASE();
	CALL_DEBUG_OUT("+spx_collate_locale();");
	SPX_FUNC_NUM_ARGS_IS(0);
	PG_RETURN_CSTRING(NewStr(CALL_ setlocale(LC_COLLATE, 0), call_palloc));
}

/* Utility Function Definitions */

static int spx_stack_limit_ = 1;

FUNCTION_DEFINE(spx_stack_limit) {
	const int limit = spx_stack_limit_;
	AssertThat(SpxFuncNargs(fcinfo) < 2);
	if (SpxFuncNargs(fcinfo) > 0) {
		const int new_limit = PG_GETARG_INT32(0);
		AssertThat(new_limit > 0);
		spx_stack_limit_ = new_limit;
	}
	PG_RETURN_INT32(limit);
}

// connect to SPI context
extern int StartSPX(_CALLS_) {
	CALL_LINK();
	AssertNotSPX(_CALL_);
	const int status = SPI_connect();
	CallAssertMsg(status >= 0, "status %d", status);
	return IncSPX(_CALL_);
}
// connect to SPI context 

extern int FinishSPX(CALLS_ int start_level) {
	CALL_LINK();
	AssertSPX(_CALL_);
	CallAssert( start_level == StackedSPX(_CALL_) );
	const int status = SPI_finish();
	CallAssertMsg(status >= 0, "status %d", status);
	DecSPX(_CALL_);
	AssertNotSPX(_CALL_);
	return StackedSPX(_CALL_);
}

/* Queries */

#if 0

/* Within the context of a query which has returned num_oids
	 rows and where where column col holds namespace oids, pick
	 the row number which corresponds to the earliest hit in
	 the current search path, or -1 if none.
*/
static int ResolveSchema(CALLS_ int col, int num_oids) {
	CALL_LINK();
	typedef SpxSchemas * SpxSchemaPtrs;
	const SpxSchemas *const end = Spx_Schema_Path->path+Spx_Schema_Path->size;
	const SpxSchemas *p, *best = end;
	Oid oid;
	int row, best_row = -1;
	for (row = 0; row < num_oids; row++) {
		oid = RowColTypedOid(CALL_ row, col, Oid_Type, NULL);
		// CALL_DEBUG_OUT("Candidate schema %d", oid );
		for (p = Spx_Schema_Path->path; p < best; p++) {
			CALL_DEBUG_OUT("Path schema %d", (*p)->oid );
			if ( (*p)->oid == oid ) {
	best = p;
	best_row = row;
	CALL_DEBUG_OUT("Best row so far %d", best_row );
	break;
			}
		}
	}
	CALL_DEBUG_OUT("Returning best row %d", best_row );
	return best_row;
}

#endif

#ifndef NO_TEST_FRAME

// For debugging only!!
FUNCTION_DEFINE(spx_debug_schemas) {
	extern SpxSchemaCaches Spx_Schema_Cache;
	extern SpxSchemaPaths Spx_Schema_Path;
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	const SpxSchemaCaches c = Spx_Schema_Cache;
	CallAssert(c);
	CALL_DEBUG_OUT(
		"Schema Min Id = %d, Max Id = %d, Array Len = %d",
		c->min_id, c->max_id, c->size);
	const SpxSchemaPaths p = Spx_Schema_Path;
	if (!p)
		CALL_WARN_OUT("Spx_Schema_Path NULL");
	else
		CALL_DEBUG_OUT("Schema Path Len = %d", p->size);
	CALL_DEBUG_OUT("Schemas By Id:");
	for (int id = c->min_id; id <= c->max_id; id++) {
		const SpxSchemas s = SpxSchemaById(CALL_ id);
		if (s) {
	CALL_DEBUG_OUT(
		"Schema id = %2d, oid = %6d, name = %s",
		s->id, s->oid, s->name
	);
	CallAssert(s->id == id);
		} else {
			SpxSchemas s = Spx_Schema_Cache->by_id[id];
			CALL_DEBUG_OUT(
				"Schema id = %2d, name = %s",
				s->id, s->name ?: "NULL"
			);
		}
	}
	CALL_DEBUG_OUT("Schemas By Oid:");
	SpxSchemasPtrs s;
	Oid max_oid = 0;
	SpxSchemasPtrs by_oid = spx_schema_cache_by_oid(c);
	for ( s = by_oid ; s < by_oid + c->size ; s++ ) {
		CALL_DEBUG_OUT(
	"Schema id = %2d, oid = %6d, name = %s",
	(*s)->id, (*s)->oid, (*s)->name
		);
		CallAssert((*s)->oid >= max_oid);
		max_oid = (*s)->oid;
	}
	CALL_DEBUG_OUT("Schemas By Name:");
	StrPtr max_name = "";
	SpxSchemasPtrs by_name = spx_schema_cache_by_name(c);
	for (s = by_name ; s < by_name+c->size ; s++ ) {
		CALL_DEBUG_OUT(
	"Schema id = %2d, oid = %6d, name = %s",
	(*s)->id, (*s)->oid, (*s)->name
		);
		CallAssert(strcoll((*s)->name, max_name) >= 0);
		max_name = (*s)->name;
	}
	PG_RETURN_INT32(c->size);
}

FUNCTION_DEFINE(spx_schema_by_id) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	const SpxSchemas s = SpxSchemaById(CALL_ PG_GETARG_INT32(0) );
	if (s) {
		CallAssert(s->name);
		PG_RETURN_CSTRING( NewStr(CALL_ s->name, call_palloc) );
	}
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(spx_schema_by_oid) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	const SpxSchemas s = SpxSchemaByOid(CALL_ PG_GETARG_OID(0) );
	if (s) PG_RETURN_CSTRING( NewStr(CALL_ s->name, call_palloc) );
	PG_RETURN_NULL();
}

FUNCTION_DEFINE(spx_schema_path_by_oid) {
	AssertThat(SpxFuncNargs(fcinfo) == 1);
	const SpxSchemaPaths p = Spx_Schema_Path;
	const Oid oid = PG_GETARG_OID(0);
	int i;
	for (i = 0; i < p->size && p->path[i]->oid != oid; i++)
		;
	PG_RETURN_INT32( i == p->size ? -1 : i );
}

FUNCTION_DEFINE(spx_debug_types) {
	const SpxTypeCache c = Spx_Type_Cache;
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	CALL_DEBUG_OUT(
		"Type Array Len = %d", c->size);
	CALL_DEBUG_OUT("Types By Oid:");
	const SpxTypes *p;
	Oid max_oid = 0;
	SpxTypesPtrs by_oid = spx_type_cache_by_oid(c);
	for ( p = by_oid ; p < by_oid + c->size ; p++ ) {
		CALL_DEBUG_OUT(
	"Type oid = %6d, name = %s",
	(*p)->oid, (*p)->name
		);
		CallAssert((*p)->oid >= max_oid);
		max_oid = (*p)->oid;
	}
	CALL_DEBUG_OUT("Types By Name:");
	StrPtr max_name = "";
	for ( p=c->by_name ; p < c->by_name+c->size ; p++ ) {
		CALL_DEBUG_OUT(
	"Type oid = %6d, name = %s",
	(*p)->oid, (*p)->name
		);
		CallAssert(strcoll((*p)->name, max_name) >= 0);
		max_name = (*p)->name;
	}
	PG_RETURN_INT32(c->size);
}

FUNCTION_DEFINE(spx_type_by_oid) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	const SpxTypes t = SpxTypeByOid(CALL_  PG_GETARG_OID(0) );
	if (t) PG_RETURN_CSTRING( NewStr(CALL_ t->name, call_palloc) );
	PG_RETURN_NULL();
}

void DebugShowProc(CALLS_ SpxProcs p) {
	CALL_LINK();
	char sig[1 + SpxProcSig(CALL_ 0, 0, p)];
	SpxProcSig(CALL_ RAnLEN(sig), p);
	CALL_DEBUG_OUT(
	SPX_OID_FMT "%s%s min_args %d",
	p->oid, sig, p->readonly ? " READONLY" : "", p->min_args
	);
}

FUNCTION_DEFINE(spx_debug_procs) {
	const SpxProcCache c = Spx_Proc_Cache;
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(0);
	CALL_DEBUG_OUT(
		"Proc Array Len = %d", c->size);
	CALL_DEBUG_OUT("Procs By Oid:");
	const SpxProcs *p, *const by_oid = spx_proc_cache_by_oid(c);
	for (p=by_oid; p < by_oid+c->size; p++) {
		DebugShowProc(CALL_ *p);
		if (p > by_oid) CallAssert(p[0]->oid >= p[-1]->oid);
	}
	CallAssert(p - by_oid == c->size);
	CALL_DEBUG_OUT("Procs By Name:");
	for ( p=c->by_name ; p<c->by_name + c->size ; p++ ) {
		DebugShowProc(CALL_ *p);
		if (p > c->by_name)
			CallAssert( strcoll(spx_proc_name(p[0]), spx_proc_name(p[-1])) >= 0 );
	}
	CallAssert(p - c->by_name == c->size);
	PG_RETURN_INT32(c->size);
}

FUNCTION_DEFINE(spx_proc_by_oid) {
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	const SpxProcs p = SpxProcByOid( CALL_ PG_GETARG_OID(0) );
	if (!p) PG_RETURN_NULL();
	char buf[ 1 + SpxProcSig(CALL_ 0, 0, p) ];
	CALL_DEBUG_OUT(C_SIZE_FMT(sizeof buf), C_SIZE_VAL(sizeof buf));
	CallAssert( sizeof buf == 1 + SpxProcSig(CALL_ buf, sizeof buf, p) );
	PG_RETURN_CSTRING( NewStr(CALL_ buf, call_palloc) );
}

#endif

/* planned queries */

// Prefer (const Oid arg_types[]) if SPI API gets fixed
// create query plan using type oids
extern SpxPlans SpxPlanQuery(
	CALLS_ StrPtr sql, const Oid arg_types[], int num_arg_types
) {
	CALL_LINK();
	AssertSPX(_CALL_);
	SpxPlans plan;
	plan.num_args = 0;
	StrPtr sp;
	CALL_DEBUG_OUT( "%s", sql);
	for (sp = sql; *sp; sp++) if (*sp == '$') plan.num_args++;
	CallAssert(plan.num_args == num_arg_types);
	// I'm paranoid about passing a null arg_types
	CallAssert( !num_arg_types || arg_types );
	static Oid no_arg_types[] = { 0 };
	// arg 3 of ‘SPI_prepare’ won't take a ptr to const !!
	Oid *const paranoia = num_arg_types ? (Oid *) arg_types : no_arg_types;
	plan.plan = SPI_prepare(sql, num_arg_types, paranoia);
	CallAssertMsg( !SpxPlanNull(plan),
		"SPI_prepare(%s) failed, SPI_result = %d: %s",
		sql, SPI_result, SpxQueryDecode(SPI_result) );
	plan.plan = SPI_saveplan(plan.plan);
	CALL_DEBUG_OUT( "%s -> " SPX_PLAN_FMT, sql, SPX_PLAN_VAL(plan) );
	return plan;
}

extern void SpxPlanNvargs(
	CALLS_ SpxPlans *plan, StrPtr sql_str,
	size_t n, SpxTypeOids arg0, ...
) {
	CALL_LINK();
	va_list args;
	Oid arg_types[n];

	if  ( SpxPlanNull(*plan) ) {
		CallAssert(n >= 0);
		if ( n > 0 ) {
			va_start(args, arg0);
			for ( size_t i = 0; i < n; i++ ) {
	const SpxTypeOids t = va_arg(args, SpxTypeOids);
				arg_types[i] = t.type_oid;
			}
			va_end(args);
		}
		*plan = SpxPlanQuery(CALL_ sql_str, arg_types, n);
	}
}

// col is 0-based in Spx, 1-based in SPI!
// If it might return NULL, is_null_ret must not be null!
extern Datum RowColDatum( CALLS_ const int row, const int col,
					 SpxTypeOids *type_ret, bool *is_null_ret ) {
	const int col1 = col+1;
	CALL_LINK();
	AssertSPX(_CALL_);
	CALL_DEBUG_OUT( "row %d col %d", row, col );
	CallAssertMsg( SPI_processed > row,
		"row %d, col %d, SPI_processed %d",
		row, col, (int) SPI_processed	);
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	SpxTypeOids type, *const type_ptr = type_ret ?: &type;
	type_ptr->type_oid = SPI_gettypeid(SPI_tuptable->tupdesc, col1);
	CallAssertMsg(SPI_tuptable, "row %d, col %d", row, col );
	const Datum result = SPI_getbinval(
		SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col1,	is_null_ptr	);
	CallAssert( !*is_null_ptr || is_null_ret );
	CallAssertMsg( SPI_result != SPI_ERROR_NOATTRIBUTE,
		"row %d, col %d", row, col );
	CallAssertMsg( SPI_tuptable, "%d, %d", row, col );
	CALL_DEBUG_OUT( "%d, %d -> %s::%s",
		row, col,
		SPI_fname(SPI_tuptable->tupdesc, col1),		// field name
		SPI_gettype(SPI_tuptable->tupdesc, col1)	// type name
	);
	if ( *is_null_ptr ) {
		CALL_DEBUG_OUT( "%d, %d -> NULL", row, col );
		CallAssert(is_null_ret);
	}
	return result;
}

// col is 0-based in Spx, 1-based in SPI!
// If it might return NULL, is_null_ret must not be null!
extern Datum RowColTypedDatum( CALLS_ int row, int col,
			 SpxTypeOids expected_type, bool *is_null_ret ) {
	CALL_LINK();
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	SpxTypeOids result_type;
	const Datum d = RowColDatum(CALL_ row, col, &result_type, is_null_ptr);
	CallAssertMsg(
		result_type.type_oid == expected_type.type_oid,
		"row %d, col %d, result_type %d, expected_type %d",
		row, col, result_type.type_oid, expected_type.type_oid
	);
	if (*is_null_ptr)	CallAssert(is_null_ret);
	return d;
}

extern int32 RowColInt32( CALLS_ int row, int col,
					 SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALL_LINK();
	const Datum d = RowColDatum(CALL_ row, col, result_type_ret, is_null_ret);
	CALL_DEBUG_OUT( "--> %d", DatumGetInt32(d) );
	return DatumGetInt32(d);
}

// will this work on a 32-bit machine???
extern int64 RowColInt64( CALLS_ int row, int col,
					 SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALL_LINK();
	const Datum d = RowColDatum(CALL_ row, col, result_type_ret, is_null_ret);
	CALL_DEBUG_OUT( "--> %ld", DatumGetInt64(d) );
	return DatumGetInt64(d);
}

extern int32 RowColTypedInt32(
	CALLS_ int row, int col, SpxTypeOids expected, bool *is_null_ret
) {
	CALL_LINK();
	const Datum d = RowColTypedDatum(CALL_ row, col, expected, is_null_ret);
	CALL_DEBUG_OUT( "--> %d", DatumGetInt32(d) );
	return DatumGetInt32(d);
}

extern int64 RowColTypedInt64(
	CALLS_ int row, int col, SpxTypeOids expected, bool *is_null_ret
) {
	CALL_LINK();
	const Datum d = RowColTypedDatum(CALL_ row, col, expected, is_null_ret);
	CALL_DEBUG_OUT( "--> %ld", DatumGetInt64(d) );
	return DatumGetInt64(d);
}

#if 0
// stupid ideas!!
extern int32 RowColIfInt32(
	CALLS_ int row, int col, SpxTypeOids *result_type_ret, int32 or_else
) {
	CALL_LINK();
	bool is_null;
	const int32 i = RowColInt32(CALL_ row, col, result_type_ret, &is_null);
	return is_null ? or_else : i;
}

extern int64 RowColIfInt64(
	CALLS_ int row, int col, SpxTypeOids *result_type_ret, int64 or_else
) {
	CALL_LINK();
	bool is_null;
	const int64 i = RowColInt64(CALL_ row, col, result_type_ret, &is_null);
	return is_null ? or_else : i;
}
#endif

extern bool RowColBool(CALLS_ int row, int col, bool *is_null_ret) {
	CALL_LINK();
	const Datum d = RowColTypedDatum(CALL_ row, col, Bool_Type, is_null_ret);
	CALL_DEBUG_OUT( "--> %s", DatumGetBool(d) ? "true" : "false" );
	return DatumGetBool(d);
}

// Experimenting with handling NULLs more explicitly!!
extern Oid RowColOid( CALLS_ int row, int col,
				 SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALL_LINK();
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	SpxTypeOids result_type, *const result_type_ptr = result_type_ret ?: &result_type;
	const Datum d = RowColDatum(CALL_ row, col, result_type_ptr, is_null_ptr);
	CallAssert(!*is_null_ptr || is_null_ret);
	if (*is_null_ptr)
		CALL_DEBUG_OUT( "--> NULL" );
	else
		CALL_DEBUG_OUT( "--> %d::%d",
										(int) DatumGetObjectId(d), (int) result_type.type_oid );
	return *is_null_ptr ? 0 : DatumGetObjectId(d);
}

// Experimenting with handling NULLs more explicitly!!
extern Oid RowColTypedOid(
	CALLS_ int row, int col, SpxTypeOids expected, bool *is_null_ret
) {
	CALL_LINK();
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	const Datum d = RowColTypedDatum(CALL_ row, col, expected, is_null_ptr);
	CallAssert(!*is_null_ptr || is_null_ret);
	if (*is_null_ptr)
		CALL_DEBUG_OUT( "--> NULL" );
	else
		CALL_DEBUG_OUT( "--> %d", (int) DatumGetObjectId(d) );
	return *is_null_ptr ? 0 : DatumGetObjectId(d);
}

extern StrPtr RowColStrPtr(CALLS_ int row, int col) {
	CALL_LINK();
	AssertSPX(_CALL_);
/*
 * NULL if the column is null, colnumber is out of range (SPI_result is
 * set to SPI_ERROR_NOATTRIBUTE), or no no output function available
 * (SPI_result is set to SPI_ERROR_NOOUTFUNC)
*/
	SPI_result = 0; // necessary???
	const StrPtr s = SPI_getvalue(
		SPI_tuptable->vals[row],  SPI_tuptable->tupdesc, col+1
	);
	CallAssertMsg( SPI_result != SPI_ERROR_NOATTRIBUTE,
			 "%d, %d out of range", row, col );
	CallAssertMsg(
	SPI_result != SPI_ERROR_NOOUTFUNC, "row %d, col %d", row, col );
	CALL_DEBUG_OUT( "--> %s", s ?: "NULL" );
	return s;
}

extern StrPtr RowColStr(CALLS_ int row, int col, ALLOCATOR_PTR(alloc)) {
	CALLS_LINK();
	CallAssert(alloc);
	const StrPtr s = RowColStrPtr(CALL_ row, col);
	return alloc ? NewStr( CALL_ s, alloc ) : s;
}

#if 0
// Deprecated!
// This seems to be quite a mess!!  We're not even using alloc!!
extern SpxText RowColText(CALLS_ int row, int col, ALLOCATOR_PTR(alloc)) {
	CALL_LINK();
	const StrPtr s = RowColStrPtr(CALL_  row, col);
	WARN_OUT("%s: row %d, col %d, value %s", __func__, row, col, s ?: "NULL");
	text *const tp = CStringToText(CALL_ s, alloc);
	// if ( DebugLevel() )
	{ // paranoid testing: !!
		char *const s2 = text_to_cstring(tp);
		CallAssert(!strcmp(s ?: "NULL", s2 ?: "NULL"));
		pfree(s2);
  }
	return (SpxText){tp};
}
#endif

#if 0
extern SpxText RowColText(CALLS_ int row, int col, bool *is_null_ret) {
	CALL_LINK();
	CallAssert(is_null_ret);
	SpxTypeOids actual_type;
	SpxText ret;
	const Datum d = RowColDatum(CALL_ row, col, &actual_type, is_null_ret);
	if ( !*is_null_ret && actual_type.type_oid != Text_Type.type_oid ) {
		CALL_WARN_OUT("actual regtype is %d", actual_type.type_oid);
		*is_null_ret = 1;
		ret.varchar = 0;
		return ret;
	}
	ret.varchar = DatumGetTextPP(d);
	if (*is_null_ret != !ret.varchar) // equivalent??
		CALL_WARN_OUT("is_null_ret %d, ret: %p", *is_null_ret, ret.varchar);
	if ( DebugLevel() ) {
		if ( *is_null_ret )
			CALL_DEBUG_OUT("--> NULL");
		else {
			TEXT_BUFFER_TMP_STR(ret.varchar, str)
			CALL_DEBUG_OUT("--> %s", str);
		}
	}
	return ret;
}
#endif

// we're copying a whole tuple when we're only interested in one field
// unfortunately, the current PostgreSQL SPI API makes it hard to copy just one field
// fortunately, we're usually using this on a singleton tuple
extern SpxText RowColText(CALLS_ int row, int col, bool *is_null_ret) {
	CALL_LINK();
	SpxText ret;
	int col1 = col + 1;
	CallAssert(is_null_ret);
	int num_cols = SPI_tuptable->tupdesc->natts;
	CallAssertMsg( row >= 0 && row < SPI_processed,
		"row %d, col %d, SPI_processed %d",
		row, col, (int) SPI_processed	);
	CallAssertMsg( col >= 0 && col < num_cols,
		"row %d, col %d, num_cols %d",
		row, col, num_cols	);
	SpxTypeOids actual_type;
	actual_type.type_oid = SPI_gettypeid(SPI_tuptable->tupdesc, col1);
	if ( actual_type.type_oid != Text_Type.type_oid ) {
		CALL_WARN_OUT("actual regtype is %d", actual_type.type_oid);
		*is_null_ret = 1;
		ret.varchar = 0;
		return ret;
	}
	HeapTuple ht = SpxCopyTuple(CALL_ row);
	const Datum d = SpxTupleField(CALL_ ht,  col, Text_Type, is_null_ret);
	ret.varchar = *is_null_ret ? (text *) NULL : DatumGetTextPP(d);
	return ret;
}

// Returns 0 for NULL
// OK to not provide an is_null_ret pointer
// Are we sure this works with toasted or packed datums?
extern size_t RowColTextLen(CALLS_ int row, int col, bool *is_null_ret) {
	CALL_LINK();
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	const Datum text_datum = RowColTypedDatum(CALL_ row, col, Text_Type, is_null_ptr);
	if (is_null) {
		CALL_DEBUG_OUT("--> NULL");
		return 0;
	}
	size_t len = SpxTextOctetLen(SpxDatumVarlena(text_datum));
	CALL_DEBUG_OUT("--> %zu", len);
	return len;
}

#if 0
// Copy the varlena datum into fresh storage
struct varlena *
SpxCopyDatum(CALLS_ struct varlena *datum, ALLOCATOR_PTR(alloc)) {
	CALL_LINK();
	size_t size = VARSIZE( SpxVarlenaDatum(datum) );
	return memcpy( CALL_ALLOC(alloc, size), datum, size );
}

// Copy the text object into fresh storage
// We suspect this does NOT work with toasted or packed datums!!
SpxText
SpxCopyText(CALLS_ SpxText old_text, ALLOCATOR_PTR(alloc)) {
	CALLS_LINK();
	SpxText new_text;
  if ( DebugLevel() ) {
		TEXT_BUFFER_TMP_STR(old_text.varchar, str);
		CALL_DEBUG_OUT("old text %s '%s'", *str ? "=" : "IS", *str ? str : "NULL");
	}
	new_text.varchar = SpxCopyDatum(CALL_ old_text.varchar, alloc);
  if ( DebugLevel() ) {
		TEXT_BUFFER_TMP_STR(new_text.varchar, str);
		CALL_DEBUG_OUT("new text %s '%s'", *str ? "=" : "IS", *str ? str : "NULL");
	}
	return new_text;
}
#endif

// Buffer size must be greater than 0
// Returns string length, not length stored!
// Always '\0'-terminates string, even if truncated or NULL!
// OK to not provide an is_null_ret pointer
extern size_t RowColTextCopy(
	CALLS_ int row, int col, char *buffer, size_t buffer_size, bool *is_null_ret
) {
	CALL_LINK();
	bool is_null, *const is_null_ptr = is_null_ret ?: &is_null;
	const Datum text_datum = RowColTypedDatum(CALL_ row, col, Text_Type, is_null_ptr);
	if (is_null) {
		CALL_DEBUG_OUT("--> NULL --> NUL");
		CallAssert(buffer_size > 0);
		*buffer = '\0';
		return 0;
	}
	const size_t text_len = SpxTextOctetLen(SpxDatumVarlena(text_datum));
	text *const text_ptr = DatumGetTextPP(text_datum);
	text_to_cstring_buffer(text_ptr, buffer, buffer_size);
	CALL_DEBUG_OUT("--> room: %zu, len: %zu, text: %s", buffer_size, text_len, buffer);
	return text_len;
}

// Copies tuple into upper memory context and returns a
// pointer to the HeapTupleData structure.  Fields can be
// accessed as a Datum with SpxTupleField while still in the
// SPI context.  They are then still valid afterwards.
HeapTuple SpxCopyTuple(CALLS_ int row ) {
	CALL_LINK();
	AssertSPX(_CALL_);
	CALL_DEBUG_OUT( "row %d", row );
	CallAssertMsg( SPI_processed > row,
		"row %d, SPI_processed %d",
		row, (int) SPI_processed	);
	return SPI_copytuple(SPI_tuptable->vals[row]);
}

/* Return a specific field from a tuple which is either part
	 of a result set or has been copied from a result set
	 which is still present, i.e. we need to still be in the
	 same SPI context of the query which generated the
	 original tuple.  We currently do thorough checking, even
	 though it has probably been done by our callers.  If this
	 is an extended attribute field, i.e. one whose value does
	 not fit into one machine word, it will case to be valid
	 once its memory context is reclaimed, i.e. if its part of
	 a result set, it will be invalid once you call
	 SPI_finish, so consider using SpxCopyTuple first!
 */
Datum SpxTupleField(
	CALLS_ HeapTuple tup, int col, SpxTypeOids expected_type, bool *is_null_ret
) {
	const int col1 = col + 1;
	CALL_LINK();
	AssertSPX(_CALL_);
	CALL_DEBUG_OUT( "col %d", col );
	CallAssert( is_null_ret );
	int num_cols = SPI_tuptable->tupdesc->natts;
	CallAssertMsg( col >= 0 && col < num_cols,
		"col %d, num_cols %d",
		col, num_cols	);
	SpxTypeOids actual_type;
	actual_type.type_oid = SPI_gettypeid(SPI_tuptable->tupdesc, col1);
	// this should really be CallAssertMsg!!!
	if ( actual_type.type_oid != expected_type.type_oid ) {
		CALL_WARN_OUT(
			"regtypes: actual %d != expected %d; returning NULL!",
			actual_type.type_oid, expected_type.type_oid
		);
		*is_null_ret = 1;
		return (Datum) NULL;
	}
	//	heap_getattr(tup, attnum, tupleDesc, isnull)
	Datum ret = heap_getattr(tup, col1, SPI_tuptable->tupdesc, is_null_ret);
	if ( ! (int) ret || *is_null_ret ) {
		if ( (int) ret || !*is_null_ret )
			CALL_WARN_OUT("!ret: %d, is_null: %d", ! (int) ret, *is_null_ret);
		*is_null_ret = 1;
		return (Datum) NULL;
	}
  if ( DebugLevel() && expected_type.type_oid == Text_Type.type_oid ) {
		if ( *is_null_ret )
			CALL_DEBUG_OUT("--> NULL");
		else {
			TEXT_BUFFER_TMP_STR(SpxDatumVarlena(ret), str);
			CALL_DEBUG_OUT("--> %s", str);
		}
	}
	return ret;
}

// execute a query plan returning status
extern int SpxAccessDB(
	CALLS_ SpxPlans plan, Datum args[], int maxrows, bool readonly
) {
	CALL_LINK();
	AssertSPX(_CALL_);
	CallAssertMsg( ! SpxPlanNull(plan), " plan null, maxrows: %d%s",
	maxrows, readonly ? " readonly" : "" );
	CALL_DEBUG_OUT( "plan: " SPX_PLAN_FMT ", maxrows: %d%s",
	SPX_PLAN_VAL(plan), maxrows, readonly ? " readonly" : "" );
	const int status = SPI_execute_plan(plan.plan, args, NULL, readonly, maxrows);
	if (status < 0)
		CALL_BUG_OUT( "SPI_execute_plan returned code %d: %s",
	status, SpxQueryDecode(status) );
	else
		CALL_DEBUG_OUT( "tuples: %d, status: %d %s",
										(int) SPI_processed, status,SpxQueryDecode(status) );
	return SPI_processed;
}

/* read-write query convenience functions */

// execute a read-write query plan returning exactly 1 row with 1 column
static void Update1(CALLS_ SpxPlans plan, Datum args[]) {
	CALLS_LINK();
	CallAssertMsg(
		SpxUpdateDB(plan, args, 1) == 1,
		"SPI_processed  %d", (int) SPI_processed
	);
}

// execute a read-write query plan returning a datum or NULL result type 
extern Datum SpxUpdateDatum( CALLS_ SpxPlans plan, Datum args[],
	SpxTypeOids *result_type_ret, bool *is_null_ret
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColDatum(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a read-write query plan returning a datum or NULL result type 
extern Datum SpxUpdateTypedDatum( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColTypedDatum(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an oid
extern Oid SpxUpdateOid( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColOid(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an oid
extern Oid SpxUpdateTypedOid( CALLS_ SpxPlans plan, Datum args[],
		SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColTypedOid(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer
extern int32 SpxUpdateInt32(
	CALLS_ SpxPlans plan, Datum args[],
	SpxTypeOids *result_type_ret, bool *is_null_ret
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColInt32(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an integer
extern int64 SpxUpdateInt64(
	CALLS_ SpxPlans plan, Datum args[],
	SpxTypeOids *result_type_ret, bool *is_null_ret
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColInt64(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an integer
extern int32 SpxUpdateTypedInt32( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColTypedInt32(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer
extern int64 SpxUpdateTypedInt64( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColTypedInt64(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer or NULL result_type or just 0 
extern int32 SpxUpdateIfInt32( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, int32 or_else ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	bool is_null;
	const int32 i =  RowColInt32(CALL_ 0, 0, result_type_ret, &is_null);
	return is_null ? or_else : i;
}

// execute a query plan returning an integer or NULL result_type or just 0 
extern int64 SpxUpdateIfInt64( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, int64 or_else ) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	bool is_null;
	const int64 i =  RowColInt64(CALL_ 0, 0, result_type_ret, &is_null);
	return is_null ? or_else : i;
}

#if 0
// We seem to use very little of the SpxUpdate* machinery!
// How much of it should we get rid of??

// execute a query plan returning a boolean result
extern bool SpxUpdateBool(
	CALLS_ SpxPlans plan, Datum args[], bool *is_null_ret
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColBool(CALL_ 0, 0, is_null_ret);
}

// execute a query plan returning a String or NULL 
extern StrPtr SpxUpdateStr(
	CALLS_ SpxPlans plan, Datum args[], ALLOCATOR_PTR(alloc)
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColStr(CALL_ 0, 0, alloc);
}

// execute a query plan returning a VarChar or NULL result_type
extern SpxText SpxUpdateText(
	CALLS_ SpxPlans plan, Datum args[], ALLOCATOR_PTR(alloc)
) {
	CALLS_LINK();
	Update1(CALL_ plan, args);
	return RowColText(CALL_ 0, 0, is_null_ret);
}
#endif

/* read-only query convenience functions */

// execute a readonly query plan (returns exactly 1 row with 1 column) 
static void Query1(CALLS_ SpxPlans plan, Datum args[]) {
	CALL_LINK();
	CallAssertMsg(
		SpxQueryDB(plan, args, 1) == 1,
		"SPI_processed  %d",  (int) SPI_processed
	 );
}

// execute a readonly query plan returning a datum or NULL result type 
extern Datum SpxQueryDatum( CALLS_ SpxPlans plan, Datum args[],
		SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColDatum(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a readonly query plan returning a datum or NULL result type 
extern Datum SpxQueryTypedDatum( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColTypedDatum(CALL_ 0, 0, expected, is_null_ret);
}

#if 0
// execute a query plan returning a VarChar or NULL result_type
SpxText 
SpxQueryText(CALLS_ SpxPlans plan, Datum args[], bool *is_null_ret) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColText(CALL_ 0, 0, is_null_ret);
}
#endif

// execute a query plan returning a VarChar or NULL result_type
SpxText 
SpxQueryText(CALLS_ SpxPlans plan, Datum args[], bool *is_null_ret) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColText(CALL_ 0, 0, is_null_ret);
}

// execute a query plan returning an oid
extern Oid SpxQueryOid( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColOid(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an oid
extern Oid SpxQueryTypedOid( CALLS_ SpxPlans plan, Datum args[],
		SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColTypedOid(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer
extern int32 SpxQueryInt32(
	CALLS_ SpxPlans plan, Datum args[],
	SpxTypeOids *result_type_ret, bool *is_null_ret
) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColInt32(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an integer
extern int64 SpxQueryInt64(
	CALLS_ SpxPlans plan, Datum args[],
	SpxTypeOids *result_type_ret, bool *is_null_ret
) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColInt64(CALL_ 0, 0, result_type_ret, is_null_ret);
}

// execute a query plan returning an integer
extern int32 SpxQueryTypedInt32( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColTypedInt32(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer
extern int64 SpxQueryTypedInt64( CALLS_ SpxPlans plan, Datum args[],
			SpxTypeOids expected, bool *is_null_ret ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColTypedInt64(CALL_ 0, 0, expected, is_null_ret);
}

// execute a query plan returning an integer or NULL result_type or just 0 
extern int32 SpxQueryIfInt32( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, int32 or_else ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	bool is_null;
	const int32 i =  RowColInt32(CALL_ 0, 0, result_type_ret, &is_null);
	return is_null ? or_else : i;
}

// execute a query plan returning an integer or NULL result_type or just 0 
extern int64 SpxQueryIfInt64( CALLS_ SpxPlans plan, Datum args[],
				SpxTypeOids *result_type_ret, int64 or_else ) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	bool is_null;
	const int64 i =  RowColInt64(CALL_ 0, 0, result_type_ret, &is_null);
	return is_null ? or_else : i;
}

#if 0
// Not currently used
// At some point let's use a tool to find all of the unused things
// and determine their proper status!!

// execute a query plan returning a boolean result
bool 
SpxQueryBool(CALLS_ SpxPlans plan, Datum args[], bool *is_null_ret) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColBool(CALL_ 0, 0, is_null_ret);
}

#endif

// execute a query plan returning a String or NULL 
StrPtr 
SpxQueryStr(CALLS_ SpxPlans plan, Datum args[], ALLOCATOR_PTR(alloc)) {
	CALLS_LINK();
	Query1(CALL_ plan, args);
	return RowColStr(CALL_ 0, 0, alloc);
}

/* Call Graph Functions */

// join call graph into call context string 
static TmpStrPtr JoinCallAccum(CALLS_ size_t accum) {
	//	CALL_BASE();
	CALLS_LINK();
	char *result;
	static const char separator[] = ".";
	static const size_t sep_len = sizeof separator - 1;
	if ( !_CALL_ ) {
		result = (char *) palloc(accum + 1);
		*result = '\0';
	} else {
		const size_t fname_len = strlen(_CALL_->fname);
		result = (char *) JoinCallAccum(
	_CALL_->parent, accum + fname_len + (_CALL_->parent ? sep_len : 0)
		);
		if ( _CALL_->parent )
			strcat(result, separator);
		strcat(result, _CALL_->fname);
	}
	return result;
}
		
// join call graph into call context string 
TmpStrPtr JoinCalls(_CALLS_) {
	CALLS_LINK();
	return !_CALL_  ? "()" : JoinCallAccum(CALL_ 0);
}
		
/* Memory Allocation Functions */

ALLOCATOR_FUNCTION(SessionAlloc) {
	CALLS_LINK();
	CallAssert(size < MaxAllocationSize);
	return malloc(size);
}

ALLOCATOR_FUNCTION(call_palloc) {
	CALL_LINK();
	// Why would we want this???
	// Musn't use in debugging code -> infinite regress!!
	// Guard against _CALL_ being NULL, not just here???
	// so if we keep it we need to make it conditional!!!
	AssertNotSPX(_CALL_);
	CallAssertMsg(size < MaxAllocationSize, "size: %zd", size);
	return palloc(size);
}

ALLOCATOR_FUNCTION(call_SPI_palloc) {
	CALL_LINK();
	AssertSPX(_CALL_);
	CallAssertMsg(size < MaxAllocationSize, "size: %zd", size);
	return SPI_palloc(size);
}

void * CheckedAlloc(
	CALLS_ size_t size, size_t max_size, ALLOCATOR_PTR(alloc)
) {
	CALL_LINK();
	AssertThatMsg( size <= max_size,
	C_SIZE_FMT(size) C_SIZE_FMT(max_size),
	C_SIZE_VAL(size), C_SIZE_VAL(max_size) );
  void * p = CALL_ALLOC(alloc, size);
	AssertThat(p != NULL);
	return p;
}

void * MemAlloc( CALLS_ size_t size, ALLOCATOR_PTR(alloc) ) {
	CALL_LINK();
	CallAssert( alloc);
	return memset( CheckedAlloc(CALL_ size, MaxAllocationSize, alloc), 0, size );
}

char * StrAlloc( CALLS_ size_t size, ALLOCATOR_PTR(alloc) ) {
	CALL_LINK();
	CallAssert( alloc);
	char * s = CheckedAlloc(CALL_ size+1, MaxTextStrLen, alloc);
	s[size] = '\0';
	return s;
}

/*
	Ideally I'd use the
cstring_to_text_with_len
text_to_cstring
functions from varlena.c
but they only allocate with palloc.
I could wrap them and then copy the storage up with ??? what is it?
Good idea for later to avoid software rot.
For now, let's hack them to be better!
 */


text *
CStringToText( CALLS_ UtilStr s, ALLOCATOR_PTR(alloc) ) {
	CALLS_LINK();
	size_t len = s ? strlen(s) : 0;
	text *const result = CALL_ALLOC(alloc, len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

UtilStr
TextToCString( CALLS_ const text *const t, ALLOCATOR_PTR(alloc) ) {
	CALL_LINK();
	text	  *tunpacked = pg_detoast_datum_packed(SpxMutableVarlenaPtr(t));
	size_t	len = VARSIZE_ANY_EXHDR(tunpacked);
	char	  *result;

	result = CALL_ALLOC(alloc, len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);

	return result;
}

#if 0
// NULL && empty strings get a new 1-byte '\0'-terminated buffer
// We need this to avoid infinite regress when used in debugging
// code which is called by NewStr!
// Should we WARN when we have a NULL string?
StrPtr AllocStr( Str old_str, void *(*alloc)(size_t) ) {
	Str old = old_str ?: "";
	return strcpy( alloc( strlen(old) + 1 ), old );
}
#endif

// NULL => NULL
// all other strings (including empty strings) will be reallocated
// (empty strings get a new 1-byte buffer for a '\0' byte
StrPtr NewStr( CALLS_ Str old, ALLOCATOR_PTR(alloc) ) {
	CALLS_LINK();
	CallAssert( alloc);
	if (!old) return old;
	return strcpy( StrAlloc(CALL_ strlen(old), alloc), old );
}


/* String <--> Conversion and Allocation Functions */

// See src/backend/utils/adt/varchar.c:  varcharin and varchar_input

// one way to do it:
// #define CStringGetTextP(c)
//	DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(c)))

#if 0
extern SpxText SpxStrText( CALLS_ StrPtr str, ALLOCATOR_PTR(alloc) ) {
	CALL_LINK();
	if (str == NULL)
		return Spx_Null_Text;
	const size_t str_len = strlen(str);
	// const size_t td_len = str_len + VARHDRSZ;
	const size_t td_len = VARHDRSZ + str_len + 1;
	// const size_t td_len_bytes = (VARHDRSZ + str_len + 1);
	// const size_t td_len = td_len_bytes + (td_len_bytes + 4) % 4;
	SpxText td;
	td.varchar = MemAlloc(CALL_ td_len, alloc);
	SET_VARSIZE(td.varchar, td_len);
	CallAssert(td_len < MaxTextStrLen);
	memcpy( VARDATA(td.varchar), str, str_len );
	return td;
}

// One way to do it:
// #define TextPGetCString(t) 
//	DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(t)))

extern StrPtr SpxTextStr( CALLS_ SpxText td, ALLOCATOR_PTR(alloc) ) {
	CALLS_LINK();
	const size_t len = VARSIZE_ANY_EXHDR(td.varchar);
	return memcpy( StrAlloc(CALL_ len, alloc), VARDATA_ANY(td.varchar), len);
}
#endif

/* String Concatenation and Allocation Functions */

StrPtr StrCat(CALLS_ ALLOCATOR_PTR(alloc), const StrPtr s0, ...) {
	va_list args;
	StrPtr s;
	char *buf;
	size_t len = 0;
	CALL_LINK();

	va_start(args, s0);
	for (s = s0; s; s = va_arg(args, StrPtr))
		len += strlen(s);
	va_end(args);

	buf = StrAlloc(CALL_ len, alloc);
	buf[0] = '\0';

	va_start(args, s0);
	for (s = s0; s; s = va_arg(args, StrPtr))
		strcat(buf, s);
	va_end(args);

	return buf;
}

StrPtr StrCatSlices(CALLS_ ALLOCATOR_PTR(alloc), const StrPtr start0, const StrPtr end0, ...) {
	va_list args;
	StrPtr start, end;
	size_t len = 0;
	CALL_LINK();

	va_start(args, end0);
	for ( start = start0, end = end0
			 ; start
			 ; start = va_arg(args, StrPtr), end = va_arg(args, StrPtr)
	)
		len += end ? (end - start) : strlen(start) ;
	va_end(args);

	char *const buf = StrAlloc(CALL_ len, alloc);
	buf[0] = '\0';

	va_start(args, end0);
	for ( start = start0, end = end0
			 ; start
			 ; start = va_arg(args, StrPtr), end = va_arg(args, StrPtr)
	)
		if ( end )
			strncat(buf, start, end - start);
		else
			strcat(buf, start);
	va_end(args);

	return buf;
}

// syntactically possible entity name
bool is_entity_name(StrPtr s) {
	const StrPtr s0 = s;
	if (!s) return 0;
	if (!isalpha(*s) && *s != '_') return 0;
	for ( ; *s ; s++ ) {
		if (s - s0 > NAMEDATALEN) return 0;
		if (!isalnum(*s) && *s != '_' && *s != '.') return 0;
	}
	return 1;
}

// syntactically possible lowercase entity name
bool is_simple_name(StrPtr s) {
	const StrPtr s0 = s;
	if (!s) return 0;
	if (!islower(*s) && *s != '_') return 0;
	for ( ; *s ; s++ ) {
		if (s - s0 > NAMEDATALEN) return 0;
		if (!islower(*s) && !isdigit(*s) && *s != '_') return 0;
	}
	return 1;
}

// free of single quotes
bool no_quotes(StrPtr s) {
	if (!s) return 0;
	while (*s)
		if (*s == '\'') return 0;
	return 1;
}

// Array Support

#if 0

// gleanings from server/utils/array.h

typedef struct {
	int32 vl_len_;		// varlena header - do not touch directly!
	int ndim;			// # of dimensions
	int32 dataoffset;	// offset to data, or 0 if no bitmap
	Oid	elemtype;		// element type OID
} ArrayType;

// type metadata cache needed for array manipulation
typedef struct ArrayMetaState {
	Oid element_type;
	int16 typlen;
	bool typbyval;
	char typalign;
	char typdelim;
	Oid typioparam;
	Oid typiofunc;
	FmgrInfo	proc;
} ArrayMetaState;

// my pseudo-code:
struct general_array {
	int32 vl_len_;		// varlena header - do not touch directly!
	int ndim;			// # of dimensions
	int32 dataoffset;	// offset to data, or 0 if no bitmap
	Oid	elemtype;		// element type OID
	int dims[ndim];
	int lower_bounds[ndim];
	dataoffset ? bitmap null_elements;
	// pad to a MAXALIGN boundary
	element_type data[*/dims];
};

// Macros we should use to fetch and deconstruct arrays:
// written as if they were functions

static inline ArrayType *DatumGetArrayTypeP(Datum X) {
	return (ArrayType *) PG_DETOAST_DATUM(X);
}
static inline ArrayTYpe *DatumGetArrayTypePCopy(Datum X) {
	return (ArrayTYpe *) PG_DETOAST_DATUM_COPY(X);
}
static inline ArrayType *PG_GETARG_ARRAYTYPE_P(int n) {
	return DatumGetArrayTypeP( PG_GETARG_DATUM(n) );
}
static inline ArrayType *PG_GETARG_ARRAYTYPE_P_COPY(int n) {
	return DatumGetArrayTypePCopy( PG_GETARG_DATUM(n) );
}
#define PG_RETURN_ARRAYTYPE_P(x) PG_RETURN_POINTER(x)
static inline size_t ARR_SIZE(ArrayType *a) { return VARSIZE(a); }
static inline int ARR_NDIM(ArrayType *a) { return a->ndim; }
static inline bool ARR_HASNULL(ArrayType *a) {return a->dataoffset;}
static inline Oid ARR_ELEMTYPE(ArrayType *a) {return a->elemtype; }

static inline int *ARR_DIMS(ArrayTYpe *a) { return (int *) (a + 1); }
static inline int *ARR_LBOUND(a) {return ARR_DIMS(a)+ARR_NDIM(a);}

static inline bits8 *ARR_NULLBITMAP(ArrayType *a) {
	return ARR_HASNULL(a)
		? (bits8 *) (ARR_DIMS(a) + 2*ARR_NDIM(a))
		 : (bits8 *) NULL;
}

// array header size with specified ndims and number of elements
static inline size_t ARR_OVERHEAD_NONULLS(int ndims) {
	return MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int)*ndims);
}
static inline size_t ARR_OVERHEAD_WITHNULLS(int ndims,int nitems) {
	return MAXALIGN(
		sizeof(ArrayType) + 2 * sizeof(int) * ndims + (nitems + 7) / 8
	);
}

static inline size_t ARR_DATA_OFFSET(ArrayType *a) {
	return ARR_HASNULL(a)
	? (a)->dataoffset
	: ARR_OVERHEAD_NONULLS( ARR_NDIM(a) );
}

// pointer to array data.
static inline void *ARR_DATA_PTR(ArrayType *a) {
	return (char *) a + ARR_DATA_OFFSET(a);
}

// Hmm, DatumGetArrayTypePCopy sounds interesting!

/* Caution: A few "fixed-length array" datatypes,
	 e.g. NAME, POINT  use a simpler layout.
 */

#endif

static inline SpxArrayInfo ZeroArrayInfo(
	CALLS_ SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALL_LINK();
	CallAssert(a == 0 || alloc == 0);
	static const struct array_info zip; // zero filled because static
	if (a == 0) a = CALL_ALLOC(alloc, sizeof *a );
	*a = zip;
	a->dims = a->dims_array;
	a->base = a->base_array;
	return a;
}

static SpxArrayInfo InitArrayInfo(
	CALLS_ SpxTypeOids elem_type, bool isnull,
	int dim, int low_bound,
	Datum *data,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALL_LINK();
	a = ZeroArrayInfo(CALL_ a, alloc);
	a->elem_type = elem_type.type_oid;
	a->isnull = isnull;
	a->ndims = 1;
	a->dims[0] = dim;
	a->base[0] = low_bound;
	a->data = data;

	CallAssertMsg( OidIsValid(a->elem_type),
	"Invalid element type " SPX_TYPEOID_FMT,
	SPX_TYPEOID_VAL(elem_type) );

	get_typlenbyvalalign(
		a->elem_type, &a->typlen, &a->typbyval, &a->typalign
	);
	
	construct_md_array(
		data, &a->isnull,
		a->ndims, a->dims, a->base,
		a->elem_type,
		a->typlen, a->typbyval, a->typalign
	);
	return a;
}

extern SpxArrayInfo SpxNullArray(
	CALLS_ SpxTypeOids elem_type,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALLS_LINK();
	return InitArrayInfo(CALL_ elem_type, true, 0, 1, 0, a, alloc);
}

extern SpxArrayInfo SpxEmptyArray(
	CALLS_ SpxTypeOids elem_type,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALLS_LINK();
	return InitArrayInfo(CALL_ elem_type, false, 0, 1, 0, a, alloc);
}

extern SpxArrayInfo SpxNewArray(
	CALLS_ SpxTypeOids elem_type,
	int nelems,
	Datum *data,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALLS_LINK();
	return InitArrayInfo(CALL_ elem_type, false, nelems, 1, data, a, alloc);
}

extern SpxArrayInfo SpxGetArray(
	CALLS_ ArrayType *atp, bool isnull,
	SpxArrayInfo a, ALLOCATOR_PTR(alloc)
) {
	CALL_LINK();
	a = ZeroArrayInfo(CALL_ a, alloc);
	a->isnull = isnull;
	a->array_type_ptr = atp;
	a->data = ARR_DATA_PTR(atp);
	a->ndims = ARR_NDIM(atp);
	if ( a->ndims != 1 ) {
		CALL_WARN_OUT("ndims = %d!", a->ndims);
		a->dims = ARR_DIMS(atp);
	} else {
		const int *const dims = ARR_DIMS(atp);
		a->dims[0] = dims[0];
	}
	a->base[0] = 1;               // Not necessarily !!!
	a->elem_type = ARR_ELEMTYPE(atp);
#if 0
	get_type_io_data(a->elem_type, IOFunc_output,
								   &a->typlen, &a->typbyval,
								   &a->typalign, &a->typdelim,
								   &a->typelem, &a->typiofunc);
	fmgr_info_cxt(a->typiofunc, &a->proc, fcinfo-flinfo->fn_mcxt);
#endif
	get_typlenbyvalalign(
		a->elem_type, &a->typlen, &a->typbyval, &a->typalign
	);
	return a;
}

#ifndef NO_TEST_FRAME

// array testing code
FUNCTION_DEFINE(spx_describe_array) { // ANYARRAY -> nitems
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	struct array_info array_info;
	SpxGetArray(
		CALL_ PG_GETARG_ARRAYTYPE_P(0), PG_ARGISNULL(0), &array_info, 0
	);
	EREPORT_LEVEL_MSG(
		NOTICE, "describe_array: ndims %d, dims[0]: %d, type: %d",
		(int) SpxArrayNDims(&array_info),
		(int) SpxArrayDims(&array_info),
		SpxArrayElemType(&array_info)
	);
	PG_RETURN_INT32(SpxArrayDims(&array_info));
}

// array testing code
FUNCTION_DEFINE(spx_item_to_singleton) { // item -> [item]
	CALL_BASE();
	SPX_FUNC_NUM_ARGS_IS(1);
	struct array_info array_info;
	SpxTypeOids elem_type;
	elem_type.type_oid = SpxFuncArgType(fcinfo, 0);
	
	CallAssertMsg( OidIsValid(elem_type.type_oid), "no argument type" );

	/* get the provided element, being careful in case it's NULL */
	const bool isnull = PG_ARGISNULL(0);
	if ( isnull ) {
		SpxNullArray(CALL_ elem_type, &array_info, 0);
	}
	else {
		Datum element = PG_GETARG_DATUM(0); // const prop prohibits :(
		SpxNewArray(CALL_ elem_type, 1, &element, &array_info, 0);
	}  

	PG_RETURN_ARRAYTYPE_P( SpxArrayTypePtr(&array_info) );
}

#endif
