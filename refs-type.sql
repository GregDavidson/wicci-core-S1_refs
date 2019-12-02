-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-type.sql', '$Id');

--	Wicci Project
--	Tagged Typed Object Reference Type Bindings

-- ** Copyright

--	Copyright (c) 2005, 2006, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- See Notes/refs.txt for background

-- * Typed Object System

-- A ref is a Tagged/Typed Reference to a
--  specific tuple
--  of a specific type
--  supporting a common set of operations

-- a ref is just a pair of
--  class tag
--  object ID
-- wrapped into a single 32 or 64 bit integer

-- * Fundamental ref domains

DROP DOMAIN IF EXISTS ref_tags CASCADE;
DROP DOMAIN IF EXISTS ref_ids CASCADE;
DROP DOMAIN IF EXISTS refs_as_ints CASCADE;

CREATE DOMAIN ref_tags AS INTEGER;

CREATE DOMAIN ref_ids AS :WordIntsPG;

-- ** refs_as_ints = int4 or int8
CREATE DOMAIN refs_as_ints AS :WordIntsPG;

CREATE OR REPLACE
FUNCTION refs_base_init() RETURNS cstring
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION refs_base_init() IS
'initialize the fundamental refs package
returning the module id string
';

CREATE OR REPLACE
FUNCTION ref_nil_tag() RETURNS ref_tags
AS 'spx.so' LANGUAGE c IMMUTABLE;

CREATE OR REPLACE
FUNCTION unsafe_refs_load_toms() RETURNS integer
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION unsafe_refs_load_toms() IS
'(Re)Load all methods from typed_object_methods table,
returning the count.';

CREATE OR REPLACE
FUNCTION refs_debug_toms() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_op_tag_to_method(regprocedure, ref_tags)
RETURNS regprocedure
AS 'spx.so' LANGUAGE c;
COMMENT ON
FUNCTION refs_op_tag_to_method(regprocedure, ref_tags)
IS '4debugging';

CREATE OR REPLACE
FUNCTION unsafe_refs_load_tocs() RETURNS integer
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION unsafe_refs_load_tocs() IS
'(Re)Load all classes from typed_object_classes table,
returning the count.';

CREATE OR REPLACE
FUNCTION refs_type_to_tag(regtype) RETURNS ref_tags
AS 'spx.so' LANGUAGE c; COMMENT ON FUNCTION
refs_type_to_tag(regtype) IS 'debugging';

CREATE OR REPLACE
FUNCTION refs_table_type_to_tag(regclass, regtype) RETURNS ref_tags
AS 'spx.so' LANGUAGE c; COMMENT ON FUNCTION
refs_table_type_to_tag(regclass, regtype) IS 'yields the unique tag';

CREATE OR REPLACE
FUNCTION refs_tag_to_type(ref_tags) RETURNS regtype
AS 'spx.so' LANGUAGE c; COMMENT ON FUNCTION
refs_tag_to_type(ref_tags) IS 'debugging';

CREATE OR REPLACE
FUNCTION refs_debug_tocs_by_tag() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug_tocs_by_type() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug_level() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug_set(integer) RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug_on() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug_off() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION refs_debug() RETURNS boolean
AS 'spx.so' LANGUAGE c;

-- * Types and I/O functions

-- crefs are a datatype opaque to PostgreSQL
-- which pass a collection of references to
-- objects of various types implemented in C.

-- So far crefs are used to hold wicci context built in C
-- during the recursive rendering of a web-page as
-- control passes through PostgreSQL code and then
-- back to C again.


-- * TYPE refs

DROP TYPE IF EXISTS refs CASCADE;

CREATE TYPE refs;

CREATE OR REPLACE
FUNCTION call_in_method(cstring, oid, integer) RETURNS refs
AS 'spx.so' LANGUAGE c STRICT;

CREATE OR REPLACE
FUNCTION call_out_method(refs) RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE TYPE refs (
	INTERNALLENGTH = :BytesPerWord,
	ALIGNMENT = :WordAlignPG,
	INPUT = call_in_method,
	OUTPUT = call_out_method,
	PASSEDBYVALUE,
	CATEGORY = 't',
	PREFERRED = true
);

SELECT declare_type('refs');

-- * Low level Refs Functions

CREATE OR REPLACE
FUNCTION refs_tag_width() RETURNS integer
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION refs_tag_width() IS
'Returns the width of tags in bits';

-- Replace with inline???
CREATE OR REPLACE
FUNCTION refs_min_id() RETURNS ref_ids
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION refs_min_id() IS
'Returns the minimum allowed id value';

-- Replace with inline???
CREATE OR REPLACE
FUNCTION refs_max_id() RETURNS ref_ids
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION refs_max_id() IS
'Returns the maximum allowed id value';

-- Replace with inline???
CREATE OR REPLACE
FUNCTION refs_min_tag() RETURNS ref_tags
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION refs_min_tag() IS
'Returns the minimum allowed tag value';

-- Replace with inline???
CREATE OR REPLACE
FUNCTION refs_max_tag() RETURNS ref_tags
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION refs_max_tag() IS
'Returns the maximum allowed tag value';

CREATE OR REPLACE
FUNCTION ref_tag(refs) RETURNS ref_tags
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION ref_tag(refs) IS
'Extracts tag from a ref- written in C, could be in SQL';

-- ** ref_id(refs) -> ref_ids
CREATE OR REPLACE
FUNCTION ref_id(refs) RETURNS ref_ids
AS 'spx.so', 'ref_id' LANGUAGE c IMMUTABLE;
COMMENT ON FUNCTION ref_id(refs) IS
'Extracts id from a ref - written in C, could be in SQL';

-- * Comparison Functions and Operators for Indexing

-- see Refs/refs-op-class.sql create_ref_op_class(regtype)

CREATE FUNCTION ref_cmp(refs, refs) RETURNS int4
	 AS 'spx.so' LANGUAGE C IMMUTABLE;

CREATE FUNCTION ref_lt(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) < 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ref_le(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) <= 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ref_eq(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) = 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ref_neq(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) <> 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ref_ge(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) >= 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION ref_gt(refs, refs) RETURNS bool
AS $$ SELECT ref_cmp($1, $2) > 0 $$
LANGUAGE SQL IMMUTABLE;

CREATE OPERATOR < (
	 leftarg = refs, rightarg = refs, procedure = ref_lt,
	 commutator = > , negator = >= ,
	 restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR <= (
	 leftarg = refs, rightarg = refs, procedure = ref_le,
	 commutator = >= , negator = > ,
	 restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR = (
	 leftarg = refs, rightarg = refs, procedure = ref_eq,
	 commutator = = ,
	 negator = <> ,
	 restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR <> (
	 leftarg = refs, rightarg = refs, procedure = ref_neq,
	 commutator = <> ,
	 negator = = ,
	 restrict = neqsel, join = neqjoinsel
);
CREATE OPERATOR >= (
	 leftarg = refs, rightarg = refs, procedure = ref_ge,
	 commutator = <= , negator = < ,
	 restrict = scalargtsel, join = scalargtjoinsel
);
CREATE OPERATOR > (
	 leftarg = refs, rightarg = refs, procedure = ref_gt,
	 commutator = < , negator = <= ,
	 restrict = scalargtsel, join = scalargtjoinsel
);

-- now we can make the operator class
CREATE OPERATOR CLASS ref_cmp_ops
		DEFAULT FOR TYPE refs USING btree AS
				OPERATOR        1       < ,
				OPERATOR        2       <= ,
				OPERATOR        3       = ,
				OPERATOR        4       >= ,
				OPERATOR        5       > ,
				FUNCTION        1       ref_cmp(refs, refs);

-- * TYPE unchecked_refs

-- unchecked_refs allows for unsafe downcasts,
-- e.g. x::unchecked_refs::other_ref_type.
-- It is the responsibility of any code using it to
-- ensure class and referential integrity!

DROP TYPE IF EXISTS unchecked_refs CASCADE;

CREATE TYPE unchecked_refs;

CREATE OR REPLACE
FUNCTION unchecked_call_in_method(cstring, oid, integer) RETURNS unchecked_refs
AS 'spx.so', 'call_in_method' LANGUAGE c STRICT;

CREATE OR REPLACE
FUNCTION unchecked_call_out_method(unchecked_refs) RETURNS cstring
AS 'spx.so', 'call_out_method' LANGUAGE c IMMUTABLE;

CREATE TYPE unchecked_refs (
	input = unchecked_call_in_method,
	output = unchecked_call_out_method,
	LIKE = refs,
	CATEGORY = 't'
);

-- ironically, both of these casts are safe!

DROP CAST IF EXISTS (unchecked_refs AS refs) CASCADE;

CREATE CAST (unchecked_refs AS refs) WITHOUT FUNCTION;

DROP CAST IF EXISTS (refs AS unchecked_refs) CASCADE;

CREATE CAST (refs AS unchecked_refs) WITHOUT FUNCTION;

-- * Special Refs Construction and Tests

-- ++ unchecked_ref_from_tag_id(ref_tags, ref_ids) -> refs
CREATE OR REPLACE
FUNCTION unchecked_ref_from_tag_id(ref_tags, ref_ids)
RETURNS unchecked_refs
AS 'spx.so' LANGUAGE c IMMUTABLE;
COMMENT ON
FUNCTION unchecked_ref_from_tag_id(ref_tags, ref_ids) IS
'Constructs an unchecked_refs from a tag and an id - written in C';

CREATE OR REPLACE
FUNCTION unchecked_ref_tag_seq(ref_tags, regclass)
RETURNS unchecked_refs AS $$
	SELECT unchecked_ref_from_tag_id($1, nextval($2)::ref_ids)
$$ LANGUAGE SQL;

CREATE OR REPLACE
FUNCTION ref_nil(id ref_ids DEFAULT 0) RETURNS refs AS $$
	SELECT unchecked_ref_from_tag_id(ref_nil_tag(), $1)::refs
$$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION ref_nil(ref_ids) IS
'Returns a nil_tag ref id, default 0';

CREATE OR REPLACE
FUNCTION nil_tagged(refs) RETURNS boolean AS $$
	SELECT $1 IS NULL OR ref_tag($1) = ref_nil_tag()
$$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION nil_tagged(refs) IS 'Is the reference NULL or nil tagged?';

CREATE OR REPLACE
FUNCTION is_nil(refs) RETURNS boolean AS $$
	SELECT nil_tagged($1) OR ref_id($1) = 0
$$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION is_nil(refs) IS 'Is the reference NULL or nil?';

CREATE OR REPLACE
FUNCTION non_nil(refs) RETURNS boolean AS $$
	SELECT NOT is_nil($1)
$$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION non_nil(refs) IS 'Is the reference non-nil?';

CREATE CAST (refs AS boolean) WITH FUNCTION non_nil(refs);

-- * TYPE crefs

DROP TYPE IF EXISTS crefs CASCADE;

CREATE TYPE crefs;

-- ** crefs_nil() -> crefs
CREATE OR REPLACE
FUNCTION crefs_nil() RETURNS crefs
AS 'spx.so' LANGUAGE c IMMUTABLE;

-- ** crefs_in(cstring) -> crefs
CREATE OR REPLACE
FUNCTION crefs_in(cstring) RETURNS crefs
AS 'spx.so' LANGUAGE c STRICT;

-- ** crefs_out(crefs) -> cstring
CREATE OR REPLACE
FUNCTION crefs_out(crefs) RETURNS cstring
AS 'spx.so' LANGUAGE c;

-- ** TYPE crefs
CREATE TYPE crefs (
	INTERNALLENGTH = :BytesPerWord,
	ALIGNMENT = :WordAlignPG,
	INPUT = crefs_in,
	OUTPUT = crefs_out,
	PASSEDBYVALUE
);

SELECT declare_type('crefs');

-- ** type crefs functions

-- ** crefs_calls(crefs) -> text
CREATE OR REPLACE
FUNCTION crefs_calls(crefs) RETURNS text
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION crefs_calls(crefs) IS
'returns the call chain leading up to the caller';

/*
-- ** crefs_env(crefs) -> env_refs
CREATE OR REPLACE
FUNCTION crefs_env(crefs) RETURNS env_refs
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION crefs_env(crefs) IS
'returns the env_ref in the given crefs';

-- ** crefs_node(crefs) -> refs (doc_node)
CREATE OR REPLACE
FUNCTION crefs_node(crefs) RETURNS refs
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION crefs_node(crefs) IS
'returns the current tree node being rendered';

-- ** crefs_parent(crefs) -> refs (doc_node)
CREATE OR REPLACE
FUNCTION crefs_parent(crefs) RETURNS refs
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION crefs_parent(crefs) IS
'returns the parent of the tree node being rendered';
*/

-- ** crefs_indent(crefs) -> integer
CREATE OR REPLACE
FUNCTION crefs_indent(crefs) RETURNS integer
AS 'spx.so' LANGUAGE c;
COMMENT ON FUNCTION crefs_indent(crefs) IS
'returns the call chain leading up to the caller';

-- * type refs operations

/*
CREATE OR REPLACE
FUNCTION ref_show_op(refs) RETURNS text
AS 'spx.so','call_text_method' LANGUAGE c;
COMMENT ON FUNCTION ref_show_op(refs) IS
'Show the value of the referenced object in a
manner nice for interactive debugging - generally
suppressing or collapsing the presentaiton of
sub-objects';
*/

CREATE OR REPLACE
FUNCTION ref_text_op(refs) RETURNS text
AS 'spx.so','call_text_method' LANGUAGE c;
COMMENT ON FUNCTION ref_text_op(refs) IS
'Return the value of the referenced object as
complete text, including all sub-objects';

CREATE OR REPLACE
FUNCTION ref_scalar_op(refs) RETURNS refs
AS 'spx.so','call_scalar_method' LANGUAGE c;

CREATE OR REPLACE
FUNCTION ref_length_op(refs) RETURNS :WordIntsPG
AS 'spx.so', 'call_scalar_method'  LANGUAGE c;

CREATE OR REPLACE
FUNCTION ref_exists_op(refs) RETURNS boolean
AS 'spx.so', 'call_scalar_method' LANGUAGE c;
