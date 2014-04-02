-- * Header  -*-Mode: sql;-*-

--	Metaprogramming Support Demonstrated
--	Refs and Rows Naming Conventions Shown

-- ** Copyright
--	Copyright (c) 2011, J. Greg Davidson.

-- * The omnibus metafunction being called here:

SELECT create_name_ref_schema(
	'name', _norm := 'str_trim_lower(text)'
);

/*
	Will create a new first-class reference type, a table to store its
	data, functions to provide for the most common use cases and
	provide appropriate access through generic operators
*/

-- * In more detail, it will create:

-- ** a first class reference type: name_refs

CREATE OR REPLACE
FUNCTION name_in_op(cstring, oid, integer) RETURNS name_refs
AS 'spx.so', 'call_in_method' LANGUAGE 'c' STRICT;

CREATE OR REPLACE
FUNCTION name_out_op(name_refs) RETURNS cstring
AS 'spx.so', 'call_out_method' LANGUAGE 'c' IMMUTABLE;

CREATE TYPE name_refs (
	input = name_in_op,
	output = name_out_op,
	LIKE = refs,
);

-- along with these safe up-casts:

CREATE CAST (name_refs AS refs) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (name_refs[] AS refs[])
	WITH FUNCTION to_array_ref(ANYARRAY) AS IMPLICIT;

-- and this one very unsafe cast:

CREATE CAST (unchecked_refs AS name_refs) WITHOUT FUNCTION;

-- and appropriate comparison operators for indexes, etc.

-- ** it will create the table:

CREATE TABLE name_rows (
	ref name_refs PRIMARY KEY,
	name_ TEXT UNIQUE NOT NULL
);

-- which will accept inserts but not updates

-- ** along with a sequence generator:

CREATE SEQUENCE name_id_seq
	OWNED BY name_rows.ref
	MINVALUE 1 MAXVALUE :RefIdMax CYCLE;

-- ** and three key functions:

FUNCTION unchecked_name_from_id(ref_ids) -- use with care

FUNCTION name_nil() RETURNS name_refs	-- will have ref_id = 0

FUNCTION next_name_ref() RETURNS name_refs -- from the sequence

-- ** The PRIMARY KEY ref will be given a DEFAULT:

ALTER TABLE name_rows ALTER COLUMN ref
	SET DEFAULT next_name_ref();

-- ** the type and table will be declared as a ref class:

SELECT declare_ref_class('name_refs', 'name_rows');

-- ** and an exists method will be created and bound:

FUNCTION name_exists(name_refs) RETURNS boolean

SELECT type_class_op_method(
	'name_refs', 'name_rows',
	'ref_exists_op(refs)', 'name_exists(name_refs)'
);

-- ** additionally, that same command will create:

-- *** a safe downcast function:

FUNCTION try_name(refs) RETURNS name_refs -- NULL if it isn't

-- *** find-from-text functions:

FUNCTION try_name_in(text) RETURNS name_refs -- NULL if none
-- will accept numeric format when system is not initialized

FUNCTION try_name(text) RETURNS name_refs -- NULL if none

FUNCTION find_name(text) RETURNS name_refs -- error if none

-- *** find--or-create-from-text functions:

FUNCTION try_get_name(text) RETURNS name_refs -- NULL if fails

FUNCTION get_name(text) RETURNS name_refs -- error if fails

FUNCTION declare_name(VARIADIC text[]) RETURNS name_refs[]
-- calls get_name for each argument

-- *** to-text and to-length conversion functions:

FUNCTION name_text(name_refs) RETURNS text

FUNCTION name_length(name_refs) RETURNS integer

-- *** along with appropriate operator-->method bindings:

SELECT type_class_in('name_refs', 'name_rows', 'try_name_in(text)');
SELECT type_class_out('name_refs', 'name_rows', 'name_text(name_refs)');

SELECT type_class_op_method(
	'name_refs', 'name_rows',
	'ref_text_op(refs)', 'name_text(name_refs)'
);

SELECT type_class_op_method(
	'name_refs', 'name_rows',
  'ref_length_op(refs)', 'name_length(name_refs)'
);

/*
	You can get variations on or a subset of these features by passing
	extra parameters, or by using the mid-level meta-function API
	instead of using this omnibus meta-function.  This example is
	indended to demonstrate the most common features created for
	most datatypes along with the standard naming conventions.

	Not all these features will exist for all types, but where they exist
	they will have these names and signatures.  Some types will
	provide many more features in addition to these fundamentals.	
*/
