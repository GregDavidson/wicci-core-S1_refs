-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('spx-code.sql', '$Id');

--	SQL Support for Utility C Code for PostgreSQL Server Extensions

-- ** Copyright

--	Copyright (c) 2005, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- * garbage collection

CREATE OR REPLACE
FUNCTION spx_clean() RETURNS void AS $$
	DELETE FROM our_procs WHERE NOT EXISTS (
		SELECT p.oid FROM pg_proc p WHERE p.oid = oid_
	);
	DELETE FROM our_procs WHERE NOT EXISTS (
		SELECT p.oid FROM pg_type p WHERE p.oid = rettype
	);
	DELETE FROM our_types
	WHERE NOT EXISTS (
		SELECT p.oid FROM pg_type p WHERE p.oid = oid_
	);
	SELECT schema_clean();
$$ LANGUAGE SQL;
COMMENT ON FUNCTION spx_clean() IS
'Get rid of any garbage in our_(proc|type).';


-- * the views which are consulted by the C code

-- ** Utility Functions for Views

CREATE OR REPLACE
FUNCTION schematize(schema text, obj text) RETURNS TEXT AS $$
	SELECT CASE
		WHEN $2 ILIKE ($1 || '.%') THEN $2
		ELSE $1 || '.' || $2
		END
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE
FUNCTION schematize_type(regtype) RETURNS TEXT AS $$
	SELECT schematize(nspname, $1::regtype::text)
	FROM pg_type, pg_namespace
	WHERE typnamespace = pg_namespace.oid
$$ LANGUAGE sql IMMUTABLE;

-- ** type_view

CREATE OR REPLACE VIEW type_view__ AS
	SELECT
		oid_,
--    typname::text AS name_,		-- PostgreSQL type names
		oid_::regtype::text AS name_, 	-- more standard SQL type names
		ons.id::integer AS schema_id_,
		typlen::integer AS typlen_,
		typbyval AS typbyval_
	FROM our_types our, pg_type pg, our_namespaces ons
	WHERE our.oid_ = pg.oid AND typnamespace = ons.schema_oid;

CREATE OR REPLACE VIEW type_view_ AS
	SELECT oid_, name_, schema_id_, typlen_, typbyval_,
		aligned_length(name_) AS name_size_
	FROM type_view__;

CREATE OR REPLACE VIEW type_view AS
	SELECT
	 oid_, name_, schema_id_, typlen_, typbyval_, name_size_, sum_text_
	FROM type_view_,  (
			SELECT SUM(name_size_)::bigint FROM type_view_
	) AS foo(sum_text_);
COMMENT ON VIEW type_view IS
'Used by spx.so function unsafe_spx_load_types';

-- ** proc_view

CREATE OR REPLACE VIEW old_procs_ AS
	SELECT oid_, proname::text AS name_,
		ons.id::integer as schema_id,
		prorettype::regtype AS rettype_,
		(pronargs - pronargdefaults)::integer AS minargs_,
		pronargs::integer AS maxargs_,
		proargtypes AS argtypes_,
		aligned_length(proname) AS name_size_
	FROM our_procs our, pg_proc pg, our_namespaces ons
	WHERE our.oid_ = pg.oid AND pronamespace = ons.schema_oid;

CREATE OR REPLACE VIEW old_procs AS
	SELECT
	 oid_, name_, schema_id,
	 rettype_, minargs_, maxargs_, argtypes_,
	 name_size_,
	 sum_text_, sum_nargs_
	FROM old_procs_,  (
			SELECT SUM(name_size_)::bigint, SUM(maxargs_)::bigint FROM old_procs_
	) AS foo(sum_text_, sum_nargs_);
COMMENT ON VIEW old_procs IS
'Was used by spx.so function LoadProcs';

TABLE old_procs;

CREATE OR REPLACE VIEW proc_view_ AS
	SELECT DISTINCT oid_, proname::text AS name_,
		ons.id::integer as schema_id,
		prorettype::regtype AS rettype_,
		provolatile <> 'v' AS readonly_,
		(pronargs - pronargdefaults)::integer AS minargs_,
		pronargs::integer AS maxargs_,
		proargtypes AS argtypes_,
		aligned_length(proname) AS name_size_
	FROM our_procs our, pg_proc pg, our_namespaces ons
	WHERE our.oid_ = pg.oid AND pronamespace = ons.schema_oid;

CREATE OR REPLACE VIEW proc_view AS
	SELECT
	 oid_, name_, schema_id, rettype_, readonly_,
	 minargs_, maxargs_, argtypes_,
	 name_size_, sum_text_, sum_nargs_
	FROM proc_view_,  (
			SELECT SUM(name_size_)::bigint, SUM(maxargs_)::bigint FROM proc_view_
	) AS foo(sum_text_, sum_nargs_);
COMMENT ON VIEW proc_view IS
'Used by spx.so function LoadProcs';

CREATE OR REPLACE VIEW spx_proc_view AS
SELECT oid_::oid, (
	rettype_::regtype::text || ' ' || oid_::regprocedure || CASE
		WHEN NOT readonly_ THEN ' VOLATILE' ELSE ''
	END
) AS "signature" FROM proc_view;

-- * C functions

-- ** fundamental functions

-- CREATE OR REPLACE
-- FUNCTION spx_init() RETURNS cstring
-- AS 'spx.so' LANGUAGE c;

/* NOT SAFE!  Only call if we've already called
 spx_collate_locale()
 unsafe_spx_load_schemas()
 spx_debug_schemas()
 unsafe_spx_load_schema_path()
*/
-- needed for jgd debug!!  Remove DROP when remove that!!
DROP FUNCTION IF EXISTS spx_initialize();
CREATE OR REPLACE
FUNCTION unsafe_spx_initialize() RETURNS cstring
AS 'spx.so' LANGUAGE c;

-- needed for jgd debug!!  Remove DROP when remove that!!
DROP FUNCTION IF EXISTS spx_collate_locale();
CREATE OR REPLACE
FUNCTION spx_collate_locale() RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION unsafe_spx_load_schemas() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION unsafe_spx_load_schema_path() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION unsafe_spx_load_types() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION unsafe_spx_load_procs() RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_test_select(text, integer) RETURNS int8
AS 'spx.so' LANGUAGE c STRICT;

-- ** testing functions

CREATE OR REPLACE
FUNCTION spx_schema_by_id(integer) RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_schema_by_oid(oid) RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_schema_path_by_oid(oid) RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_type_by_oid(regtype) RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_proc_by_oid(regprocedure) RETURNS cstring
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_proc_call_proc_str(regprocedure, regprocedure)
RETURNS cstring
AS 'spx.so' LANGUAGE c;

-- ** array functions

CREATE OR REPLACE
FUNCTION spx_describe_array(ANYARRAY) RETURNS integer
AS 'spx.so' LANGUAGE c;

CREATE OR REPLACE
FUNCTION spx_item_to_singleton(anyelement) RETURNS anyarray
AS 'spx.so' LANGUAGE c;

-- ** debugging functions

CREATE OR REPLACE
FUNCTION spx_debug_schemas() RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_types() RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_procs() RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_level() RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug() RETURNS boolean
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug() RETURNS boolean
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_set(integer) RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_on() RETURNS integer
AS 'spx.so' LANGUAGE c ;

CREATE OR REPLACE
FUNCTION spx_debug_off() RETURNS integer
AS 'spx.so' LANGUAGE c ;

-- * sql functions

-- Ensure Sql module ready
CREATE OR REPLACE
FUNCTION spx_ready() RETURNS regprocedure[] AS $$
--  PERFORM require_module('s1_refs.spx-code');
	SELECT ARRAY[ sql_wicci_ready() ] || ARRAY[
			'spx_collate_locale()',
			'unsafe_spx_load_schemas()',	'unsafe_spx_load_schema_path()',
			'unsafe_spx_load_types()',	'unsafe_spx_load_procs()'
	]::regprocedure[] FROM
		spx_collate_locale(),
		unsafe_spx_load_schemas(), unsafe_spx_load_schema_path(),
		unsafe_spx_load_types(),	unsafe_spx_load_procs(),
		unsafe_spx_initialize()
$$ LANGUAGE sql;
COMMENT ON FUNCTION spx_ready() IS '
	Ensure that all modules of the spx schema
	are present and initialized.
';

CREATE OR REPLACE
FUNCTION ensure_schema_ready() RETURNS regprocedure[] AS $$
	SELECT spx_ready();
$$ LANGUAGE sql;

-- * spx-test data

SELECT declare_proc('xml_pure_text(text,text)');
