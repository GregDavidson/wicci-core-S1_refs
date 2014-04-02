-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('spx-test.sql', '$Id');

--	Wicci Project Virtual Text Schema
--	SQL Wrappers for Utility C Code for Server Extensions
--	Test Version

-- ** Copyright

--	Copyright (c) 2005, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- * test postgres-callable functions

-- * cautious tiny steps

-- SELECT spx_debug_set(2);

-- SELECT spx_debug_on();

-- SELECT spx_debug_set(2);

-- SELECT spx_debug_off();

-- TABLE our_schemas_by_name;

-- SELECT spx_load_schemas();

-- TABLE schema_path_by_id;

-- SELECT spx_load_schema_path();

-- SELECT spx_debug_schemas();

-- TABLE our_types;

-- SELECT spx_load_types();

-- SELECT spx_debug_types();

-- TABLE schema_view;

-- TABLE schema_path_by_id;

-- TABLE our_types ORDER BY name_;

-- TABLE our_procs ORDER BY name_;

-- TABLE spx_proc_view;

-- TABLE our_procs;

-- SELECT spx_load_procs();

-- SELECT spx_debug_procs();

-- * bigger steps

-- provide some procs and types to play with:

-- SELECT p.oid::regprocedure FROM pg_proc p, pg_namespace n
-- WHERE pronamespace = n.oid AND nspname='sql'
-- AND pronargs > 0 AND prorettype = 'text'::regtype AND proname LIKE 'xml_%';

-- SELECT declare_proc(p.oid) FROM pg_proc p, pg_namespace n
-- WHERE pronamespace = n.oid AND nspname='sql'
-- AND pronargs > 0 AND prorettype = 'text'::regtype AND proname LIKE 'xml_%';

-- SELECT DISTINCT id_, name_, oid_ 
-- FROM s0_lib.schema_path_by_id;

-- SELECT DISTINCT 
-- 	id_, name_, oid_, name_size_, min_id_, max_id_, sum_text_ 
-- FROM s0_lib.schema_view  ORDER BY name_;

-- SELECT DISTINCT 
-- 	oid_, name_, schema_id_, typlen_, typbyval_, name_size_, sum_text_
-- FROM s1_refs.our_types ORDER BY name_;

-- SELECT DISTINCT 
-- 	oid_, name_, schema_id, 
-- 	rettype_::regtype, minargs_, maxargs_, argtypes_, 
-- 	name_size_, sum_text_, sum_nargs_ 
-- FROM s1_refs.our_procs   ORDER BY name_;

SELECT test_func(
	'spx_schema_by_id(integer)',
	spx_schema_by_id(id_)::text,
	'sql'
) FROM schema_view WHERE name_ = 'sql';

SELECT test_func(
	'spx_type_by_oid(regtype)',
	spx_type_by_oid('regtype')::text,
	'regtype'
);

SELECT test_func(
	'spx_proc_by_oid(regprocedure)',
	spx_proc_by_oid('declare_type(regtype)')::text,
	'declare_type(regtype)'
);

SELECT test_func(
	'spx_proc_call_proc_str(regprocedure, regprocedure)',
	spx_proc_call_proc_str('xml_pure_text(text,text)','xml_pure_text(text,text)')::text,
	'SELECT s0_lib.xml_pure_text($1,$2)'
);

SELECT test_func(
	'spx_proc_call_proc_str(regprocedure, regprocedure)',
	spx_proc_call_proc_str('xml_pure_text(text,text)','declare_type(regtype)')::text,
	'SELECT s1_refs.declare_type($1::regtype)::text'
);

-- SELECT spx_debug_set(2);

-- SELECT oid_::oid, spx_proc_by_oid(oid_), oid_::regprocedure AS "signature"
-- FROM our_procs ORDER BY name_;
