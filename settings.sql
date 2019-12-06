-- * Header  -*-Mode: sql;-*-
\cd
\cd .Wicci/Core/S1_refs
\i ../settings+sizes.sql

SELECT set_schema_path('s1_refs','s0_lib','public');
SELECT spx_debug_on(); -- jgd debugging!!
SELECT ensure_schema_ready();
-- these things don't really exist until part way through this schema
-- so I've defined dummy versions of them that do nothing at the
-- end of ../S0_lib/s0-lib.sql
-- SELECT spx_test_select('SELECT ''About to call spx_collate_locate''', 0);
-- SELECT spx_collate_locale();
-- SELECT spx_test_select('SELECT ''About to call unsafe_spx_load_schemas''', 0);
-- SELECT unsafe_spx_load_schemas();
-- SELECT spx_test_select('SELECT ''About to call unsafe_spx_load_schema_path''', 0);
-- SELECT unsafe_spx_load_schema_path();
-- SELECT spx_test_select('SELECT ''About to call unsafe_spx_load_types''', 0);
-- SELECT unsafe_spx_load_types();
-- SELECT spx_test_select('SELECT ''About to call unsafe_spx_load_procs''', 0);
-- SELECT unsafe_spx_load_procs();
-- SELECT spx_test_select('SELECT ''About to call unsafe_spx_initialize''', 0);
-- SELECT unsafe_spx_initialize();
