-- * Header  -*-Mode: sql;-*-
\cd
\cd .Wicci/Core/S1_refs
\i ../settings+sizes.sql

SELECT set_schema_path('S1_refs','S0_lib','public');
SELECT ensure_schema_ready();
