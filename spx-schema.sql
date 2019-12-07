-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('spx-schema.sql', '$Id');

--	SQL Support for Utility C Code for PostgreSQL Server Extensions

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- * Caveats

-- ** PostgreSQL prohibits REFERENCES to system catalogs

-- Therefore we have to shadow what we need
-- and manually manage its accuracy.
-- If cached in RAM, must (re)load caches manually!

-- ** Things we need to shadow & cache

-- Schemas
-- Types
-- Procedures
-- Maybe later some table metadata?

-- See refs files for caching of Class structure

-- *** Schemas are shadowed  in:
	--	s0_lib.spx_schema_names
	--	s0_lib.our_schema_namespaces (materialized view)

-- unsafe_spx_load_schemas() -- loads schema_view
-- unsafe_spx_load_schema_path() -- loads schema_path_by_id
-- schema_clean() -- deletes rows of dropped schemas
-- drop_schemas(schema_names)
	-- drops any dependent schemas and the specified one
	-- ON DELETE CASCADE references should then cause
	-- any rows referenceing dropped objects to go away.
	-- Be sure to call spx_init (perhaps-indirectly) to ensure
	-- that the new tables' contents get loaded!!

-- *** Type & Proc shadowing:
-- unsafe_spx_load_types() -- loads our_types via type_view
-- unsafe_spx_load_procs() -- loads our_procs via proc_view
-- spx_clean -- deletes rows of dropped objects

-- ** Refs tables & functions:
-- refs_clean() in refs-code.sql
	-- calls spx_clean
	-- gets rid of references to dropped classes  & methods

-- ** Our code which manages shadowing and caching is fragile!!
-- Ready functions call init functions call load functions.
-- Assumes nothing cached changes during session!!
-- When assumption violated, e.g. during a debug session,
-- then manually drop, clean, reload as needed!!

-- ** Types

CREATE TABLE IF NOT EXISTS our_types (
	oid_ regtype PRIMARY KEY,
	-- REFERENCES pg_type(oid) ON DELETE CASCADE
	schema_ schema_ids DEFAULT 0 NOT NULL
		REFERENCES our_namespaces(id) ON DELETE CASCADE,
	typname name NOT NULL,
	UNIQUE(schema_, typname)
);
COMMENT ON TABLE our_types IS
'Our functions in our_procs reference this table.
See caveats at top of file spx-schema.sql.';
COMMENT ON COLUMN our_types.oid_ IS
'REFERENCES pg_type(oid) but PostgreSQL does not allow
real reference to system tables, sigh!.';
COMMENT ON COLUMN our_types.schema_ IS
'When 0 the insert trigger will try to fill it in from pg_type';
COMMENT ON COLUMN our_types.oid_ IS
'REFERENCES pg_type(oid) but no system table REFERENCES in pgsql!!';
COMMENT ON COLUMN our_types.typname IS
'Protects against  change in pg_type(oid) for existing type.  Kludge!!';

CREATE OR REPLACE
FUNCTION assert_type_exists(regtype) RETURNS regtype AS $$
	SELECT non_null(
		( SELECT typnamespace FROM pg_type WHERE oid = $1 ),
		'assert_type_exists(regtype)',
		'No regtype', $1::text
	)
$$ LANGUAGE sql;

/*
CREATE OR REPLACE
FUNCTION assert_type_exists(regtype) RETURNS oid AS $$
DECLARE
	ns_oid oid;
BEGIN
	SELECT typnamespace INTO ns_oid FROM pg_type WHERE oid = $1;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'assert_type_exists: No regtype %', $1;
	END IF;
	RETURN ns_oid;
END
$$ LANGUAGE plpgsql;
*/

CREATE OR REPLACE
FUNCTION our_type_inserter() RETURNS trigger AS $$
DECLARE
	ns oid := assert_type_exists(NEW.oid_);
	sid maybe_schema_ids := declare_schema(ns);
BEGIN
	IF NEW.typname IS NULL THEN
		NEW.typname := NEW.oid_::name;
	END IF;
	IF NEW.schema_ = 0 THEN
		NEW.schema_ = sid;
	ELSEIF NEW.schema_ != sid THEN
		RAISE EXCEPTION
		'Table % % error: type % has schema % not %',
			TG_TABLE_NAME, TG_OP, NEW.oid_, NEW.schema_, sid;
	END IF;
	RETURN NEW;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION our_type_inserter() IS
'Ensure that any new row in our_types corresponds to a valid row in
pg_type and pg_namespace, filling in the latter if necessary and
perhaps creating rows in our_namespaces and spx_schema_names if
necessary.  Any inconsistency will throw an exception.';

CREATE OR REPLACE
FUNCTION declare_type(regtype) RETURNS regtype AS $$
DECLARE
	ns oid := assert_type_exists($1);
	type_schema maybe_schema_ids := declare_schema(ns);
	our_type_schema maybe_schema_ids;
	kilroy_was_here boolean := false;
	this regprocedure := 'declare_type(regtype)';
BEGIN
	LOOP
		SELECT schema_ INTO our_type_schema
		FROM our_types WHERE oid_ = $1;
		IF FOUND THEN
			IF our_type_schema != type_schema THEN
				RAISE EXCEPTION
					'%: pg_type schema % != our_types schema %',
					this, type_schema, our_type_schema; 
			END IF;
			RETURN $1;
		END IF;
		IF kilroy_was_here THEN
			RAISE EXCEPTION '% looping with %', this, $1;
		END IF;
		kilroy_was_here := true;
		BEGIN
			INSERT INTO our_types(oid_, typname) VALUES ($1, $1::name);
			RETURN $1;
		EXCEPTION
			WHEN unique_violation THEN	-- another thread?
				RAISE NOTICE '% % raised %!', this, $1, 'unique_violation';
		END;
	END LOOP;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION declare_type(regtype) IS
'Ensures the given regtype exists in pg_type and that the associated
schema exists in our_namespaces and is consistent, creating
our_namespaces and spx_schema_names rows if necessary.';

DROP TRIGGER IF EXISTS our_type_inserter ON our_types CASCADE;

CREATE TRIGGER our_type_inserter
	BEFORE INSERT ON our_types
	FOR EACH ROW EXECUTE PROCEDURE our_type_inserter();

DROP TRIGGER IF EXISTS our_type_update ON our_types CASCADE;

CREATE TRIGGER our_type_update
	BEFORE UPDATE ON our_types
	FOR EACH ROW EXECUTE PROCEDURE prohibition_trigger();

-- * our_procs

CREATE TABLE IF NOT EXISTS our_procs (
	oid_ regprocedure PRIMARY KEY,
	-- REFERENCES pg_proc(oid) ON DELETE CASCADE
	schema_ schema_ids DEFAULT 0 NOT NULL
		REFERENCES our_namespaces(id) ON DELETE CASCADE,
	rettype regtype DEFAULT 0 NOT NULL
		REFERENCES our_types ON DELETE CASCADE,
	-- REFERENCES pg_type(oid) ON DELETE CASCADE
	name_ name NOT NULL,
	argtypes oidvector NOT NULL,
	UNIQUE(schema_, name_, argtypes)
);
COMMENT ON TABLE our_procs IS
'Used for operator and method functions referenced by
tables in refs-schema.sql.  See Caveats at top of file
spx-schema.sql.';
COMMENT ON COLUMN our_procs.schema_ IS
'When 0 the insert trigger will try to fill it in from pg_proc';
COMMENT ON COLUMN our_procs.rettype IS
'When 0 the insert trigger will try to fill it in from pg_proc;
REFERENCES pg_type(oid) but no system table REFERENCES in pgsql!!';
COMMENT ON COLUMN our_procs.oid_ IS
'REFERENCES pg_proc(oid) but no system table REFERENCES in pgsql!!';
COMMENT ON COLUMN our_procs.name_ IS
'Protects against  change in pg_proc(oid) for existing proc.  Kludge!!';
COMMENT ON COLUMN our_procs.argtypes IS
'Protects against  change in pg_proc(oid) for existing proc.  Kludge!!';

DROP TRIGGER IF EXISTS our_procs_update ON our_procs CASCADE;

CREATE TRIGGER our_procs_update
	BEFORE UPDATE ON our_procs
	FOR EACH ROW EXECUTE PROCEDURE prohibition_trigger();

CREATE TABLE IF NOT EXISTS our_procs_types (
	proc_oid regprocedure NOT NULL
		REFERENCES our_procs ON DELETE CASCADE,
	arg_type regtype NOT NULL
		REFERENCES our_types ON DELETE CASCADE,
	arg_i integer NOT NULL,
	PRIMARY KEY(proc_oid, arg_type, arg_i)
);
COMMENT ON TABLE our_procs_types IS
'For referential integrity of argument types, this duplicates
the data in pg_proc.proargtypes.  Rows should only be
inserted or dropped as a side-effect of inserting or dropping
an our_procs row.';

CREATE OR REPLACE
FUNCTION our_procs_types_inserter() RETURNS trigger AS $$
	DECLARE
		type_oid regtype;
		foo regtype;
	BEGIN
		SELECT proargtypes[NEW.arg_i] INTO type_oid
		FROM pg_proc WHERE oid = NEW.proc_oid;
		IF NOT FOUND THEN
			RAISE EXCEPTION
	'Table % % error: proc % argument % not found',
	TG_TABLE_NAME, TG_OP, NEW.proc_oid, NEW.arg_i;
		END IF;
		IF NEW.arg_type != type_oid THEN
			RAISE EXCEPTION
	'Table % % error: proc % argument % of type % not %',
	TG_TABLE_NAME, TG_OP, NEW.proc_oid, NEW.arg_i, type_oid, NEW.arg_type;
		END IF;
		foo := declare_type(type_oid);    
		RETURN NEW;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION our_procs_types_inserter() IS
'ensures element exists in pg_proc.proargtypes';

DROP TRIGGER IF EXISTS our_procs_types_inserter ON our_procs_types CASCADE;

CREATE TRIGGER our_procs_types_inserter
	BEFORE INSERT OR UPDATE ON our_procs_types
	FOR EACH ROW EXECUTE PROCEDURE our_procs_types_inserter();

CREATE OR REPLACE
FUNCTION our_procs_before_inserter() RETURNS trigger AS $$
	DECLARE
		proc pg_proc%ROWTYPE;
		sid maybe_schema_ids;
		dont_care regtype;
	BEGIN
		SELECT * INTO proc FROM pg_proc pg
			WHERE pg.oid = NEW.oid_;
		IF NOT FOUND THEN
			RAISE EXCEPTION
			'Table % % error: % not in pg_proc',
	TG_TABLE_NAME, TG_OP, NEW.oid_;
			RETURN NULL;
		END IF;
		sid := declare_schema(proc.pronamespace);
		IF NEW.schema_ = 0 THEN
			NEW.schema_ := sid;
		ELSEIF NEW.schema_ != sid THEN
			RAISE EXCEPTION
			'Table % % error: proc % has schema % not %',
	TG_TABLE_NAME, TG_OP, NEW.oid_, NEW.schema_, sid;
		END IF;
		IF NEW.rettype = 0 THEN
			NEW.rettype = proc.prorettype;
		ELSEIF NEW.rettype != proc.prorettype THEN
			RAISE EXCEPTION
	'Table % % error: proc % oid % has rettype % not %',
	TG_TABLE_NAME, TG_OP, proc.proname,
	NEW.oid_, NEW.rettype, proc.prorettype;
		END IF;
		dont_care := declare_type(proc.prorettype);
		RETURN NEW;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION our_procs_before_inserter() IS
'ensures proc exists in pg_proc, ensures return type exists';

DROP TRIGGER IF EXISTS our_procs_before_inserter ON our_procs CASCADE;

CREATE TRIGGER our_procs_before_inserter
	BEFORE INSERT ON our_procs
	FOR EACH ROW EXECUTE PROCEDURE our_procs_before_inserter();

 -- create argument type rows in our_procs_types
CREATE OR REPLACE
FUNCTION our_procs_after_inserter() RETURNS trigger AS $$
	DECLARE
		proc RECORD;
		i INTEGER;
	BEGIN
		SELECT * INTO proc FROM pg_proc pg
			WHERE pg.oid = NEW.oid_;
		FOR i IN
array_lower(proc.proargtypes,1)..array_upper(proc.proargtypes,1)
		LOOP
			BEGIN
				INSERT INTO our_procs_types(proc_oid, arg_type, arg_i)
				VALUES(NEW.oid_, proc.proargtypes[i], i);
			EXCEPTION
					WHEN unique_violation THEN
						-- already there, all's cool!
			END;
		END LOOP;
		RETURN NEW;			-- ignored since is an after trigger
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION our_procs_after_inserter() IS
'creates our_procs_types rows corresponding to pg_proc.proargtypes';

DROP TRIGGER IF EXISTS our_procs_after_inserter ON our_procs CASCADE;

CREATE TRIGGER our_procs_after_inserter
	AFTER INSERT ON our_procs
	FOR EACH ROW EXECUTE PROCEDURE our_procs_after_inserter();

-- CREATE OR REPLACE
-- FUNCTION our_procs_deleter() RETURNS trigger AS $$
--   BEGIN
--     DELETE FROM our_procs_types WHERE proc_oid = OLD.oid_;
--     RETURN NULL;
--   END
-- $$ LANGUAGE plpgsql;
-- COMMENT ON FUNCTION our_procs_deleter() IS
-- 'delete our_procs_types rows holding argtypes data on expiring proc';

-- DROP TRIGGER IF EXISTS our_procs_deleter ON our_procs CASCADE;

-- CREATE TRIGGER our_procs_deleter
-- 	BEFORE DELETE ON our_procs
-- 	FOR EACH ROW EXECUTE PROCEDURE our_procs_deleter();

CREATE OR REPLACE
FUNCTION assert_proc_exists(regprocedure) RETURNS oid AS $$
DECLARE
	ns_oid oid;
BEGIN
	SELECT pronamespace INTO ns_oid FROM pg_proc WHERE oid = $1;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'assert_proc_exists: No regprocedure %', $1;
	END IF;
	RETURN ns_oid;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION declare_proc(regprocedure) RETURNS regprocedure AS $$
	DECLARE
		ns oid := assert_proc_exists($1);
		proc_schema maybe_schema_ids := declare_schema(ns);
		our_procs_schema maybe_schema_ids;
		kilroy_was_here boolean := false;
		this regprocedure := 'declare_proc(regprocedure)';
	BEGIN
		LOOP
			SELECT schema_ INTO our_procs_schema
			FROM our_procs WHERE oid_ = $1;
			IF FOUND THEN
				IF our_procs_schema != proc_schema THEN
					RAISE EXCEPTION
						'% error: pg_proc schema % != our_procs schema %',
						this, proc_schema, our_procs_schema; 
				END IF;
				RETURN $1;
			END IF;
			IF kilroy_was_here THEN
				RAISE EXCEPTION '% looping on %', this, $1; 
			END IF;
			kilroy_was_here := true;
			BEGIN
				INSERT INTO our_procs(oid_, schema_, rettype, name_, argtypes)
				SELECT $1, id, prorettype, proname , proargtypes
				FROM pg_proc, our_namespaces
				WHERE pg_proc.oid = $1 AND pg_proc.pronamespace = schema_oid;
				RETURN $1;
			EXCEPTION
				WHEN unique_violation THEN	-- another thread?
					RAISE NOTICE '% % raised %!', this, $1, 'unique_violation';
			END;
		END LOOP;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION declare_proc(regprocedure) IS
'Ensures the given regprocedure exists in pg_proc and that the
associated schema exists in our_namespaces and is consistent,
creating our_namespaces and spx_schema_names rows if necessary.';

-- why are these here???

SELECT declare_proc('declare_type(regtype)');

SELECT declare_proc('declare_proc(regprocedure)');
