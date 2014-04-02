-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-meta-triggers.sql', '$Id');

--	Wicci Project
--	Meta Code for managing triggers and key tables

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- * metacode for managing key tables

-- generalize function generation code and add to sql meta code??
-- upgrade to use meta_code.sql:create_trigger!!

CREATE OR REPLACE
FUNCTION key_insert_trigger_sig_(regclass) RETURNS text AS $$
	 SELECT $1::text || '_insert_trigger' || '()'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION key_insert_trigger_name_(regclass) RETURNS text AS $$
	 SELECT $1::text || '_key_insert_trigger'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION key_delete_trigger_sig_(regclass) RETURNS text AS $$
	 SELECT $1::text || '_delete_trigger' || '()'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION key_delete_trigger_name_(regclass) RETURNS text AS $$
	 SELECT $1::text || '_key_delete_trigger'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION trigger_function_text(sig_ text, body_ text) RETURNS text AS $$
	 SELECT E'CREATE OR REPLACE\n' || 'FUNCTION ' || $1
	|| E' RETURNS trigger\n'
	|| 'LANGUAGE plpgsql AS $body$ ' || $2 || '$body$'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION key_insert_trigger_function_text_(regclass, text = 'ref')
RETURNS text AS $x$
SELECT trigger_function_text(
	key_insert_trigger_sig_($1), $$
	BEGIN
		IF TG_OP = 'INSERT' THEN
			INSERT INTO $$ || $1 || ' VALUES ( NEW.' || $2 || $$ );
			RETURN NEW;
		ELSE
			RAISE NOTICE 'Unexpected operation!';
			RETURN NULL;
		END IF;
	END
$$
)
$x$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION key_delete_trigger_function_text_(regclass, text = 'ref')
RETURNS text AS $x$
SELECT trigger_function_text(
	key_delete_trigger_sig_($1), $$
	BEGIN
		IF TG_OP = 'DELETE' THEN
			DELETE FROM $$ || $1 || ' WHERE key = OLD.' || $2 || $$;
		ELSE
			RAISE NOTICE 'Unexpected operation!';
		END IF;
		RETURN NULL;
	END
$$
)
$x$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_key_insert_trigger_function_for(
	regclass, text = 'ref'
) RETURNS regclass AS $$
	SELECT meta_execute(
		'create_key_insert_trigger_function_for(regclass, text)',
		key_insert_trigger_function_text_($1, $2)
	);
	SELECT $1;
$$ LANGUAGE sql;
COMMENT ON
FUNCTION create_key_insert_trigger_function_for(regclass, text)
IS 'Creates insert trigger to maintain keys on associated tables.';

CREATE OR REPLACE FUNCTION create_key_delete_trigger_function_for(
	regclass, text = 'ref'
) RETURNS regclass AS $$
	SELECT meta_execute(
		'create_key_delete_trigger_function_for(regclass, text)',
		key_delete_trigger_function_text_($1, $2)
	);
	SELECT $1;
$$ LANGUAGE sql;
COMMENT ON
FUNCTION create_key_delete_trigger_function_for(regclass, text)
IS 'Creates delete trigger to maintain keys on associated tables.';

CREATE OR REPLACE
FUNCTION create_key_trigger_functions_for(regclass, text = 'ref')
RETURNS regclass AS $$
	SELECT create_key_insert_trigger_function_for($1, $2);
	SELECT create_key_delete_trigger_function_for($1, $2)
$$ LANGUAGE sql;
COMMENT ON
FUNCTION create_key_trigger_functions_for(regclass, text)
IS 'Creates trigger functions to maintain keys on associated tables.';

CREATE OR REPLACE FUNCTION create_key_insert_trigger_for(
	regclass, key_class_ regclass
) RETURNS regclass AS $$
DECLARE
	this regprocedure :=
		'create_key_insert_trigger_for(regclass, regclass)';
	inserter_name text := key_insert_trigger_name_($1);
	drop_result boolean :=
		meta_execute(this, drop_trigger_text($1, inserter_name));
	create_result boolean :=
		meta_execute( this, before_trigger_text(
			$1, inserter_name,
			key_insert_trigger_sig_($2)::regprocedure, 'INSERT'
	) );
BEGIN
	RETURN $1;
END
$$ LANGUAGE plpgsql;
COMMENT ON
FUNCTION create_key_insert_trigger_for(regclass, regclass)
IS 'Attaches insert trigger to maintain associated key table.';

CREATE OR REPLACE FUNCTION create_key_delete_trigger_for(
	regclass, key_class_ regclass
) RETURNS regclass AS $$
DECLARE
	this regprocedure :=
		'create_key_delete_trigger_for(regclass, regclass)';
	deleter_name text := key_delete_trigger_name_($1);
	drop_result boolean :=
		meta_execute( this, drop_trigger_text($1, deleter_name) );
	create_result boolean :=
		meta_execute( this, after_trigger_text(
			$1, deleter_name,
			key_delete_trigger_sig_($2)::regprocedure,
			'DELETE'
	) );
BEGIN RETURN $1; END
$$ LANGUAGE plpgsql;
COMMENT ON
FUNCTION create_key_delete_trigger_for(regclass, regclass)
IS 'Attaches delete trigger to maintain associated key table.';

CREATE OR REPLACE
FUNCTION create_key_triggers_for(regclass, key_class_ regclass)
RETURNS regclass AS $$
	SELECT create_key_insert_trigger_for($1, $2);
	SELECT create_key_delete_trigger_for($1, $2)
$$ LANGUAGE sql;
COMMENT ON
FUNCTION create_key_triggers_for(regclass, regclass)
IS 'Attaches triggers to maintain associated key table.';
