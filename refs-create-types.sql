-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-create-types.sql', '$Id');

--	Wicci Project
--	Generate boilerplate to make a  ref type

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- ** Dependencies

-- refs: ref_cmp, ref_lt, ref_le, ref_eq, ref_neq, ref_ge
-- spx.so: call_in_method(cstring, oid, integer) --> refs, call_out_method(refs) --> cstring

-- * create_ref_op_class

-- ** Helper Functions

--  ::refs ---> '-->this-proc-returns<--' ??
CREATE OR REPLACE
FUNCTION gen_ref_op_func(regtype, text, regtype DEFAULT 'bool')
RETURNS meta_funcs AS $$
	SELECT 	meta_sql_func(
		_name := $1 || '_' || $2,
		_args := ARRAY[arg, arg],
		_returns := $3,
		_stability := 'meta__immutable',
		_strict := 'meta__strict', -- operators are normally strict in pgsql
		_body := 'SELECT ' || quote_ident('ref_' || $2) || '($1::s1_refs.refs, $2::s1_refs.refs)',
		_ := 'gen_ref_op_func'
	) FROM meta_arg($1) arg
$$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION gen_ref_op_func(regtype, text, regtype) IS
$_$ E.g.: meta_func_text(gen_ref_op_func('env_refs', 'cmp', 'int4')) -->
 CREATE OR REPLACE
 FUNCTION env_cmp(env_refs, env_refs) RETURNS int4 AS $$
	 SELECT ref_cmp($1::s1_refs.refs, $2::s1_refs.refs)
 $$ LANGUAGE SQL IMMUTABLE;
 -- Note the s1_refs schema qualifications.
$_$;

-- gen_ref_operator(type, name, op, commutator, negator, sel)
CREATE OR REPLACE
FUNCTION gen_ref_operator(regtype, text, text, text, text, text)
RETURNS text AS $_$
	SELECT $$CREATE OPERATOR $$|| $3 ||$$ (
	 leftarg = $$|| $1 ||$$,
	 rightarg = $$|| $1 ||$$,
	 procedure = $$|| $1 || '_' || $2 ||$$,
	 commutator = $$|| $4 ||$$,
	 negator = $$|| $5 ||$$,
	 restrict = $$|| $6 || 'sel' ||$$,
	 join = $$|| $6 || 'joinsel' ||$$
);
$$
$_$ LANGUAGE SQL IMMUTABLE STRICT;
COMMENT ON FUNCTION
gen_ref_operator(regtype, text, text, text, text, text) IS $$
gen_ref_operator(type, tag, operator, commutator, negator, restrict/join)
For example:
gen_ref_operator('env_refs', 'lt', '<', '>', '>=', 'scalarlt') -->
 CREATE OPERATOR < (
		leftarg = env_refs,
		rightarg = env_refs,
		procedure = env_lt,
		commutator = >,
		negator = >=,
		restrict = scalarltsel,
		join = scalarltjoinsel
 );
$$;

CREATE OR REPLACE
FUNCTION gen_ref_operator_class(regtype) RETURNS text AS $_$
	SELECT $$ CREATE OPERATOR CLASS $$|| $1 || '_ops' ||$$
		DEFAULT FOR TYPE $$|| $1 ||$$ USING btree AS
				OPERATOR        1       < ,
				OPERATOR        2       <= ,
				OPERATOR        3       = ,
				OPERATOR        4       >= ,
				OPERATOR        5       > ,
				FUNCTION        1       $$|| $1 || '_cmp(' || $1 || ', ' || $1 || ');'
$_$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION gen_ref_operator_class(regtype) IS $$
For example, gen_ref_operator_class('env_refs') returns:
	CREATE OPERATOR CLASS env_ops
		 DEFAULT FOR TYPE env_refs USING btree AS
				 OPERATOR        1       < ,
				 OPERATOR        2       <= ,
				 OPERATOR        3       = ,
				 OPERATOR        4       >= ,
				 OPERATOR        5       > ,
				 FUNCTION        1       env_cmp(env_refs, env_refs);
$$;

-- ** Creation functions

CREATE OR REPLACE FUNCTION show_ref_op_func(
	regtype, text, regtype DEFAULT 'bool'
) RETURNS text AS $$
	SELECT meta_func_text(gen_ref_op_func($1, $2, $3))
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION show_ref_op(regtype, text, text, text, text, text)
RETURNS text AS $$
	SELECT show_ref_op_func($1, $2) || E'\n'
	|| gen_ref_operator($1, $2, $3, $4, $5, $6);
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION create_ref_op(regtype, text, text, text, text, text)
RETURNS regprocedure AS $$
	SELECT _func
	FROM create_func(gen_ref_op_func($1, $2)) _func
	WHERE _func IS NOT NULL AND meta_execute(
		'create_ref_op(regtype, text, text, text, text, text)',
		gen_ref_operator($1, $2, $3, $4, $5, $6)
	)
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION show_ref_op_class(regtype) RETURNS text AS $$
	SELECT show_ref_op_func($1, 'cmp', 'int4') || E'\n'
	|| show_ref_op($1, 'lt', '<', '>', '>=', 'scalarlt') || E'\n'
	|| show_ref_op($1, 'le', '<=', '>=', '>', 'scalarlt') || E'\n'
	|| show_ref_op($1, 'eq', '=', '=', '<>', 'eq') || E'\n'
	|| show_ref_op($1, 'neq', '<>', '<>', '=', 'neq') || E'\n'
	|| show_ref_op($1, 'ge', '>=', '<=', '<', 'scalargt') || E'\n'
	|| show_ref_op($1, 'gt', '>', '<', '<=', 'scalargt') || E'\n'
	|| gen_ref_operator_class($1) || E'\n';
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION create_ref_op_class(regtype) RETURNS boolean AS $$
	SELECT create_func( gen_ref_op_func($1, 'cmp', 'int4') );
	SELECT create_ref_op($1, 'lt', '<', '>', '>=', 'scalarlt');
	SELECT create_ref_op($1, 'le', '<=', '>=', '>', 'scalarlt');
	SELECT create_ref_op($1, 'eq', '=', '=', '<>', 'eq');
	SELECT create_ref_op($1, 'neq', '<>', '<>', '=', 'neq');
	SELECT create_ref_op($1, 'ge', '>=', '<=', '<', 'scalargt');
	SELECT create_ref_op($1, 'gt', '>', '<', '<=', 'scalargt');;
	SELECT meta_execute(
		'create_ref_op_class(regtype)',
		gen_ref_operator_class($1)
	)
$$ LANGUAGE sql;

COMMENT ON FUNCTION create_ref_op_class(regtype) IS $$
	Defines the boilerplate necessary for allowing the given
	ref type to be comparable.  This involves creating
	7 functions,  6 operators and 1 operator class.
$$;

-- * create_ref_type

CREATE OR REPLACE
FUNCTION create_shell_type(text) RETURNS regtype AS $$
DECLARE
	_type regtype;
	this regprocedure := 'create_shell_type(text)';
BEGIN
	SELECT oid INTO _type FROM pg_type WHERE typname = $1;
	IF FOUND THEN
		RAISE NOTICE '%: % already exists!', this, $1;
		RETURN _type;
	END IF;
	PERFORM meta_execute(this, 'CREATE TYPE ' || $1);
	SELECT oid INTO _type FROM pg_type WHERE typname = $1;
	IF FOUND THEN
		RAISE NOTICE 'created shell type %', $1;
	ELSE
		RAISE EXCEPTION 'failed to create shell type %', $1;
	END IF;
	RETURN _type;
END
$$ LANGUAGE plpgsql;

-- ** type procedure support

CREATE OR REPLACE
FUNCTION gen_proc_sig_text(text, regtype[]) RETURNS text AS $$
	SELECT $1 || '(' || array_to_string($2, ',') || ')'
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION gen_c_function_text(
	regtype, text, regtype[], text, text=NULL, text='STRICT VOLATILE'
) RETURNS text AS $$
	SELECT
		'CREATE OR REPLACE '  || E'\n'
		|| 'FUNCTION ' || _sig  || E'\n'
		|| 'RETURNS ' || $1 || E'\n'
		|| 'LANGUAGE ' || quote_literal('c') || ' ' || $6 || E'\n'
		|| 'AS ' || quote_literal($4)
		|| COALESCE (', ' || quote_literal($5), '') || ';'
	FROM gen_proc_sig_text($2, $3) _sig
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_c_function(
	regtype, text, regtype[], text, text, text='STRICT VOLATILE'
) RETURNS regprocedure AS $$
DECLARE
	_sig text := gen_proc_sig_text($2, $3);
	_create text := gen_c_function_text($1, $2, $3, $4, $5, $6);
	_proc regprocedure;
	this regprocedure :=
		'create_c_function(regtype, text, regtype[], text, text, text)';
BEGIN
	PERFORM meta_execute(this, _create);
--		EXECUTE 'SELECT CAST($1 AS regprocedure)' INTO _proc USING $1;
	SELECT oid INTO _proc FROM pg_proc WHERE proname = $2;
	IF FOUND THEN
		RAISE NOTICE 'CREATED %', _sig;
	ELSE
		RAISE EXCEPTION 'failed to %', _create;
	END IF;
	RETURN _proc;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION create_ref_c_func(regtype, text, text, VARIADIC regtype[])
RETURNS regprocedure AS $$
	SELECT create_c_function($1, $2, $4, 'spx.so', $3)
$$ LANGUAGE sql;

-- * types

-- can the _in and _out procedures be full signatures???
CREATE OR REPLACE FUNCTION gen_ref_type_text(
	regtype, regprocedure, regprocedure
) RETURNS TEXT AS $$
	SELECT 'CREATE TYPE ' || $1 || E' (\n'
	|| '  INPUT = ' || $2::regproc || E',\n'
	|| '  OUTPUT = ' || $3::regproc || E',\n'
	|| '  LIKE = refs' || E',\n'
	|| '  CATEGORY = ' || quote_literal('t') || E'\n'
	|| ')'
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_ref_type(
	regtype, regprocedure, regprocedure
) RETURNS regtype AS $$
DECLARE
	_create TEXT := gen_ref_type_text($1, $2, $3);
	_type regtype;
	this regprocedure :=
'create_ref_type(regtype, regprocedure, regprocedure)';
BEGIN
	PERFORM meta_execute(this, _create);
	SELECT oid INTO _type FROM pg_type
	WHERE oid = $1 AND typinput = $2 AND typoutput = $3;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'failed to %', _create;
	END IF;
	RAISE NOTICE '%', _create;
	RETURN _type;
END
$$ LANGUAGE plpgsql;

-- * domains

CREATE OR REPLACE
FUNCTION gen_domain_text(text, regtype, boolean=false, text=NULL)
RETURNS text AS $$
	SELECT 'CREATE DOMAIN ' || $1 || ' AS ' || $2
	|| CASE WHEN $3 THEN ' NOT NULL' ELSE '' END
	|| COALESCE(E'\n\tCHECK(' || $4 || ')', '')
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION create_domain(text, regtype, boolean=false, text=NULL)
RETURNS boolean AS $$
	SELECT drop_entity('meta__domain', $1);
	SELECT meta_execute(
		'create_domain(text, regtype, boolean, text)',
		gen_domain_text($1, $2, $3, $4)
	);
$$ LANGUAGE sql;

-- * casts

CREATE OR REPLACE
FUNCTION gen_cast_sig_text(regtype, regtype) RETURNS text AS $$
	SELECT  '(' || $1 || ' AS ' || $2 || ')'
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION gen_cast_text(regtype, regtype, regprocedure, boolean=false)
RETURNS text AS $$
	SELECT 'CREATE CAST ' || gen_cast_sig_text($1, $2)
	|| ' ' || COALESCE('WITH FUNCTION ' || $3, 'WITHOUT FUNCTION')
	|| CASE WHEN $4 THEN ' AS IMPLICIT' ELSE '' END
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION create_cast(regtype, regtype, regprocedure, boolean=true)
RETURNS boolean AS $$
	SELECT drop_entity('meta__cast', gen_cast_sig_text($1, $2));
	SELECT meta_execute(
		'create_cast(regtype, regtype, regprocedure, boolean)',
		gen_cast_text($1, $2, $3, $4)
	)
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION create_ref_type_(text, VARIADIC regtype[] = '{}')
RETURNS regtype AS $$
DECLARE
	_bases regtype[] = $2 || 'refs'::regtype;
	_base regtype;										-- element of _bases
	_type regtype := create_shell_type($1);
	_in_proc regprocedure := create_ref_c_func(
		_type, $1 || '_in_op', 'call_in_method', 'cstring', 'oid', 'integer'
	);
	_out_proc regprocedure := create_ref_c_func(
		'cstring', $1 || '_out_op', 'call_out_method', _type
	);
	this regprocedure := 'create_ref_type(text, regtype[])';
BEGIN
	PERFORM create_ref_type(_type, _in_proc, _out_proc);
	PERFORM create_cast('unchecked_refs', _type, NULL); -- !!
	FOREACH _base IN ARRAY _bases LOOP
		PERFORM create_cast(_type, _base, NULL, true);
		PERFORM create_cast(t.typarray, b.typarray, 'to_array_ref(ANYARRAY)')
		FROM pg_type t, pg_type b WHERE t.oid = _type AND b.oid=_base;
	END LOOP;
	PERFORM create_ref_op_class(_type);
	RETURN _type;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE
FUNCTION create_ref_type(text, VARIADIC regtype[] = '{}')
RETURNS regtype AS $$
DECLARE
	_type regtype;
	this regprocedure := 'create_ref_type(text, regtype[])';
BEGIN
	SELECT oid INTO _type FROM pg_type WHERE typname = $1;
	IF FOUND THEN
		RAISE NOTICE '%: % already exists!', this, $1;
		RETURN _type;
	END IF;
	RETURN create_ref_type_($1, VARIADIC $2);
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_ref_type(text, regtype[]) IS
'Creates a new typed object reference type of the given name.
Also creates safe casts to refs and refs[].
Also creates an unsafe cast from unchecked_refs. 
Any implementations or methods must be created separately.';

CREATE OR REPLACE
FUNCTION create_ref_subtype(text, regtype, regtype=NULL)
RETURNS regtype AS $$
	SELECT CASE
		WHEN create_domain('maybe_' || $1, COALESCE($3,$2), false)
--			AND create_domain($1, $2, true)	-- better, but more debugging needed
			AND create_domain($1, $2, false) -- kludge for now
			AND CASE WHEN $3 IS NOT NULL THEN
				create_domain($1 || '_arrays', ($3 || '[]')::regtype)
				ELSE true
			END
		THEN $1::regtype
	END
$$ LANGUAGE sql;
COMMENT ON FUNCTION create_ref_subtype(text, regtype, regtype) IS
'Creates new domains named $1 and maybe_$1 to reference a subset
of the values of the given type $2.  The former is constrained to be
NOT NULL.  The new types are less general, more specialized.
Kludge: When $2 is itself a subtype, supply the real type as $3.
There is another version of this function which implements subtypes
as full ref types - check it out!';
