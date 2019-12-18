-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-code.sql', '$Id');

--	Wicci Project
--	Typed Object Code

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- See Notes/ref.txt for background

CREATE OR REPLACE
FUNCTION refs_clean() RETURNS void AS $$
	SELECT spx_clean();
	DELETE FROM typed_object_classes WHERE NOT EXISTS
		(SELECT p.oid FROM pg_class p WHERE p.oid = class_);
	DELETE FROM typed_object_classes WHERE NOT EXISTS
	(SELECT p.oid FROM pg_type p WHERE p.oid = type_);
	DELETE FROM typed_object_classes
		WHERE CASE WHEN out_ IS NULL THEN false ELSE NOT EXISTS
	(SELECT p.oid FROM pg_proc p WHERE p.oid = out_)
		END;
	DELETE FROM typed_object_classes
		WHERE CASE WHEN in_ IS NULL THEN false ELSE NOT EXISTS
	(SELECT p.oid FROM pg_proc p WHERE p.oid = in_)
		END;
	DELETE FROM typed_object_methods WHERE NOT EXISTS
	(SELECT p.oid FROM pg_proc p WHERE p.oid = operation_);
	DELETE FROM typed_object_methods WHERE NOT EXISTS
	(SELECT p.oid FROM pg_proc p WHERE p.oid = method_);
$$ LANGUAGE SQL;
COMMENT ON FUNCTION refs_clean() IS
'Get rid of any garbage in typed_object_(classes|methods).';

-- * typed_object_classes getters

-- reimplement in C ???

CREATE OR REPLACE
FUNCTION ref_type(refs) RETURNS regtype AS $$
	SELECT type_ FROM typed_object_classes
	WHERE tag_ = ref_tag($1)
$$ LANGUAGE SQL STABLE;
COMMENT ON FUNCTION ref_type(refs) IS '
Returns the oid of the class associated with a ref';

CREATE OR REPLACE
FUNCTION ref_table(refs) RETURNS regclass AS $$
	SELECT class_ FROM typed_object_classes
	WHERE tag_ = ref_tag($1)
$$ LANGUAGE SQL STABLE;
COMMENT ON FUNCTION ref_table(refs) IS '
Returns the oid of the class associated with a ref';

CREATE OR REPLACE
FUNCTION try_type_class_tag(regtype, regclass)
RETURNS ref_tags AS $$
	SELECT tag_ FROM typed_object_classes
	WHERE type_ = $1 AND class_ = $2
$$ LANGUAGE sql;
COMMENT ON FUNCTION try_type_class_tag(regtype, regclass) IS
'Tries to find tag given type and table';

CREATE OR REPLACE
FUNCTION type_class_tag(regtype, regclass)
RETURNS ref_tags AS $$
	SELECT non_null(
		try_type_class_tag($1, $2), 'type_class_tag(regtype, regclass)'
	)
$$ LANGUAGE sql;
COMMENT ON FUNCTION type_class_tag(regtype, regclass) IS
'Finds tag given type and table';

CREATE OR REPLACE FUNCTION unchecked_ref(
	regtype, regclass, ref_ids
) RETURNS unchecked_refs AS $$
	SELECT non_null(
		unchecked_ref_from_tag_id(type_class_tag($1, $2), $3),
		'unchecked_ref(regtype, regclass, ref_ids)'
	)
$$ LANGUAGE SQL STABLE;

-- * type refs and functions

CREATE OR REPLACE
FUNCTION  ref_no_eval(refs, refs) RETURNS refs AS $$
	SELECT $1
$$ LANGUAGE sql IMMUTABLE;

-- ++ is_array_ref_of(tag, refs[]) -> boolean
CREATE OR REPLACE
FUNCTION is_array_ref_of(ref_tags, refs[]) RETURNS boolean AS $$
	SELECT $1 = ALL(SELECT ref_tag(x) FROM unnest($2) x)
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION bad_tag(ref_tags) RETURNS boolean AS $$
	SELECT EXISTS(
		SELECT tag_ FROM typed_object_classes WHERE tag_ = $1
	)
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE
FUNCTION bad_ref(refs) RETURNS boolean AS $$
	SELECT bad_tag(ref_tag($1))
$$ LANGUAGE sql STABLE;

-- ++ is_array_ref_of(type, table, refs[]) -> boolean
CREATE OR REPLACE
FUNCTION is_array_ref_of(regtype, regclass, refs[])
RETURNS boolean AS $$
	SELECT non_null(
		is_array_ref_of( type_class_tag($1, $2), $3 ),
		'is_array_ref_of(regtype, regclass, refs[])'
	)
$$ LANGUAGE sql;

/*
CREATE OR REPLACE FUNCTION assert_array_ref_of(
	regprocedure, ref_tags, refs[]
) RETURNS void AS $$
	SELECT debug_assert_failed($1, x, ARRAY['ref_tag'])
	FROM unnest($3) x WHERE $2 <> ref_tag(x)
$$ LANGUAGE sql;
*/

/*
CREATE OR REPLACE FUNCTION assert_array_ref_of(
	regtype, regclass, refs[], regprocedure
) RETURNS void AS $$
	SELECT debug_assert(
		$1,
		is_array_ref_of($2, $3, $4),
		'expected array of type ' || $2 || ' class ' || $3
		$4
	)
$$ LANGUAGE sql;
*/

CREATE OR REPLACE
FUNCTION to_array_ref(ANYARRAY) RETURNS refs[] AS $$
	SELECT ARRAY( SELECT x::refs FROM unnest($1) x )
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION xml_to_make(text) RETURNS text AS $$
	SELECT
		 CASE WHEN NOT xml_valid($1) THEN ''
		ELSE xslt_process('~/.Wicci/Lib/refs.xsl', $1)
		END
$$ LANGUAGE sql;
COMMENT ON FUNCTION xml_to_make(text) IS
'Convert xml representation of object tree into nested
factory function calls which will recreate/discover the
equivalent tree.';


-- ** ref_textout, ref_textin

CREATE OR REPLACE
FUNCTION ref_str_delim() RETURNS text AS $$
	SELECT ':'::text
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str_delim() IS
'The delimiter used in text representations of tors';

CREATE OR REPLACE
FUNCTION ref_str(text, ref_ids) RETURNS text AS $$
	SELECT $1 || ref_str_delim() || $2
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str(text, ref_ids) IS
'Returns a text representation of a ref.';

CREATE OR REPLACE
FUNCTION ref_str(text, text, ref_ids) RETURNS text AS $$
	SELECT $1 || ref_str_delim() || $2 || ref_str_delim() || $3
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str(text, text, ref_ids) IS
'Returns a text representation of a ref.';

CREATE OR REPLACE
FUNCTION ref_str(regclass, ref_ids) RETURNS text AS $$
	SELECT ref_str($1::text, $2)
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str(regclass, ref_ids) IS
'Returns a text representation of a ref';

CREATE OR REPLACE
FUNCTION ref_str(regtype, regclass, ref_ids) RETURNS text AS $$
	SELECT ref_str($1::text, $2::text, $3)
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str(regtype, regclass, ref_ids) IS
'Returns a text representation of a ref';

CREATE OR REPLACE
FUNCTION ref_str_match() RETURNS text AS $$
	SELECT
		E'^(?:(?:[a-z][a-z0-9_]*\\.)?([a-z][a-z0-9_]*)' || ref_str_delim() || ')?'
		|| E'((?:[a-z][a-z0-9_]*\\.)?[a-z][a-z0-9_]*)?' || ref_str_delim()
		|| '(-?[1-9][0-9]*)$'
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str_match() IS
'a regular expression matching a text representation of a ref
as an optional type name, an optional class name and an id';

CREATE OR REPLACE
FUNCTION ref_str_match(text) RETURNS boolean AS $$
	SELECT $1 ~ ref_str_match()
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str_match(text) IS
'does the given text match the pattern of a ref expressed as text';

CREATE OR REPLACE
FUNCTION ref_str_parse(text, OUT text, OUT text, OUT integer) AS $$
	SELECT x[1], x[2], x[3]::integer
	FROM regexp_matches($1, ref_str_match()) x LIMIT 1
$$ LANGUAGE sql;
COMMENT ON FUNCTION ref_str_parse(text) IS
'parses text into an optional type name, class name and ref_id;
none of these values are checked for referential integrity!';

/*
CREATE OR REPLACE
FUNCTION ref_textout__(regtype, regclass, ref_ids)
RETURNS text AS $$
	SELECT ref_str($1, $2, $3)
$$ LANGUAGE sql;
COMMENT ON
FUNCTION ref_textout__(regtype, regclass, ref_ids) IS
'Convert a ref of given type, class and id to to a text representation
which can be read back by ref_textin(text) into the same ref.';
*/

/*
-- why would we ever want this?
-- ref tag values are not stable!
CREATE OR REPLACE
FUNCTION ref_textout__(ref_tags, ref_ids)
RETURNS text AS $$
	SELECT $1::text || ':' || $2
$$ LANGUAGE sql;
*/

CREATE OR REPLACE
FUNCTION ref_textout(ref_tags, ref_ids)
RETURNS text AS $$
	SELECT ref_str( type_, class_, $2 )
	FROM typed_object_classes WHERE tag_ = $1
$$ LANGUAGE sql STABLE;
COMMENT ON
FUNCTION ref_textout(ref_tags, ref_ids) IS
'Convert a ref of given tag and id to to a text representation
which can be read back by ref_textin(text) into the same ref.';

/*
CREATE OR REPLACE
FUNCTION ref_textout(ref_tags, ref_ids)
RETURNS text AS $$
	SELECT COALESCE(
		ref_textout_($1, $2),
		ref_textout__($1, $2)
	)
$$ LANGUAGE sql;
*/

CREATE OR REPLACE
FUNCTION ref_textout(refs) RETURNS text AS $$
	SELECT ref_textout( ref_tag($1), ref_id($1) )
$$ LANGUAGE sql STABLE;
COMMENT ON FUNCTION ref_textout(refs) IS
'Convert a ref to a text output which is reasonably compact and
human readable, and which can be read back into the same ref.';

CREATE OR REPLACE
FUNCTION try_show_ref(refs, ref_tags=NULL)
RETURNS text AS $$
	SELECT ref_text_op($1) FROM typed_object_methods
	WHERE tag_ = COALESCE($2, ref_tag($1))
	AND operation_ = this('ref_text_op(refs)')
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION show_ref(refs, text=NULL) RETURNS text AS $$
	SELECT
		COALESCE($2 || ': ', '') ||
		COALESCE( try_show_ref($1), ref_textout($1) )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION case_failed_any_ref(
	regprocedure, ANYELEMENT, refs, VARIADIC text[] = '{}'
) RETURNS ANYELEMENT AS $$
	SELECT debug_fail(
		$1, $2, VARIADIC ARRAY['case failed on '] || show_ref($3) || $4
	);
$$ LANGUAGE SQL;
COMMENT ON FUNCTION case_failed_any_ref(
	regprocedure, ANYELEMENT, refs, text[]
) IS 'handy for failed case expressions';

CREATE OR REPLACE FUNCTION try_unchecked_ref(
	regtype, regclass, ref_ids
) RETURNS unchecked_refs AS $$
	SELECT unchecked_ref_from_tag_id(tag_, $3)
	FROM typed_object_classes toc
	WHERE COALESCE($1 = toc.type_, true) AND $2 = toc.class_
$$ LANGUAGE sql;

/*
CREATE OR REPLACE FUNCTION try_unchecked_ref_from_type_class_id(
	text, text, ref_ids
) RETURNS unchecked_refs AS $$
	SELECT try_unchecked_ref(
		_type.oid::regtype, _class.oid::regclass, $3
	) FROM pg_type _type, pg_class _class
	WHERE COALESCE($1 = typname, true) AND $2 = relname
$$ LANGUAGE sql;
*/

-- We need to check the validity of the type and class
-- names before forcing them!
-- The exists check won't work for the primary keys
-- of rows being inserted.
CREATE OR REPLACE
FUNCTION try_unchecked_ref(text)
RETURNS unchecked_refs AS $$
	SELECT try_unchecked_ref(
		_type::regtype, _class::regclass, _id::ref_ids
	) FROM ref_str_parse($1) AS foo(_type, _class, _id)
$$ LANGUAGE sql STRICT;

CREATE OR REPLACE
FUNCTION unchecked_ref(text) RETURNS unchecked_refs AS $$
	SELECT non_null(
		try_unchecked_ref($1), 'unchecked_ref(text)', $1
	)
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION ref_textin(text) RETURNS unchecked_refs AS $$
	SELECT COALESCE(
		try_unchecked_ref($1),
--		debug_fail(this, unchecked_ref_null(), 'no such ref ', $1)
		debug_fail(this, NULL::unchecked_refs, 'no such ref ', $1)
	)
	FROM debug_enter('ref_textin(text)', $1) this
	WHERE non_null($1, this) IS NOT NULL
$$ LANGUAGE sql;

COMMENT ON FUNCTION ref_textin(text) IS '
	What do we really need here?
	Do we need to warn or fail on NULL or just return it?
	Simplify along with
		- try_unchecked_ref(text)
		- unchecked_ref(text)
	and maybe get rid of one of them!
';

-- ** 

CREATE OR REPLACE
VIEW tag_operation_method_fallback_view AS
SELECT
	(-1)::integer AS tag_, operation_, fallback_ AS method_
FROM operations_fallbacks;
COMMENT ON VIEW tag_operation_method_fallback_view  IS
'"operations_fallbacks" as "typed_object_methods",
"fallback_" as "method_", -1 as "tag_".
Used by "refs.c" "LoadToms"';

CREATE OR REPLACE
VIEW tag_operation_method_view_ AS
	SELECT tag_, operation_, method_
	FROM tag_operation_method_fallback_view
UNION
	SELECT tag_, operation_, method_
	FROM typed_object_methods;

CREATE OR REPLACE
VIEW tag_operation_method_view AS
	SELECT tag_, operation_, method_
	FROM tag_operation_method_view_, pg_proc p1, pg_proc p2
	WHERE operation_ = p1.oid AND method_ = p2.oid;
COMMENT ON VIEW tag_operation_method_view  IS
'Used by "refs.c" "LoadToms"';

CREATE OR REPLACE
FUNCTION count_ops_by_tag(ref_tags) RETURNS bigint AS $$
	SELECT COUNT(operation_)
	FROM typed_object_methods WHERE tag_ = $1
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION max_class_tag() RETURNS ref_tags AS $$
	SELECT MAX(tag_)::ref_tags FROM typed_object_classes
$$ LANGUAGE sql;

CREATE OR REPLACE VIEW
tag_class_type_out_in_numops_maxtag_view AS SELECT tag_,
class_, type_, out_, in_, count_ops_by_tag(tag_)::integer AS
numops_, max_class_tag() AS maxtag_ FROM
typed_object_classes WHERE type_ IN (SELECT oid FROM
pg_type) AND class_ IN (SELECT oid FROM pg_class) AND
COALESCE( in_ IN (SELECT oid FROM pg_proc), true ) AND
COALESCE( out_ IN (SELECT oid FROM pg_proc), true ); COMMENT
ON VIEW tag_class_type_out_in_numops_maxtag_view IS '
This rather paranoid VIEW is used by FUNCTION
unsafe_refs_load_tocs in spx.so to load the
typed_object_classes into a session cache.';

/*
CREATE OR REPLACE
FUNCTION refs_ready() RETURNS regprocedure[] AS $$
-- 	PERFORM require_module('s1_refs.refs-code');
	SELECT spx_ready() || ARRAY[
		'unsafe_refs_load_toms()',
		'unsafe_refs_load_tocs()'
	]::regprocedure[]
	FROM refs_base_init();
$$ LANGUAGE sql;
COMMENT ON FUNCTION refs_ready() IS '
	Ensure that all modules of the refs schema
	are present and initialized.
';

CREATE OR REPLACE
FUNCTION unsafe_refs_initialize() RETURNS cstring
AS 'spx.so' LANGUAGE C STRICT;

CREATE OR REPLACE
FUNCTION refs_ready() RETURNS regprocedure[] AS $$
	SELECT spx_ready || ARRAY[
		'unsafe_refs_load_toms()',	'unsafe_refs_load_tocs()'
	]::regprocedure[] FROM
		spx_ready(), unsafe_refs_load_toms(), unsafe_refs_load_tocs(), unsafe_refs_initialize()
$$ LANGUAGE sql;
COMMENT ON FUNCTION refs_ready() IS '
	Ensure that all modules of the refs schema
	are present and initialized.
';
*/

CREATE OR REPLACE
FUNCTION refs_initialized() RETURNS bool
AS 'spx.so' LANGUAGE C STRICT;

CREATE OR REPLACE
FUNCTION ensure_schema_ready() RETURNS text AS $$
	SELECT CASE refs_initialized()
	WHEN true THEN 'Already Initialized'
	WHEN false THEN refs_base_init()::text
  END
$$ LANGUAGE sql;

-- * typed_object_classes registry support

-- ** Old API requires tag

/*
CREATE OR REPLACE FUNCTION tag_type_class(
	ref_tags, regtype, regclass,
	regprocedure='ref_textin(text)',
	regprocedure='ref_text(refs)'
) RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes%ROWTYPE;
		this regprocedure :=
	'tag_type_class(ref_tags, regtype, regclass, regprocedure, regprocedure)';
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes WHERE tag_ = $1;
			IF FOUND THEN
				IF row.tag_ = $1 AND row.type_ = $2 AND row.class_ = $3
					AND $4 IS NOT DISTINCT FROM in_, true)
					AND $5 IS NOT DISTINCT FROM out_, true)
				THEN RETURN row;
				ELSE RAISE EXCEPTION '% % % % != % % %',
					this, $1, $2, $3, row.tag_, row.type_, row.class_;
				END IF;
			END IF;
			BEGIN
				INSERT INTO typed_object_classes(tag_,type_,class_, in_, out_)
				VALUES($1, $2, $3, $4, $5);
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION tag_type_class(ref_tags, regtype, regclass) IS
'Registers typed_object_classes';
*/

/*
CREATE OR REPLACE
FUNCTION tag_input_method(ref_tags, regprocedure)
RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes;
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes WHERE tag_ = $1;
			IF FOUND AND row.in_ IS NOT NULL THEN
				IF row.in_ = $2 THEN
					RETURN row;
				ELSE
					RAISE EXCEPTION 'tag_input_method: % % != %',
					$1, $2, row.in_;
				END IF;
			END IF;
			BEGIN
				UPDATE typed_object_classes SET in_=$2 WHERE tag_ = $1;
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION tag_input_method(ref_tags,regprocedure)
IS 'Registers typed_object input method';
*/

/*
CREATE OR REPLACE
FUNCTION tag_output_method(ref_tags, regprocedure)
RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes;
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes WHERE tag_ = $1;
			IF FOUND AND row.out_ IS NOT NULL THEN
				IF row.out_ = $2 THEN
					RETURN row;
				ELSE
					RAISE EXCEPTION 'tag_input_method: % % != %',
					$1, $2, row.out_;
				END IF;
			END IF;
			BEGIN
				UPDATE typed_object_classes SET out_=$2 WHERE tag_ = $1;
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION tag_output_method(ref_tags,regprocedure)
IS 'Registers typed_object output method';
*/

/*
CREATE OR REPLACE
FUNCTION tag_type_class_io(ref_tags, regtype, regclass, regprocedure, regprocedure)
RETURNS typed_object_classes AS $$
	SELECT tag_type_class($1, $2, $3);
	SELECT tag_input_method($1, $4);
	SELECT tag_output_method($1, $5);
$$ LANGUAGE sql;
*/

/*
CREATE OR REPLACE
FUNCTION tag_type_class_input(ref_tags, regtype, regclass, regprocedure)
RETURNS typed_object_classes AS $$
	SELECT tag_type_class($1, $2, $3);
	SELECT tag_input_method($1, $4)
$$ LANGUAGE sql;
*/

/*
CREATE OR REPLACE
FUNCTION tag_type_class_output(ref_tags, regtype, regclass, regprocedure)
RETURNS typed_object_classes AS $$
	SELECT tag_type_class($1, $2, $3);
	SELECT tag_output_method($1, $4)
$$ LANGUAGE sql;
*/

CREATE OR REPLACE FUNCTION tag_op_method_(
	ref_tags, regprocedure, regprocedure, or_bust bool
) RETURNS typed_object_methods AS $$
	DECLARE
		row typed_object_methods%ROWTYPE;
		op regprocedure := declare_proc($2);
		method regprocedure := declare_proc($3);
		kilroy_was_here boolean := false;
		this regprocedure := 'tag_op_method_(
			ref_tags, regprocedure, regprocedure, bool
		)';
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_methods
			WHERE tag_ = $1 AND operation_ = $2;
			IF FOUND THEN
				IF row.method_ = $3 THEN
					RETURN row;
				ELSEIF or_bust THEN
					RAISE EXCEPTION '%: % % % != % % %',
						this, $1, $2, $3, row.tag_, row.operation_, row.method_;
				ELSE
					RETURN NULL;
				END IF;
			END IF;
			IF kilroy_was_here THEN
				RAISE EXCEPTION '% looping with % % %', this, $1, $2, $3;
			END IF;
			kilroy_was_here := true;
			BEGIN
				INSERT INTO typed_object_methods(tag_, operation_, method_)
				VALUES($1, $2, $3);
			EXCEPTION
				WHEN unique_violation THEN	-- another thread?
					RAISE NOTICE '% % % % raised %!',
						this, $1, $2, $3, 'unique_violation';
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION tag_op_method_(
	ref_tags,regprocedure,regprocedure, bool
) IS
'Registers typed_object_method for given operator or bust';

-- ** Same stuff but without tags!

CREATE OR REPLACE
FUNCTION type_class_get_meta_exists_func_(regtype, regclass)
RETURNS meta_funcs AS $$
	SELECT meta_sql_func(
		_name := $2::text || '_' || type_class_tag($1, $2) || '_exists',
		_args := ARRAY[meta_arg($1, '_ref')],
		_returns := 'boolean',
		_strict := 'meta__strict',	-- ???
		_body :=
			'SELECT EXISTS( SELECT ref FROM ' || $2::text
			|| E'\nWHERE $1 = ref )',
		_ :=
			'referential integrity test method for row of ' || $2::text
	)
$$ LANGUAGE sql;
COMMENT ON
FUNCTION type_class_get_meta_exists_func_(regtype, regclass)
IS 'creates referential integrity meta_func method';

CREATE OR REPLACE
FUNCTION create_exists_method(regtype, regclass)
RETURNS typed_object_methods AS $$
	SELECT tag_op_method_(_tag, _op, (
		SELECT COALESCE(
			( SELECT method_ FROM typed_object_methods
				WHERE operation_ = _op AND tag_ = _tag
			),
			create_func( type_class_get_meta_exists_func_($1, $2) )
		)
	), true ) FROM
		CAST('ref_exists_op(refs)' AS regprocedure) _op,
		type_class_tag($1, $2) _tag
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION declare_ref_type_class(
	regtype, regclass,
	regprocedure='ref_textin(text)',
	regprocedure='ref_textout(refs)',
	ref_tags = NULL
) RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes%ROWTYPE;
		kilroy_was_here boolean := false;
		this regprocedure := 'declare_ref_type_class(
			regtype, regclass, regprocedure, regprocedure, ref_tags
		)';
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes
			WHERE type_ = $1 AND class_ = $2;
			IF FOUND THEN
				IF $3 IS NOT DISTINCT FROM row.in_
				AND $4 IS NOT DISTINCT FROM row.out_
				THEN RETURN row;
				ELSE RAISE EXCEPTION '% % % ! % % % % %',
					this, $3, $4,
					row.tag_, row.type_, row.class_, row.in_, row.out_;
				END IF;
			END IF;
			IF kilroy_was_here THEN
				RAISE EXCEPTION '% looping with % % % % %',
				this, $1, $2, $3, $4, $5;
			END IF;
			kilroy_was_here := true;
			BEGIN
				INSERT INTO typed_object_classes(type_,class_, in_, out_, tag_)
				VALUES(
					$1, $2, $3, $4,
				COALESCE($5, nextval('typed_object_classes_tag_seq')::ref_tags)
				);
			EXCEPTION
				WHEN unique_violation THEN	-- another thread?
					RAISE NOTICE '% % % % % % raised %!',
					this, $1, $2, $3, $4, $5, 'unique_violation';
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION
declare_ref_type_class(regtype, regclass, regprocedure, regprocedure, ref_tags)
IS 'Registers typed_object_classes and io methods';

/*
CREATE OR REPLACE
FUNCTION declare_ref_type_class(regtype, regclass) RETURNS ref_tags AS $$
	DECLARE
		_tag ref_tags;
	BEGIN
		LOOP
			SELECT tag_ INTO _tag FROM typed_object_classes
			WHERE type_ = $1 AND class_ = $2;
			IF FOUND THEN
				RETURN _tag;
			END IF;
			BEGIN
				INSERT INTO typed_object_classes(type_, class_) VALUES($1, $2);
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION declare_ref_type_class(regtype, regclass) IS
'Registers typed_object_classes';
*/

/*
CREATE OR REPLACE
FUNCTION type_class_in(regtype, regclass, regprocedure)
RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes;
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes
			WHERE type_ = $1 AND class_ = $2;
			IF NOT FOUND THEN
					RAISE EXCEPTION 'type_class_in: % % not found!', $1, $2;
			ELSEIF row.in_ IS NOT NULL THEN
				IF row.in_ = $3 THEN
					RETURN row;
				ELSE
					RAISE EXCEPTION 'type_class_in: % % % != %',
					$1, $2, $3, row.in_;
				END IF;
			END IF;
			BEGIN
				UPDATE typed_object_classes SET in_=$3
			WHERE type_ = $1 AND class_ = $2;
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION type_class_in(regtype, regclass, regprocedure)
IS 'Registers typed_object input method';
*/

CREATE OR REPLACE
FUNCTION type_class_in(regtype, regclass, regprocedure)
RETURNS regprocedure AS $$
	UPDATE typed_object_classes
	SET in_ = $3 WHERE type_= $1 AND class_ = $2
	RETURNING in_
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION type_class_out(regtype, regclass, regprocedure)
RETURNS regprocedure AS $$
	UPDATE typed_object_classes
	SET out_ = $3 WHERE type_= $1 AND class_ = $2
	RETURNING out_
$$ LANGUAGE sql;

/*
CREATE OR REPLACE
FUNCTION type_class_out(regtype, regclass, regprocedure)
RETURNS typed_object_classes AS $$
	DECLARE
		row typed_object_classes;
	BEGIN
		LOOP
			SELECT * INTO row FROM typed_object_classes
			WHERE type_ = $1 AND class_ = $2;
			IF NOT FOUND THEN
					RAISE EXCEPTION 'type_class_out: % % not found!', $1, $2;
			ELSEIF row.out_ IS NOT NULL THEN
				IF row.out_ = $3 THEN
					RETURN row;
				ELSE
					RAISE EXCEPTION 'type_class_out: % % % != %',
					$1, $2, $3, row.out_;
				END IF;
			END IF;
			BEGIN
				UPDATE typed_object_classes SET out_=$3
				WHERE type_ = $1 AND class_ = $2 AND out_ IS NULL;
			EXCEPTION
				WHEN unique_violation THEN
					-- evidence of another thread
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION type_class_out(regtype, regclass, regprocedure)
IS 'Registers typed_object output method';
*/

/*
CREATE OR REPLACE
FUNCTION type_class_io(regtype, regclass, regprocedure, regprocedure)
RETURNS typed_object_classes AS $$
	SELECT type_class_in($1, $2, $3);
	SELECT type_class_out($1, $2, $4)
$$ LANGUAGE sql;
*/

-- ** operation fallbacks

CREATE OR REPLACE
FUNCTION operation_fallback_array(regprocedure)
RETURNS regprocedure[] AS $$
	SELECT CASE WHEN fallback IS NULL THEN '{}'::regprocedure[]
	ELSE fallback || operation_fallback_array(fallback)
	END
	FROM (SELECT fallback_ FROM operations_fallbacks WHERE operation_ = $1) x(fallback)
$$ LANGUAGE sql;
COMMENT ON FUNCTION operation_fallback_array(regprocedure) IS
'Returns the fallbacks for the given operator as an array';

CREATE OR REPLACE
FUNCTION operation_fallbacks(regprocedure)
RETURNS SETOF regprocedure AS $$
	SELECT fallback FROM unnest(operation_fallback_array($1)) fallback
$$ LANGUAGE sql;
COMMENT ON FUNCTION operation_fallbacks(regprocedure) IS
'Returns the fallbacks for the given operator as a set';

CREATE OR REPLACE
FUNCTION tag_op_method_array_(ref_tags, regprocedure, regprocedure)
RETURNS typed_object_methods[] AS $$
	SELECT tag_op_method_($1, $2, $3, true) ||
	ARRAY(
		SELECT tag_op_method_($1, fallback, $3, false)
		FROM operation_fallbacks($2) fallback
	)
$$ LANGUAGE sql;
COMMENT ON FUNCTION tag_op_method_array_(ref_tags,regprocedure,regprocedure)
IS 'Registers typed_object_method for given operator and empty fallbacks,
returning an array of the results';

CREATE OR REPLACE
FUNCTION tag_op_method_(ref_tags, regprocedure, regprocedure)
RETURNS SETOF typed_object_methods AS $$
	SELECT tom.* FROM unnest( tag_op_method_array_($1,$2,$3) ) tom
	WHERE tom IS NOT NULL
$$ LANGUAGE sql;
COMMENT ON FUNCTION tag_op_method_(ref_tags,regprocedure,regprocedure)
IS 'Registers typed_object_method for given operator and empty fallbacks';

CREATE OR REPLACE FUNCTION type_class_op_method(
	regtype, regclass, regprocedure, regprocedure
) RETURNS SETOF typed_object_methods AS $$
	SELECT tag_op_method_(tag, $3, $4, true)
	FROM type_class_tag($1, $2) tag
$$ LANGUAGE sql;
COMMENT ON FUNCTION type_class_op_method(
	regtype, regclass, regprocedure, regprocedure
) IS 'Registers typed_object_method for given operator and empty fallbacks';

CREATE OR REPLACE FUNCTION declare_op_fallback(
	_op regprocedure, _fallback regprocedure
) RETURNS operations_fallbacks AS $$
	DECLARE
		row_ RECORD;
		kilroy_was_here boolean := false;
		this regprocedure := 'declare_op_fallback(
			regprocedure, regprocedure
		)';
	BEGIN
		LOOP
			SELECT * INTO row_ FROM operations_fallbacks
			WHERE operation_ = $1;
			IF FOUND THEN
				IF row_.fallback_ = $2 THEN
					RETURN row_;
				ELSE
					RAISE EXCEPTION '%: % % != % %',
					this, $1, $2, row_.operation_, row_.fallback_;
				END IF;
			END IF;
			IF kilroy_was_here THEN
				RAISE EXCEPTION '% looping with % %', this, $1, $2;
			END IF;
			BEGIN
				INSERT INTO operations_fallbacks(operation_, fallback_)
				VALUES(declare_proc($1), declare_proc($2));
			EXCEPTION
				WHEN unique_violation THEN	-- another thread?
					RAISE NOTICE '% % % raised %!', this, $1, $2, 'unique_violation';
			END;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
COMMENT ON
FUNCTION declare_op_fallback(regprocedure, regprocedure)
IS 'Registers operation fallbacks';

SELECT declare_type('ref_tags');

SELECT declare_ref_type_class(
	'refs', 'ref_keys',
	'ref_textin(text)', 'ref_textout(refs)',
	ref_nil_tag()
);

SELECT type_class_op_method(
	'refs', 'ref_keys', 'ref_text_op(refs)', 'ref_textout(refs)'
);
