-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-create.sql', '$Id');

--	Wicci Project
--	Generate boilerplate to make a ref class consisting of
-- (1) the type, (2) the table, (3) some casts, (4) key functions

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- * Simple type class boilerplate

-- A simple type class is a type class where:
-- The ref type name is of the form <base_name>_refs
-- The ref class (table) name is of the form <base_name>_rows
-- That type is associated only with that class and vice versa.
-- The primary key of that class is named ref and is of that ref type.

-- ** declare_ref_class_with_funcs

CREATE OR REPLACE FUNCTION declare_ref_class(
	regclass=NULL,
	_updateable_ boolean=false, _exists_ boolean=true,
 OUT _toc typed_object_classes
)  AS $$
DECLARE
	_type regtype := infer_type(_class := $1);
	_class regclass := infer_class(_class := $1);
	this regprocedure := 'declare_ref_class(regclass, boolean, boolean)';
BEGIN
	_toc := declare_ref_type_class( _type, _class );
	IF _toc IS NULL THEN
		RAISE EXCEPTION '% declaring failure % %', this, _type, class;
	END IF;
	IF _exists_ THEN
		IF create_exists_method(_type, _class) IS NULL THEN
			RAISE EXCEPTION '% exists failure % %', this, _type, class;
		END IF;
	END IF;
	IF NOT _updateable_ THEN
		IF declare_monotonic(_class) IS NULL THEN
			RAISE EXCEPTION '% monotonic failure % %', this, _type, class;
		END IF;
	END IF;
	RETURN;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_unchecked_ref_from_id_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := unchecked_ref_from_id_func_name($1,$2,$3,$4),
		_body :=
			E'\tSELECT unchecked_ref(\n\t\t'
			|| quote_literal( _type::text )
			|| ', '
			|| quote_literal( _class::text )
			|| E', $1\n\t)::'
			|| _type,
		_args := ARRAY[meta_arg('ref_ids')],
		_returns := _type
		) FROM
			infer_type($1, $2, $3) _type,
			infer_class($1, $2, $3) _class
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_const_ref_func(
	_type regtype, _suffix text, _id ref_ids
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := infer_name(_type := $1, _suffix := $2),
		_body := E'\tSELECT ' ||
			unchecked_ref_from_id_func_name(_type := $1) || '(' || $3 || ')',
		_returns := $1
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_nil_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := nil_func_name($1, $2, $3, $4),
		_body := E'\tSELECT ' ||
			unchecked_ref_from_id_func_name($1, $2, $3) || '(0)',
		_returns := infer_type($1, $2, $3)
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION try_create_ref_downcast_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
)  RETURNS regprocedure AS $$
	SELECT create_func(
		_name := prefix_try_func_name($1,$2,$3,$4),
		_args := ARRAY[meta_arg('refs', '_ref')],
		_returns := _type,
		_strict := 'meta__strict',
		_body := 'SELECT ref FROM ' || _class || ' WHERE ref::refs = $1',
		_by := 'try_create_ref_downcast_func(
			text, regtype, regclass, text
		)'
	) FROM
		infer_type($1,$2,$3) _type,
		infer_class($1,$2,$3) _class
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_ref_downcast_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT non_null(
		try_create_ref_downcast_func($1,$2,$3,$4),
		'create_ref_downcast_func(text,regtype,regclass,text)'
	)
$$ LANGUAGE sql;

COMMENT ON
FUNCTION create_ref_downcast_func(
	text, regtype,  regclass, text
) IS
'Creates a function which will try to downcast the given reference
to a reference to the name_ref class if it exists.';

-- these next two could evolve into a nice omnibus function

CREATE OR REPLACE FUNCTION create_simple_ref_funcs(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL
) RETURNS regprocedure[] AS $$
	SELECT ARRAY[
		create_unchecked_ref_from_id_func($1, $2, $3),
		create_nil_func($1, $2, $3),
		create_ref_downcast_func($1,$2,$3)
	]
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION declare_ref_class_with_funcs(
	class regclass, _updateable_ boolean=false, _exists_ boolean=true
) RETURNS regprocedure[] AS $$
	SELECT declare_ref_class($1, _updateable_ := $2, _exists_ := $3);
	SELECT create_simple_ref_funcs(_class := $1 )
$$ LANGUAGE sql;

-- ** create_simple_serial

CREATE OR REPLACE FUNCTION simple_sequence_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, '','_seq')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_simple_next_ref_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := next_ref_func_name($1,$2,$3,$4),
		_body :=
			'SELECT ' || unchecked_ref_from_id_func_name($1,$2,$3) || '('
|| 'nextval(' || quote_literal( simple_sequence_name($1,$2,$3) ) || ')'
			|| '::ref_ids' || ')',
		_returns := infer_type($1, $2, $3)
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION alter_simple_column_default_text(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_column text='ref'
) RETURNS text AS $$
	SELECT alter_column_default_text(
		infer_class($1,$2,$3),
		$4,
		next_ref_func_name($1,$2,$3) || '()'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_simple_serial(
	regclass, _column text='ref',
	_min bigint=1, _max bigint=:RefIdMax
) RETURNS regprocedure AS $$
DECLARE
	_seq_name text := simple_sequence_name(_class := $1);
	_next_proc regprocedure;
	sequence_result boolean;
	alter_result boolean;
	this regprocedure :=
		'create_simple_serial(regclass,text,bigint,bigint)';
BEGIN
	IF $1 IS NULL THEN
		RAISE EXCEPTION '% _class is NULL!', this;
	END IF;
	IF _seq_name IS NULL THEN
		RAISE EXCEPTION '% sequence name is NULL!', this;
	END IF;
	PERFORM drop_entity('meta__sequence', _seq_name);
	sequence_result := create_sequence(
		_seq_name, _owner := $1, _column := $2,
		_min := $3, _max := $4
	);
	_next_proc := create_simple_next_ref_func(_class := $1);
	alter_result := meta_execute(
		this, alter_simple_column_default_text(_class := $1)
	);
	RETURN _next_proc;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION try_create_ref_uniques_table(
	_stub text=NULL, _type regtype=NULL, _name text=NULL,
	_cols meta_columns[] = '{}', _ text = ''
)  RETURNS regclass AS $$
	SELECT create_table(
		infer_classname($1, $2, NULL, $3),
		ref || $4,
		_primary := meta_cols_primary_key(ref),
		_uniques := ARRAY[ meta_cols_unique_key($4) ],
		_ := $5,
		_by := 'try_create_ref_uniques_table(
			text,regtype,text,meta_columns[],text
		)'
	) FROM COALESCE(ARRAY[
		meta_column( 'ref', infer_type($1,$2) )
	]) ref
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_ref_uniques_table(
	_stub text=NULL, _type regtype=NULL, _name text=NULL,
	_cols meta_columns[] = '{}', _ text = ''
) RETURNS regclass AS $$
	SELECT non_null(
		try_create_ref_uniques_table($1,$2,$3,$4,$5),
		'create_ref_uniques_table(text,regtype,text,meta_columns[],text)'
	)
$$ LANGUAGE sql;

-- * name reference classes

-- ** name reference type and table

CREATE OR REPLACE FUNCTION try_create_name_ref_table(
	_stub text=NULL, _type regtype=NULL, _name text=NULL,
	name_type regtype = 'text', _ text = ''
) RETURNS regclass AS $$
	SELECT try_create_ref_uniques_table(
			$1, $2, $3,
			ARRAY[ meta_column('name_', $4, _not_null := true) ],
			$5
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_table(
	_stub text=NULL, _type regtype=NULL, _name text=NULL,
	name_type regtype = 'text', _ text = ''
) RETURNS regclass AS $$
	SELECT non_null(
		try_create_name_ref_table($1, $2, $3, $4, $5),
		'create_name_ref_table(text,regtype,text,regtype,text)'
	)
$$ LANGUAGE sql;

-- ** name reference code

CREATE OR REPLACE FUNCTION create_name_ref_text_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := text_func_name($1,$2,$3,$4),
		_args := ARRAY[ meta_arg(infer_type($1)) ],
		_returns := 'text',
		_strict := 'meta__strict2',
		_body :=
			'SELECT name_ FROM ' || infer_class($1,$2,$3) || ' WHERE ref=$1',
		_ := 'return the text associated with the given reference',
		_by := 'create_name_ref_text_func(text, regtype, regclass, text)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_length_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := length_func_name($1,$2,$3,$4),
		_args := ARRAY[ meta_arg(infer_type($1)) ],
		_returns := 'integer',
		_strict := 'meta__strict2',
		_body := 'SELECT octet_length(name_) FROM ' || infer_class($1,$2,$3) ||
			' WHERE ref=$1',
		_ := 'return length of text associated with given reference',
		_by := 'create_name_ref_length_func(text, regtype, regclass, text)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION name_ref_find_func_body_(
	regclass, _nil regprocedure, _x text, _when text, _from text
) RETURNS text AS $$
	SELECT CASE WHEN $2 IS NULL THEN select_ref ELSE
		E'SELECT CASE\n' ||
			E'\tWHEN ' || $4 || ' THEN ' || call_proc_text($2) || E'\n' ||
			E'\tELSE ( ' || select_ref  || E' )\n' ||
		'END'
	END ||  $5
	FROM text(
		'SELECT ref FROM ' || $1 || ' WHERE name_ = ' || $3
	) select_ref
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION name_ref_find_func_body(
	regclass, _nil regprocedure = NULL, _norm regprocedure = NULL
) RETURNS text AS $$
	SELECT CASE WHEN $3 IS NULL
		THEN name_ref_find_func_body_(
			$1, $2, '$1', '$1 = ''''', ''
		)
		ELSE name_ref_find_func_body_(
			$1, $2, 'x', 'x IS NULL', ' FROM ' || call_proc_text($3, '$1') || ' AS x'
		)
	END
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_find_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL,
	_norm regprocedure = NULL, _nil regprocedure = NULL,
	name_type regtype = 'text'
) RETURNS regprocedure AS $$
	SELECT create_func(
		_name := find_func_name($1,$2,$3,$4),
		_args := ARRAY[meta_arg($7)],
		_returns := infer_type($1,$2,$3),
		_strict := 'meta__strict2',
		_body := name_ref_find_func_body(
			infer_class($1,$2,$3), $6, $5
		),
		_ := 'find reference to existing row',
		_by :='create_name_ref_find_func(
			text, regtype, regclass, text,regprocedure,regprocedure,regtype
		)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION try_create_name_ref_in_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL, _try regprocedure = NULL,
	name_type regtype = 'text'
)  RETURNS regprocedure AS $$
	SELECT create_func(
		_name := in_func_name($1,$2,$3,$4),
		_args := ARRAY[meta_arg($6)],
		_returns := infer_type($1),
		_strict := 'meta__non_strict',
		_body := 'SELECT COALESCE(' ||
			call_proc_text($5, '$1') || ', ref_textin($1::text)' ||
		' )',
		_ := 'try to parse input text as reference to existing row',
		_by:='try_create_name_ref_in_func(
			text,regtype, regclass,text, regprocedure,regtype
		)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_in_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL, _try regprocedure = NULL,
	name_type regtype = 'text'
) RETURNS regprocedure AS $$
	SELECT non_null(
		try_create_name_ref_in_func($1,$2,$3,$4,$5,$6),
		'create_name_ref_in_func(
			text,regtype,regclass,text,regprocedure,regtype
		)'
	)
$$ LANGUAGE sql;

/*
The normalization function must have the following
characteristics:
(1) on an illegal value, it throws an exception
(2) on a value which should map to nil, it returns NULL
(3) on all legal values, it returns a normalized text value
*/

CREATE OR REPLACE FUNCTION name_ref_get_name_body(
	_class regclass,
	_norm regprocedure = NULL,
	_nil regprocedure = NULL,
	name_type regtype = 'text'
) RETURNS text AS $_$
	SELECT $$DECLARE
	_name $$ || $4 || $$ := $$|| CASE
		WHEN $2 IS NULL THEN '$1' ELSE call_proc_text($2, '$1')
	END ||$$;
	maybe $$|| infer_type(_class := $1) ||$$;
	kilroy_was_here boolean := false;
	this regprocedure := '-->this-proc-sig<--';
BEGIN
	$$ || CASE WHEN $3 IS NULL THEN ''
	ELSE
		$$ IF _name IS NULL THEN RETURN $$|| $3::regproc ||$$(); END IF;
	$$ END || $$
	LOOP
		maybe := $$|| try_find_func_name(_class:=$1) ||$$(_name);
		IF maybe IS NOT NULL THEN RETURN maybe; END IF;
		IF kilroy_was_here THEN
			RAISE EXCEPTION '% looping with %', this, $1;
		END IF;
		kilroy_was_here := true;
		BEGIN
			INSERT INTO $$|| $1 ||$$(name_) VALUES (_name);
		EXCEPTION
			WHEN unique_violation THEN			-- another thread??
				RAISE NOTICE '% % raised %!', this, $1, 'unique_violation';
		END;	
	END LOOP;
END$$
$_$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION try_create_name_ref_get_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL,
	_norm regprocedure = NULL, _nil regprocedure = NULL,
	name_type regtype = 'text'
)  RETURNS regprocedure AS $$
	SELECT create_func(
		_name := get_func_name($1, $2, $3, $4),
		_lang := 'meta__plpgsql',
		_args := ARRAY[meta_arg($7)],
		_returns := infer_type($1,$2,$3),
		_strict := 'meta__strict2',
		_body:=name_ref_get_name_body(infer_class($1,$2,$3),$5,$6,$7),
		_ := 'find or create row whose name = the normalized value',
		_by := 'try_create_name_ref_get_func(
			text, regtype, regclass, text, regprocedure, regprocedure,regtype
		)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_get_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL,
	_norm regprocedure = NULL, _nil regprocedure = NULL,
	name_type regtype = 'text'
) RETURNS regprocedure AS $$
	SELECT non_null(
		try_create_name_ref_get_func($1,$2,$3,$4,$5,$6,$7),
		'create_name_ref_get_func(
			text,regtype,regclass,text,regprocedure,regprocedure,regtype
		)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION create_name_ref_declare_func(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL, _get regprocedure=NULL,
	name_type regtype = 'text'
) RETURNS regprocedure AS $_$
	SELECT create_func(
		_name := _name,
		_args := ARRAY[meta_arg(array_regtype($6),_variadic := true)],
		_returns := 'integer',
		_strict := 'meta__non_strict',
		_body := $$SELECT array_length( ARRAY(
				SELECT $$||
					COALESCE(regproc_name($5), get_func_name($1,$2,$3))
				||$$(x) FROM unnest($1) x
			) )
		$$,
		_ := 'ensure rows exist with these values',
		_by := 'create_name_ref_declare_func(
				text, regtype, regclass, text, regprocedure,regtype
		)'
	) FROM
		infer_type($1, $2, $3) _type,
		infer_class($1, $2, $3) _class,
		declare_func_name($1, $2, $3, $4) _name
$_$ LANGUAGE sql;

-- * create_name_ref omnibus function

CREATE OR REPLACE FUNCTION create_name_ref_schema(
	_stub text,										-- base for creating names of things
	_nil_ boolean = true,					-- create nil value?
	_serial_ boolean = true,			-- create sequence?
	name_type regtype = 'text',
	_norm regprocedure = NULL,
	_ text = ''										-- comment
) RETURNS regprocedure[] AS $$
DECLARE
	this regprocedure :=
		'create_name_ref_schema(
			text, boolean, boolean,
			regtype,regprocedure,
			text
	)';
	_procs regprocedure[] := '{}'; -- what we return
	_unchecked regprocedure;			-- needed by nil et al
	_nil regprocedure;
	insert_result boolean;
	_type regtype := create_ref_type(infer_typename(_stub));
	_class regclass := create_name_ref_table(
		_stub, _type, NULL, name_type, _
	);
	_toc typed_object_classes;
	_tom typed_object_methods;
BEGIN
	-- default value for row or record variable is not supported
	_toc := declare_ref_type_class( _type, _class );
	_tom := create_exists_method(_type, _class);
	_unchecked := create_unchecked_ref_from_id_func(_stub);
	_procs := _procs || _unchecked;
	IF _nil_ THEN
		_nil := create_nil_func(_stub);
		_procs := _procs || _nil;
		insert_result := meta_execute(this,
			'INSERT INTO', _class::text, '(ref, name_)',
			'VALUES(', call_proc_text(_nil) || ',', ''''')'
		);
	END IF;
	DECLARE
		_class2 regclass := declare_monotonic(_class);
		_downcast regprocedure := create_ref_downcast_func(_stub);
		_text regprocedure := create_name_ref_text_func(_stub);
		_length regprocedure := create_name_ref_length_func(_stub);
		_find regprocedure := create_name_ref_find_func(
			_stub, _norm := _norm, _nil := _nil, name_type := name_type
		);
		_in regprocedure := create_name_ref_in_func(
			_stub,
			_try := prefix_try_func_name_text(_find::text)::regprocedure,
			name_type := name_type
		);
		_toc_in regprocedure := type_class_in(_type, _class, _in);
		_toc_out regprocedure := type_class_out(_type, _class, _text);
		_next regprocedure;
		_get regprocedure;
		_declare regprocedure;
	BEGIN
		_procs := _procs || _downcast || _text || _length || _find || _in;
		IF (_class <> _class2) THEN
			RAISE EXCEPTION '% class % <> %', this, _class, _class2;
		END IF;
		PERFORM type_class_op_method(
			_type,_class,'ref_text_op(refs)',_text
		);
		PERFORM type_class_op_method(
				_type,_class,'ref_length_op(refs)',_length
		);
		IF _serial_ THEN
			_next := create_simple_serial(_class);
			_get := create_name_ref_get_func(
				_stub, _norm := _norm, _nil := _nil, name_type := name_type
			);
			_declare := create_name_ref_declare_func(
				_stub, _get := _get, name_type := name_type
			);
			_procs := _procs || _next || _get || _declare;
		END IF;
		RETURN _procs;
	END;
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_name_ref_schema(
	_stub text, _nil_ boolean, _serial_ boolean,
	name_type regtype, _norm regprocedure, _ text
) IS 'Create type, table, casts, sequences, rows, procedures and
bindings to implement an abstract collection of unique names.';
