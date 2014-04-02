-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-naming-code.sql', '$Id');

--	Refs and Rows Naming Conventions Support

-- ** Copyright

--	Copyright (c) 2011, J. Greg Davidson.
--	This code may be freely used by CreditLink Corporation
--	for their internal business needs but not redistributed
--	to third parties.

-- * support functions

-- a reference type (unique type of single primary key)
-- type name = <stub>_refs
-- a class with a single primary key of such a reference type
-- class name = <stub>_rows

CREATE OR REPLACE FUNCTION trial_infer_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL, _prefix text = '', _suffix text = ''
)  RETURNS text AS $$
	SELECT COALESCE(
		$4,
		$5 || COALESCE(							-- infer the stub
			$1,
			regexp_replace($2::text, '_refs$', ''),
			regexp_replace($3::text, '_rows$', '')
		) || $6
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION infer_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL, _prefix text = '', _suffix text = ''
) RETURNS text AS $$
	SELECT non_null(
		trial_infer_name($1,$2,$3,$4,$5,$6),
		'infer_name(text,regtype,regclass,text,text,text)',
		'stub:'||$1, 'type:'||$2, 'class:'||$3, 'prefix:'||$5, 'suffix:'||$6, 'name:'||$4
	)
$$ LANGUAGE sql;

CREATE OR REPLACE
FUNCTION infer_ref_type(regclass) RETURNS regtype AS $$
	SELECT table_column_type($1, 'ref')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION trial_infer_typename(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
)  RETURNS text AS $$
	SELECT COALESCE(
		$4, $2::text, infer_ref_type($3)::text,
		infer_name($1, $2, $3) || '_refs'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION infer_typename(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT non_null(
		trial_infer_typename($1,$2,$3,$4),
		'infer_typename(text,regtype,regclass,text)',
		'stub:'||$1, 'type:'||$2, 'class:'||$3
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION trial_infer_type(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL
)  RETURNS regtype AS $$
	SELECT COALESCE(
		$2, infer_ref_type($3), infer_typename($1, $2, $3)::regtype
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION infer_type(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL
) RETURNS regtype AS $$
	SELECT non_null(
		trial_infer_type($1,$2,$3),
		'infer_type(text,regtype,regclass)'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION trial_infer_classname(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
)  RETURNS text AS $$
	SELECT COALESCE(
		$4, $3::text, infer_name($1, $2, $3) || '_rows'
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION infer_classname(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT non_null(
		trial_infer_classname($1,$2,$3,$4),
		'infer_classname(text,regtype,regclass,text)',
		'stub:'||$1, 'type:'||$2, 'class:'||$3
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION trial_infer_class(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL
)  RETURNS regclass AS $$
	SELECT COALESCE(
		$3, infer_classname($1, $2, $3)::regclass
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION infer_class(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL
) RETURNS regclass AS $$
	SELECT non_null(
		trial_infer_class($1,$2,$3),
		'infer_class(text,regtype,regclass)'
	)
$$ LANGUAGE sql;

-- * standard datatype-support function names

CREATE OR REPLACE FUNCTION unchecked_ref_from_id_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, 'unchecked_', '_from_id')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION nil_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4,'','_nil')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION next_ref_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, 'next_',  '_ref')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION prefix_try_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4,'try_')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION text_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, '',  '_text')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION length_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, '', '_length')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION find_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, 'find_');
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION try_find_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT COALESCE(
		$4,
		prefix_try_func_name_text( find_func_name($1,$2,$3) )
	)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION in_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1, $2, $3, $4, 'try_', '_in' )
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1, $2, $3, $4, 'get_')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION declare_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1, $2, $3, $4, 'declare_')
$$ LANGUAGE sql;

-- * wrappers for environment lookup

CREATE OR REPLACE FUNCTION env_value_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, 'env_',  '_value')
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION env_obj_value_func_name(
	_stub text=NULL, _type regtype=NULL, _class regclass=NULL,
	_name text=NULL
) RETURNS text AS $$
	SELECT infer_name($1,$2,$3,$4, 'env_obj_',  '_value')
$$ LANGUAGE sql;
