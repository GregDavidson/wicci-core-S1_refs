-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-test.sql', '$Id');

-- SELECT spx_debug_on();
-- SELECT spx_debug_set(2);
-- SELECT refs_debug_on();

-- TABLE tag_class_type_out_in_numops_maxtag_view;
-- TABLE typed_object_classes;
-- TABLE typed_object_methods;
-- TABLE operations_fallbacks;
-- TABLE fancy_classes;

SELECT (
	rettype::regtype::text || ' ' || oid_::regprocedure::text
) AS "our_procs" FROM our_procs;

SELECT test_func(
			 'ref_tag(refs)',
			 ref_tag(unchecked_ref_from_tag_id(100, 5)::refs),
			 100::ref_tags
);

SELECT test_func(
			 'ref_id(refs)',
			 ref_id(unchecked_ref_from_tag_id(100, 5)::refs),
			 5::ref_ids
);

SELECT test_func(
			 'ref_id(refs)',
			 ref_id(unchecked_ref_from_tag_id(100, -5)::refs),
			 (-5)::ref_ids
);

SELECT test_func(
			 'is_nil(refs)',
				is_nil(unchecked_ref_from_tag_id(0, 0)::refs),
	true
);

SELECT test_func(
			 'is_nil(refs)',
				is_nil(unchecked_ref_from_tag_id(0, 5)::refs),
	true
);

/*
-- This should not have ever been a test, since the
-- tag won't work on a 32-bit system --- however, it
-- has revealed a (gasp!) bug!  The Refs system didn't
-- throw an exception on a 32-bit system; instead,
-- it constructed a non-nil ref!!!
SELECT test_func(
			 'is_nil(refs)',
				is_nil(unchecked_ref_from_tag_id(1000, 0)::refs),
	true
);
*/

SELECT test_func(
			 'is_nil(refs)',
				is_nil(unchecked_ref_from_tag_id(100, 0)::refs),
	true
);

SELECT test_func( 
	'unchecked_ref_from_tag_id(ref_tags, ref_ids)',
	ref_tag(unchecked_ref_from_tag_id(100, 5)::refs),
	100::ref_tags
);

SELECT test_func( 
	'unchecked_ref_from_tag_id(ref_tags, ref_ids)',
	ref_id(unchecked_ref_from_tag_id(100, 5)::refs),
	5::ref_ids
);

SELECT test_func( 
	'ref_text_op(refs)',
	ref_text_op( ref_nil() ),
	'refs:ref_keys:0'
);

SELECT test_func( 
	'call_out_method(refs)',
	ref_text_op( ref_nil() ),
	'refs:ref_keys:0'
);

SELECT test_func( 
	'refs_op_tag_to_method(regprocedure, ref_tags)',
	refs_op_tag_to_method( 'ref_text_op(refs)', ref_nil_tag() ),
	'ref_textout(refs)'
);

SELECT test_func( 
			 'refs_tag_to_type(ref_tags)',
			 refs_tag_to_type( ref_nil_tag() ),
			 'refs'
);

SELECT test_func(
			 'refs_type_to_tag(regtype)',
			 refs_type_to_tag( 'refs' ),
			 ref_nil_tag()
);

-- Need more cases for the ref_str functions!!
-- Especially on non-ref-str text!
-- Don't forget optional schema-qualification
-- of both type and class names!

SELECT test_func(
	'ref_str(regclass, ref_ids)',
	ref_str('pg_type'::regclass, 42),
	'pg_type'::text || ref_str_delim() || '42'
);

SELECT test_func(
	'ref_str(regtype, regclass, ref_ids)',
	ref_str('regtype'::regtype, 'pg_type'::regclass, 42),
	'regtype'::regtype::text || ref_str_delim()
	|| 'pg_type'::text || ref_str_delim() || '42'
);

SELECT test_func(
	'ref_str_match(text)',
	ref_str_match('pg_type'::text || ref_str_delim() || '42')
);

SELECT test_func(
	'ref_str_parse(text)',
	_type IS NULL AND _class = 'pg_type'::text AND _id = 42
) FROM ref_str_parse(
	'pg_type'::text || ref_str_delim() || '42'
) AS foo(_type, _class, _id);

SELECT test_func(
	'ref_str_parse(text)',
	_type = 'regtype'::regtype::text
	AND _class = 'pg_type'::text AND _id = 42
) FROM ref_str_parse(
	'regtype'::regtype::text || ref_str_delim()
	|| 'pg_type'::text || ref_str_delim() || '42'
) AS foo(_type, _class, _id);

-- * create_name_ref_schema

SELECT create_name_ref_schema('foo');

-- is equivalent to:

/*
SELECT create_ref_type(infer_typename('foo'));

SELECT create_name_ref_table(
	'foo', 'foo_refs', NULL, 'text', NULL
);

SELECT declare_ref_type_class( 'foo_refs', 'foo_rows' );

SELECT create_exists_method('foo_refs', 'foo_rows');

SELECT create_unchecked_ref_from_id_func('foo');

SELECT create_nil_func('foo');

INSERT INTO foo_rows (ref, foo_) VALUES( foo_nil(), '');

SELECT declare_monotonic('foo_rows');

SELECT create_ref_downcast_func('foo');

SELECT create_name_ref_text_func('foo');

SELECT create_name_ref_length_func('foo');

SELECT create_name_ref_find_func('foo', _nil := 'foo_nil()');

SELECT try_func_foo_text('find_foo(text)');

SELECT create_name_ref_in_func('foo', _try := 'try_foo(text)');

SELECT type_class_in('foo_refs', 'foo_rows', 'try_foo_in(text)');

SELECT type_class_out('foo_refs', 'foo_rows', 'foo_text(foo_refs)');

SELECT type_class_op_method(
 'foo_refs','foo_rows','ref_text_op(refs)', 'foo_text(foo_refs)'
);

SELECT type_class_op_method(
 'foo_refs','foo_rows','ref_length_op(refs)', 'foo_length(foo_refs)'
);

SELECT create_simple_serial('foo_rows'); -- returns foo_next()

SELECT create_name_ref_get_func( 'foo', _nil := 'foo_nil()' );

SELECT create_name_ref_declare_func( 'foo', _get := 'get_foo(text)' );
*/
