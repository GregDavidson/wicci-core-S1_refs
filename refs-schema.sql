-- * Header  -*-Mode: sql;-*-
\ir settings.sql
SELECT set_file('refs-schema.sql', '$Id');

-- Where should I do this?  Or what else should I do?
-- \i /usr/local/SW/pgsql/share/contrib/pgxml.sql

--	Wicci Project
--	Tagged Typed Object Reference Schema

-- ** Copyright

--	Copyright (c) 2005-2012, J. Greg Davidson.
--	You may use this file under the terms of the
--	GNU AFFERO GENERAL PUBLIC LICENSE 3.0
--	as specified in the file LICENSE.md included with this distribution.
--	All other use requires my permission in writing.

-- See Notes/refs.txt for background

-- ** Requires

--Dependencies: modules.sql

-- * Provides

-- ** TABLE typed_object_classes (tag_, class_, type_)

CREATE TABLE IF NOT EXISTS typed_object_classes (
	tag_ ref_tags PRIMARY KEY,
	class_ regclass NOT NULL,
	type_ regtype NOT NULL REFERENCES our_types ON DELETE CASCADE,
	UNIQUE(class_, type_),
	out_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	in_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	is_default boolean NOT NULL DEFAULT true
);
COMMENT ON TABLE typed_object_classes IS
'Provides the ref tag for the class and the
method dictionary.  Specific types of ref classes
may inherit from this table and add their methods.
This is now a base table.  Derived tables inheriting
from it are in various dependent schemas so that when
those schemas go away, their entries go away too!';
COMMENT ON COLUMN typed_object_classes.class_ IS
'Relation which holds instances of this class.';
COMMENT ON COLUMN typed_object_classes.type_ IS
'Reference type for this class';
COMMENT ON COLUMN typed_object_classes.out_ IS
'($type_) -> text ; used by $type_ output function; make required???';
COMMENT ON COLUMN typed_object_classes.in_ IS
'(text) -> $type_ ; used by $type_ input function, if exists';
COMMENT ON COLUMN typed_object_classes.is_default IS
'a default tag depends only on its table;
there is exactly one default tag per table;
-- we do not yet have functions to manage this!!!';

-- *** tag sequence

DROP SEQUENCE IF EXISTS typed_object_classes_tag_seq CASCADE;

CREATE SEQUENCE typed_object_classes_tag_seq
	OWNED BY typed_object_classes.tag_
	MINVALUE 1 MAXVALUE :RefTagMax CYCLE;

/*
ALTER TABLE typed_object_classes ALTER COLUMN tag_
	SET DEFAULT nextval('typed_object_classes_tag_seq')::ref_tags;
*/

-- *** triggers

CREATE OR REPLACE
FUNCTION toc_before_insert() RETURNS trigger AS $$
	DECLARE
		type_oid regtype := declare_type(NEW.type_);
		in_ regprocedure := declare_proc(NEW.in_);
		out_ regprocedure := declare_proc(NEW.out_);
--    table_oid regclass := ensure_table(NEW.class_);
	BEGIN
	-- check signature of in_ and out_ procedures if they are given
		RETURN NEW;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION toc_before_insert() IS
'ensures type exists in our_types';

DROP TRIGGER IF EXISTS toc_before_insert ON typed_object_classes
	CASCADE;

CREATE TRIGGER toc_before_insert
	BEFORE INSERT ON typed_object_classes
	FOR EACH ROW EXECUTE PROCEDURE toc_before_insert();

CREATE OR REPLACE
FUNCTION toc_before_update() RETURNS trigger AS $$
	DECLARE
		type_oid regtype := declare_type(NEW.type_);
		in_ regprocedure := declare_proc(NEW.in_);
		out_ regprocedure := declare_proc(NEW.out_);
--    table_oid regclass := ensure_table(NEW.class_);
	BEGIN
	-- do not allow non-null fields to change??
	-- check signature of in_ and out_ procedures if they are given
		RETURN NEW;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION toc_before_update() IS
'prohibit inappropriate updates';

DROP TRIGGER IF EXISTS toc_before_update ON typed_object_classes
	CASCADE;

CREATE TRIGGER toc_before_update
	BEFORE UPDATE ON typed_object_classes
	FOR EACH ROW EXECUTE PROCEDURE toc_before_update();

-- ** typed_object_methods(tag_, operation_, method_)

CREATE TABLE IF NOT EXISTS typed_object_methods (
	tag_ ref_tags NOT NULL
	REFERENCES typed_object_classes ON DELETE CASCADE,
	operation_ regprocedure NOT NULL
	REFERENCES our_procs ON DELETE CASCADE,
	method_ regprocedure NOT NULL
	REFERENCES our_procs ON DELETE CASCADE,
	PRIMARY KEY(tag_, operation_)
);
COMMENT ON TABLE typed_object_methods IS '
Associates operations with the methods which implement that
operation for a particular type, indicated by its tag.  The first
argument of the operation and of the method must be tors.  The
signatures of both procedures must be compatible, e.g.:<

	(1) method argument types must be contravariant with
	respect to the corresponding operation argument types

	(2) method return types must be covariant with
	respect to the corresponding operation return types

CHANGING:
Our system does NOT handle default arguments, varargs or
returning sets or multiple values.  In some cases these
features can be obtained with SQL wrapper functions.

Before trigger could/should??? ensure type of first parameter of
operation_ and method_ functions is of type_ associated with tag_.
';

CREATE OR REPLACE
FUNCTION ok_ref_proc(regprocedure) RETURNS boolean AS $$
	SELECT EXISTS(
		SELECT toc.tag_ FROM typed_object_classes toc, pg_proc
		WHERE pg_proc.oid = $1 AND pronargs > 0 AND proargtypes[0] = toc.type_
	)
$$ LANGUAGE sql;
COMMENT ON FUNCTION ok_ref_proc(regprocedure) IS
'check that type of first agument is a ref type';

CREATE OR REPLACE
FUNCTION ok_ref_op_meth_returns(regprocedure, regprocedure) RETURNS boolean AS $$
	 SELECT true
$$ LANGUAGE sql;
COMMENT ON FUNCTION ok_ref_op_meth_returns(regprocedure, regprocedure) IS
'Could the type returned by the second method be returned by the op???
This is a research project!!';

CREATE OR REPLACE FUNCTION ok_ref_op_method_args(
	op_oid regprocedure, method_oid regprocedure
) RETURNS boolean AS $$
		SELECT TRUE;
$$ LANGUAGE sql;
COMMENT ON
	FUNCTION ok_ref_op_method_args(regprocedure, regprocedure) IS
'Could the second proc be called with the args of the first proc???
This is a research project!!';

CREATE OR REPLACE
FUNCTION ok_ref_op_meth(regprocedure, regprocedure) RETURNS boolean AS $$
	SELECT ok_ref_proc($1) AND ok_ref_proc($2)
	AND ok_ref_op_meth_returns($1, $2)
	AND ok_ref_op_method_args($1, $2)
$$ LANGUAGE sql;
COMMENT ON FUNCTION ok_ref_op_meth(regprocedure, regprocedure) IS
'Could this ref operation be implemented by this ref method?';

CREATE OR REPLACE
FUNCTION tom_before_insert() RETURNS trigger AS $$
	DECLARE
		op_oid regprocedure := declare_proc(NEW.operation_);
		meth_oid regprocedure := declare_proc(NEW.method_);
		i INTEGER;
	BEGIN
		IF NOT ok_ref_op_meth(op_oid, meth_oid) THEN
			RAISE EXCEPTION 'tom_before_insert: op % incompatible with method %!',
			op_oid, meth_oid;
		END IF;
		RETURN NEW;
	END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION tom_before_insert() IS
'ensures operation and method exist in our_procs and are compatible';

DROP TRIGGER IF EXISTS tom_before_insert ON typed_object_methods
	CASCADE;

CREATE TRIGGER tom_before_insert
	BEFORE INSERT ON typed_object_methods
	FOR EACH ROW EXECUTE PROCEDURE tom_before_insert();

-- ** TABLE operations_fallbacks(operation_, fallback_)

CREATE TABLE IF NOT EXISTS operations_fallbacks (
	operation_ regprocedure PRIMARY KEY
	-- REFERENCES typed_object_methods(operation_)
	REFERENCES our_procs ON DELETE CASCADE,
	fallback_ regprocedure NOT NULL
	REFERENCES our_procs ON DELETE CASCADE
);
COMMENT ON COLUMN operations_fallbacks.fallback_ IS '
	Must have same the same or fewer parameters as the
	associated operator_ - corresponding parameters
	must have the same types and the return types must
	also be the same.
';
COMMENT ON TABLE operations_fallbacks IS '
	When there is no method available for a given operation_
	and class, a fallback for that _operation may be tried.
	When the fallback is not an operator, then it is the
	method to use.  When the fallback IS an operator,
	and it also has no method for this class, IT may have
	a fallback, and so on.
';

-- ** TABLE ref_keys(key)

CREATE TABLE IF NOT EXISTS ref_keys (
	key refs PRIMARY KEY
);
COMMENT ON TABLE ref_keys IS
'Provides referential integrity for tors.  With proper inheritance,
all specialized ref types could simply inherit from this table.
Instead, inheritance relationships must be maintained with triggers.
Monday  4 January 2011: Just use for any generic tors for testing
and for nil tors.';

SELECT create_handles_for('ref_keys');

CREATE TABLE IF NOT EXISTS fancy_classes (
	tag_ ref_tags PRIMARY KEY
		REFERENCES typed_object_classes ON DELETE CASCADE,
	norm_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	next_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	unchecked_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	nil_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	downcast_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	try_text_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	text_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	try_length_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	length_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	try_find_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	find_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	try_get_ regprocedure REFERENCES our_procs ON DELETE CASCADE,
	get_ regprocedure REFERENCES our_procs ON DELETE CASCADE
) INHERITS (typed_object_classes);

COMMENT ON TABLE fancy_classes IS
'Hold much of the procedural API for the typical ref class
in one convenient place.  Need to add checks of the signatures
and return types of each procedure!!! Maybe the naming
conventions as well???.';
