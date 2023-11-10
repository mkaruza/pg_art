-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION art" to load this file. \quit

CREATE FUNCTION arthandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD art TYPE INDEX HANDLER arthandler;
COMMENT ON ACCESS METHOD art IS 'art index access method';

-- operators are defined based on btree strategy numbers

CREATE OPERATOR CLASS _art_int4_ops
DEFAULT FOR TYPE int4 USING art
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
STORAGE int4;

CREATE OPERATOR CLASS _art_int8_ops
DEFAULT FOR TYPE int8 USING art
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
STORAGE int8;

CREATE OPERATOR CLASS _art_date_ops
DEFAULT FOR TYPE date USING art
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
STORAGE date;


CREATE OPERATOR CLASS _art_text_ops
DEFAULT FOR TYPE text USING art
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >,
    OPERATOR        5       >=,
    FUNCTION        1       bttextcmp(text,text),
STORAGE text;