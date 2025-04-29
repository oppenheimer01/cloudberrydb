/* contrib/hashdata_chaos/hashdata_chaos--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hashdata_chaos" to load this file. \quit

CREATE FUNCTION hashdata_chaos_warehouse_begin(float8)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

CREATE FUNCTION hashdata_chaos_warehouse_end()
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

CREATE TYPE reghouse;

CREATE FUNCTION reghousein(cstring)
RETURNS reghouse
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION reghouseout(reghouse)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION reghouserecv(internal)
RETURNS reghouse
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION reghousesend(reghouse)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE reghouse(
	INPUT = reghousein,
	OUTPUT = reghouseout,
	RECEIVE = reghouserecv,
	SEND = reghousesend,
	ALIGNMENT = int4,
	INTERNALLENGTH = 4,
	PASSEDBYVALUE
)
