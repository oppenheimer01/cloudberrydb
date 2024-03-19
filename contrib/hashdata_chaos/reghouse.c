#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "catalog/gp_warehouse.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/syscache.h"

typedef Oid reghouse;

PG_FUNCTION_INFO_V1(reghousein);
PG_FUNCTION_INFO_V1(reghouseout);
PG_FUNCTION_INFO_V1(reghousesend);
PG_FUNCTION_INFO_V1(reghouserecv);

Datum
reghouserecv(PG_FUNCTION_ARGS)
{
	/* Exactly the same as oidrecv, so share code */
	return oidrecv(fcinfo);
}

Datum
reghousesend(PG_FUNCTION_ARGS)
{
	/* Exactly the same as oidsend, so share code */
	return oidsend(fcinfo);
}

Datum
reghousein(PG_FUNCTION_ARGS)
{
	char	   *house_name_or_oid = PG_GETARG_CSTRING(0);
	reghouse    result = InvalidOid;
    HeapTuple   tuple;

	/* '-' ? */
	if (strcmp(house_name_or_oid, "-") == 0)
		PG_RETURN_OID(InvalidOid);

	/* Numeric OID? */
	if (house_name_or_oid[0] >= '0' &&
		house_name_or_oid[0] <= '9' &&
		strspn(house_name_or_oid, "0123456789") == strlen(house_name_or_oid))
	{
		result = DatumGetObjectId(DirectFunctionCall1(oidin,
													  CStringGetDatum(house_name_or_oid)));
		PG_RETURN_OID(result);
	}

	/* The rest of this wouldn't work in bootstrap mode */
	if (IsBootstrapProcessingMode())
		elog(ERROR, "reghouse values must be OIDs in bootstrap mode");


    tuple = SearchSysCache1(GPWAREHOUSENAME, 
                            PointerGetDatum(cstring_to_text(house_name_or_oid)));

    if (HeapTupleIsValid(tuple))
    {
        Form_gp_warehouse form = (Form_gp_warehouse) GETSTRUCT(tuple);
        result = form->oid;
    }

    ReleaseSysCache(tuple);

    PG_RETURN_OID(result);
}

Datum
reghouseout(PG_FUNCTION_ARGS)
{
    reghouse    houseoid = PG_GETARG_OID(0);
    HeapTuple   tuple;
    char        *result;
    if (houseoid == InvalidOid)
    {
        result = pstrdup("-");
        PG_RETURN_CSTRING(result);
    }

    tuple = SearchSysCache1(GPWAREHOUSEOID, ObjectIdGetDatum(houseoid));
    if (HeapTupleIsValid(tuple))
    {
        Form_gp_warehouse form = (Form_gp_warehouse) GETSTRUCT(tuple);
        result = text_to_cstring(&form->warehouse_name);

        ReleaseSysCache(tuple);
    }
    else
    {
        /* If OID doesn't match any pg_proc entry, return it numerically */
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", houseoid);
    }

    PG_RETURN_CSTRING(result);
}
