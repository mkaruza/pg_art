/*-------------------------------------------------------------------------
 *
 * art.c
 *		ART index.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "utils/guc.h"

#include "art.h"

PG_MODULE_MAGIC;

/* GUC variables */
double page_leaf_insert_treshold = 0.8f;
bool update_parent_iptr = true;
int build_max_memory = 4000U;

void
_PG_init(void)
{

	DefineCustomBoolVariable("art.update_parent_iptr",
							 "Keep parent node pointer updated",
							 NULL,
							 &update_parent_iptr,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("art.page_leaf_insert_treshold",
							 "Sets the leaf insert page treshold",
							 "Valid range is 0.0 .. 1.0.",
							 &page_leaf_insert_treshold,
							 0.8,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomIntVariable("art.build_max_memory",
							"Memory limit for index build",
							NULL,
							&build_max_memory,
							4000U,
							4U,
							32000U,
							PGC_USERSET,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);
}


/*
 * Make ART tuple from values.
 */

ArtTuple *
_art_form_key(Relation index, ItemPointer iptr, Datum * values, bool * isnull)
{
	ArtTuple *res = (ArtTuple *) palloc0(sizeof(ArtTuple));

	if (iptr)
		res->iptr = *iptr;

	// Index is defined to use only single column
	for (int i = 0; i < index->rd_att->natts; i++)
	{
		if (!isnull[i])
		{
			FormData_pg_attribute * indexTupleAttr = TupleDescAttr(index->rd_att, i);
			if (indexTupleAttr->attlen == -1)
			{
				Datum datum  = PointerGetDatum(PG_DETOAST_DATUM(values[i]));

				res->key_len = VARSIZE_ANY_EXHDR(datum);
				res->key = palloc0(sizeof(uint8_t) * (res->key_len + 1));
				memcpy(res->key, &(*(uint8_t *) VARDATA_ANY(datum)), res->key_len);
				res->key_len++;

				if (VARATT_IS_EXTENDED(values[0]))
					pfree((void*) datum);
			}
			else
			{
				res->key_len = indexTupleAttr->attlen;
				res->key = palloc0(sizeof(uint8_t) * (res->key_len));
				for(int j = res->key_len - 1, k = 0; j > -1; j--, k++)
				{
					res->key[k] = ((uint8_t *) values + i)[j];
				}
			}
		}
	}

	return res;
}


int32
_art_compare_key(uint8 a, uint8 b)
{
	return (int32) ((uint8) a) - (int32) ((uint8) b);
}


bytea *
artoptions(Datum reloptions, bool validate)
{
  return NULL;
}


PG_FUNCTION_INFO_V1(arthandler);
Datum
arthandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 1;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = artbuild;
	amroutine->ambuildempty = artbuildempty;
	amroutine->aminsert = artinsert;
	amroutine->ambulkdelete = artbulkdelete;
	amroutine->amvacuumcleanup = artvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = artcostestimate;
	amroutine->amoptions = artoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = artvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = artbeginscan;
	amroutine->amrescan = artrescan;
	amroutine->amgettuple = artgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = artendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
