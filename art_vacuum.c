/*-------------------------------------------------------------------------
 *
 * artvacuum.c
 *		ART VACUUM functions.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "art.h"


IndexBulkDeleteResult *
artbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			  IndexBulkDeleteCallback callback, void *callback_state)
{
	return NULL;
}


IndexBulkDeleteResult *
artvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return NULL;
}
