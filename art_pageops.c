/*-------------------------------------------------------------------------
 *
 * art_pageops.c
 *		Page-handling routines for ART indexes
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"

#include "art.h"


void
_art_init_data_page(Page page, uint8 flags)
{
	ArtDataPageOpaque opaque;
	
	PageInit(page, BLCKSZ, sizeof(ArtDataPageOpaqueData));

	opaque = (ArtDataPageOpaque) PageGetSpecialPointer(page);

	opaque->page_flags = flags;
	opaque->deleted_item_size = 0;
	opaque->n_deleted = 0;
	opaque->n_total = 0;
	opaque->right_link = InvalidBlockNumber;
}

void
_art_init_metadata_page(Page page)
{
	ArtMetaDataPageOpaque opaque;
	
	PageInit(page, BLCKSZ, sizeof(ArtMetaDataPageOpaqueData));

	opaque = (ArtMetaDataPageOpaque) PageGetSpecialPointer(page);

	opaque->last_internal_node_blk_num = ART_ROOT_NODE_BLKNO;
	opaque->last_leaf_blk_num = ART_LEAF_NODE_BLKNO;
	memset(opaque->page_cache, 0, sizeof(ArtPageCache) * ART_CACHED_PAGES);
}

ArtPageEntry *
_art_get_metadata_page(Relation index)
{
	ArtPageEntry * metadata_page_entry = palloc0(sizeof(ArtPageEntry));

	metadata_page_entry->blk_num = ART_METADATA_NODE_BLKNO;
	metadata_page_entry->buffer = ReadBuffer(index, ART_METADATA_NODE_BLKNO);
	LockBuffer(metadata_page_entry->buffer, BUFFER_LOCK_EXCLUSIVE);
	metadata_page_entry->page = BufferGetPage(metadata_page_entry->buffer);
	metadata_page_entry->ref_count = 1;

	return metadata_page_entry;
}

void
_art_update_metadata_page(Page page, ArtMetaDataPageOpaque metadata)
{
	ArtMetaDataPageOpaque opaque;
	opaque = (ArtMetaDataPageOpaque) PageGetSpecialPointer(page);

	opaque->last_internal_node_blk_num = metadata->last_internal_node_blk_num;
	opaque->last_leaf_blk_num = metadata->last_leaf_blk_num;
	memcpy(opaque->page_cache, metadata->page_cache, sizeof(ArtPageCache) * ART_CACHED_PAGES);
}


void 
_art_page_release(ArtPageEntry * pageEntry)
{
	// Memory build doesn't have buffer information for page entry
	if (pageEntry == NULL || pageEntry->buffer == InvalidBuffer)
		return;

	if (pageEntry->is_copy)
		return;

	if (pageEntry->ref_count == 1)
	{
		if (pageEntry->dirty)
			MarkBufferDirty(pageEntry->buffer);
	
		UnlockReleaseBuffer(pageEntry->buffer);
		dlist_delete(&pageEntry->node);
		pfree(pageEntry);
	}
	else
	{
		pageEntry->ref_count--;
	}
}

ArtPageEntry *
_art_new_page(uint8 flags)
{
	ArtPageEntry * new_page_entry;

	new_page_entry = (ArtPageEntry *) palloc0(sizeof(ArtPageEntry));
	new_page_entry->page = (Page) palloc(BLCKSZ);

	_art_init_data_page(new_page_entry->page, flags);

	new_page_entry->dirty = true;
	new_page_entry->buffer = InvalidBuffer;
	new_page_entry->ref_count = 1;
	new_page_entry->is_copy = false;

	return new_page_entry;
}

ArtPageEntry *
_art_get_buffer(Relation index, uint8 flags)
{
	ArtPageEntry * page_entry = (ArtPageEntry *) palloc0(sizeof(ArtPageEntry));
	bool needLock;

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);

	page_entry->buffer = ReadBuffer(index, P_NEW);

	/* Acquire buffer lock on new page */
	LockBuffer(page_entry->buffer, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	page_entry->blk_num = BufferGetBlockNumber(page_entry->buffer);
	page_entry->page = BufferGetPage(page_entry->buffer);
	page_entry->ref_count = 1;
	page_entry->is_copy = false;

	_art_init_data_page(page_entry->page, flags);

	return page_entry;
}

dlist_node *
_art_load_page(Relation index, dlist_head * pageListHead,
			   BlockNumber blockNum, int bufferLockMode,
			   bool * isNewPageEntry)
{
	dlist_iter iter;
	ArtPageEntry * page_entry = NULL;

	dlist_foreach(iter, pageListHead)
	{
		page_entry = dlist_container(ArtPageEntry, node, iter.cur);
		
		if (page_entry->blk_num == blockNum)
		{
			page_entry->ref_count++;
			*isNewPageEntry = false;
			return iter.cur;
		}
	}
	
	page_entry = (ArtPageEntry *) palloc0(sizeof(ArtPageEntry));
	
	page_entry->blk_num = blockNum;
	page_entry->buffer = ReadBuffer(index, blockNum);

	LockBuffer(page_entry->buffer, bufferLockMode);

	page_entry->page = BufferGetPage(page_entry->buffer);
	page_entry->ref_count++;
	page_entry->is_copy = false;
	*isNewPageEntry = true;

	return &page_entry->node;
}

ArtPageEntry *
_art_copy_page(Relation index, BlockNumber blockNum)
{
	ArtPageEntry * page_entry = NULL;
	Buffer page_buffer = ReadBuffer(index, blockNum);
	
	page_entry = (ArtPageEntry *) palloc0(sizeof(ArtPageEntry));
	
	page_entry->blk_num = blockNum;
	page_entry->page = (Page) palloc(BLCKSZ);

	memcpy(page_entry->page, BufferGetPage(page_buffer), BLCKSZ);

	page_entry->dirty = false;
	page_entry->buffer = InvalidBuffer;
	page_entry->ref_count = 1;
	page_entry->is_copy = true;

	ReleaseBuffer(page_buffer);

	return page_entry;
}

void
_art_flush_pages(Relation index, dlist_head * pageListHead)
{

	dlist_iter iter;
	ArtPageEntry * page_entry = NULL;

	dlist_foreach(iter, pageListHead)
	{
		page_entry = dlist_container(ArtPageEntry, node, iter.cur);

		dlist_delete(&page_entry->node);

		if (page_entry->buffer)
		{
			if (page_entry->dirty)
				MarkBufferDirty(page_entry->buffer);

			UnlockReleaseBuffer(page_entry->buffer);
		}
		else if (!page_entry->is_copy)
		{
			smgrextend(RelationGetSmgr(index), MAIN_FORKNUM, page_entry->blk_num,
					   (char *) page_entry->page, false);
			
			pfree(page_entry->page);
		}
		else if (page_entry->is_copy)
		{
			if (page_entry->dirty)
			{
				// is there better way to do this ?! TODO
				Buffer buffer = ReadBuffer(index, page_entry->blk_num);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				memcpy(BufferGetPage(buffer), page_entry->page, BLCKSZ);
				MarkBufferDirty(buffer);
				UnlockReleaseBuffer(buffer);
			}
	
			pfree(page_entry->page);
		}
	}
}
