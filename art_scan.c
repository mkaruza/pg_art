/*-------------------------------------------------------------------------
 *
 * artscan.c
 *		ART index scan functions.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "art.h"

typedef struct ArtScanOpaqueData
{
	Relation index;
	ArtTuple * art_tuple;
	StrategyNumber sk_strategy;
	pairingheap * leaf_list_queue;
	Buffer node_page_buffer;
	dlist_head leaf_entry_head;
	dlist_node * leaf_page_entry;
	ArtPageEntry * leaf_page;
	OffsetNumber leaf_num_items;
	OffsetNumber leaf_current_item;
	ItemPointerData * leaf_iptr;
	bool fetching;
} ArtScanOpaqueData;

typedef ArtScanOpaqueData *ArtScanOpaque;

static void _art_search(ArtScanOpaque scanOpaque, ArtNodeHeader * rootArtNode,
					    ItemPointer iptr, bool range, bool checkRange, int depth);
static int _art_find_cmp_order(const pairingheap_node * a,
							   const pairingheap_node * b,
							   void * arg);
static int _art_find_cmp_distance(const pairingheap_node * a,
								  const pairingheap_node * b,
								  void * arg);

int
_art_find_cmp_order(const pairingheap_node * a,
					const pairingheap_node * b,
					void * arg)
{
	BlockNumber bna =
		ItemPointerGetBlockNumberNoCheck(&((ArtQueueItemPointer *) a)->iptr);

	BlockNumber bnb = 
		ItemPointerGetBlockNumberNoCheck(&((ArtQueueItemPointer *) b)->iptr);


	if (bna == bnb)
	{
		return 0;
	}
	if (bna < bnb)
	{
		return 1;
	}
	else
	{
		return -1;
	}
}

int
_art_find_cmp_distance(const pairingheap_node * a,
					   const pairingheap_node * b,
					   void * arg)
{
	BlockNumber bna =
		ItemPointerGetBlockNumberNoCheck(&((ArtQueueItemPointer *) a)->iptr);

	BlockNumber bnb =
		ItemPointerGetBlockNumberNoCheck(&((ArtQueueItemPointer *) b)->iptr);

	BlockNumber bnc =
		ItemPointerGetBlockNumberNoCheck(((ItemPointer)arg));

	if (bnc == bnb)
		return 0;

	return 1;
}

void
_art_search(ArtScanOpaque scanOpaque, ArtNodeHeader * node,
			ItemPointer iptr, bool range, bool compare, int depth)
{
	Buffer next_node_buffer = InvalidBuffer;
	Buffer current_node_buffer = scanOpaque->node_page_buffer;

	pairingheap * children_queue = pairingheap_allocate(_art_find_cmp_distance, iptr);
	int prefix_len;

	if (node->node_type == NODE_LEAF)
	{
		ArtNodeLeaf * leaf = (ArtNodeLeaf*) node;

		if (range)
		{
			int32 cmp = 0;

			if (!compare)
			{
				_art_add_queue_itemptr(scanOpaque->leaf_list_queue, 
										iptr, false);
				return;
			}

			cmp = _art_compare_key(leaf->data[depth-1], scanOpaque->art_tuple->key[depth-1]);

			if (scanOpaque->sk_strategy == BTLessStrategyNumber)
			{
				if (cmp < 0)
				{
					_art_add_queue_itemptr(scanOpaque->leaf_list_queue, 
										   iptr, false);
				}
			}
			else if (scanOpaque->sk_strategy == BTLessEqualStrategyNumber)
			{
				if (cmp <= 0)
				{
					_art_add_queue_itemptr(scanOpaque->leaf_list_queue, 
										   iptr, false);
				}
			}
			else if (scanOpaque->sk_strategy == BTGreaterStrategyNumber)
			{
				if (cmp > 0)
				{
					_art_add_queue_itemptr(scanOpaque->leaf_list_queue, 
										  iptr, false);
				}
			}
			else if (scanOpaque->sk_strategy == BTGreaterEqualStrategyNumber)
			{
				if (cmp >= 0)
				{
					_art_add_queue_itemptr(scanOpaque->leaf_list_queue, 
										   iptr, false);
				}
			}

		}
		else if (!_art_leaf_matches(leaf, scanOpaque->art_tuple->key, scanOpaque->art_tuple->key_len))
		{
			_art_add_queue_itemptr(scanOpaque->leaf_list_queue, iptr, false);
		}


		return;
	}

	// Bail if the prefix does not match
	if (node->prefix_key_len)
	{
		prefix_len = 
			_art_check_prefix(node, scanOpaque->art_tuple->key,
							  scanOpaque->art_tuple->key_len, depth);

		if (prefix_len != Min(MAX_PREFIX_KEY_LEN, node->prefix_key_len))
			return;
	
		depth = depth + node->prefix_key_len;
	}
	
	if (!range)
	{
		ItemPointer iptr =
			_art_find_child_equal(node, scanOpaque->art_tuple->key[depth]);

		if (iptr != NULL)
			_art_add_queue_itemptr(children_queue, iptr, true);
	}
	else
	{
		_art_find_child_range(node, scanOpaque->art_tuple->key[depth],
							  scanOpaque->sk_strategy,
							  children_queue, compare);
	}

	if (pairingheap_is_empty(children_queue))
	{
		return;
	}

	while(!pairingheap_is_empty(children_queue))
	{
		ArtQueueItemPointer * scan_item_ptr = 
			(ArtQueueItemPointer *) pairingheap_remove_first(children_queue);

		node = _art_get_node_from_iptr(scanOpaque->index, &scan_item_ptr->iptr,
									   &next_node_buffer, BUFFER_LOCK_SHARE);

		if (current_node_buffer != next_node_buffer)
		{
			UnlockReleaseBuffer(scanOpaque->node_page_buffer);
			scanOpaque->node_page_buffer = next_node_buffer;
		}
		else
		{
			UnlockReleaseBuffer(next_node_buffer);
		}

		_art_search(scanOpaque, node, &scan_item_ptr->iptr, 
					range, scan_item_ptr->compare,
					depth + 1);
		
		pfree(scan_item_ptr);
	}
}


IndexScanDesc
artbeginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	ArtScanOpaque so;

	scan = RelationGetIndexScan(r, nkeys, norderbys);

	so = (ArtScanOpaque) palloc0(sizeof(ArtScanOpaqueData));
	so->fetching = false;
	so->index = r;

	dlist_init(&so->leaf_entry_head);
	so->leaf_page = NULL;
	so->leaf_page_entry = NULL;
	so->leaf_current_item = 0;
	so->leaf_iptr = NULL;


	scan->opaque = so;

	return scan;
}


void
artrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	ArtScanOpaque so = (ArtScanOpaque) scan->opaque;

	if (so->art_tuple)
		pfree(so->art_tuple);

	so->art_tuple = NULL;
	so->leaf_iptr = NULL;

	/* Update scan key, if a new one is given */
	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}
}


void
artendscan(IndexScanDesc scan)
{
	ArtScanOpaque so = (ArtScanOpaque) scan->opaque;

	_art_page_release(so->leaf_page);
	
	if (so->art_tuple)
		pfree(so->art_tuple);

	if (so->leaf_iptr)
		pfree(so->leaf_iptr);

	pfree(so);
}


bool
artgettuple(IndexScanDesc scan, ScanDirection dir)
{
	ArtNodeHeader * root_node;
	ArtScanOpaque so = (ArtScanOpaque) scan->opaque;
	ArtQueueItemPointer * leaf_iptr = NULL;
	ItemPointerData root_iptr;
	BlockNumber leaf_page_blk_num;
	OffsetNumber leaf_page_offset;
	bool is_new_page_entry = false;

	if (!so->fetching)
	{
		Datum search_datum [1] = { scan->keyData[0].sk_argument };
		bool is_nulls[1] = { false };

		so->art_tuple =
			_art_form_key(so->index, NULL, search_datum, is_nulls);

		so->sk_strategy = scan->keyData->sk_strategy;
		so->leaf_list_queue = pairingheap_allocate(_art_find_cmp_order, NULL);

		ItemPointerSet(&root_iptr, ART_ROOT_NODE_BLKNO, 1);

		root_node = _art_get_node_from_iptr(scan->indexRelation, &root_iptr,
											&so->node_page_buffer, BUFFER_LOCK_SHARE);

		_art_search(so, root_node, &root_iptr, 
					so->sk_strategy != BTEqualStrategyNumber, true, 0);

		UnlockReleaseBuffer(so->node_page_buffer);

		so->fetching = true;
	}

	if (so->leaf_iptr == NULL ||
	    so->leaf_num_items == so->leaf_current_item)
	{
		ArtNodeLeaf * leaf = NULL;

		if (so->leaf_iptr)
			pfree(so->leaf_iptr);

		so->leaf_iptr = NULL;

		if(pairingheap_is_empty(so->leaf_list_queue))
		{
			return false;
		}

		leaf_iptr =
			(ArtQueueItemPointer *) pairingheap_remove_first(so->leaf_list_queue);

		leaf_page_blk_num = ItemPointerGetBlockNumber(&leaf_iptr->iptr);
		leaf_page_offset = ItemPointerGetOffsetNumber(&leaf_iptr->iptr);

		so->leaf_num_items =  0;
		so->leaf_current_item = 0;

		so->leaf_page_entry = _art_load_page(scan->indexRelation, &so->leaf_entry_head,
											 leaf_page_blk_num,
											 BUFFER_LOCK_SHARE, &is_new_page_entry);

		if (!is_new_page_entry)
		{
			_art_page_release(so->leaf_page);
		}
		else
		{
			dlist_push_tail(&so->leaf_entry_head, so->leaf_page_entry);
			so->leaf_page = dlist_container(ArtPageEntry, node, so->leaf_page_entry);
		}

		leaf = 
			(ArtNodeLeaf *) PageGetItem(so->leaf_page->page,
										PageGetItemId(so->leaf_page->page, leaf_page_offset));

		if (ItemPointerIsValid(&leaf->next_leaf_iptr))
		{
			_art_add_queue_itemptr(so->leaf_list_queue, 
								   &leaf->next_leaf_iptr, false);
		}
		
		so->leaf_num_items = leaf->num_items;
		so->leaf_iptr = palloc0(sizeof(ItemPointerData) * leaf->num_items);
		memcpy(so->leaf_iptr, &leaf->data[leaf->key_len], sizeof(ItemPointerData) *leaf->num_items);
	
		_art_page_release(so->leaf_page);

		pfree(leaf_iptr);
	}

	ItemPointerCopy(&so->leaf_iptr[so->leaf_current_item], &scan->xs_heaptid);

	so->leaf_current_item++;

	return true;
}
