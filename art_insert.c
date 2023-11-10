/*-------------------------------------------------------------------------
 *
 * artinsert.c
 *		ART index build and insert functions.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#include "art.h"

#define ART_PAGE_SIZE \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ItemPointerData)) \
		- MAXALIGN(sizeof(ArtDataPageOpaqueData)))

/*
 * This structure contain information about node
 * on index page.
 */
typedef struct ArtNodeEntry
{
	dlist_node node;
	bool memory_node;
	ItemPointerData iptr;
	ArtNodeHeader * art_node;
	dlist_node * page_entry;
} ArtNodeEntry;

/*
 * This structure is used for initial index build.
 */
typedef struct ArtBuildState
{
	ArtMetaDataPageOpaqueData metadata;	/* Metadata page */
	BlockNumber num_allocated_pages;	/* tracking allocated pages (build only) */
	uint64 n_tuples;					/* total number of tuples indexed (build only) */
	HTAB * page_lookup_hash;			/* Lookup hash */
} ArtBuildState;

/*
 * Structure used for both insert and build callbacks. Keep track
 * of pages / nodes / cache.
 */
typedef struct ArtState
{
	Relation index;
	ArtBuildState * build_state; 		/* build state */
	dlist_head art_nodes;				/* list of art nodes */
	dlist_head pages;					/* list of pages */
	dlist_node * node_last_page;		/* internal node tail page */
	dlist_node * leaf_last_page;		/* leaf tail page*/
	MemoryContext build_ctx;			/* build temporary context */
} ArtState;

/*
 * Hash entry of mapping block number to in memory ArtPageEntry.
 */
typedef struct ArtPageEntryHashEntry
{
	BlockNumber blk_num;
	void * page_entry_memory_pointer;
} ArtPageEntryHashEntry;

#define IS_MEMORY_BUILD(x) ((x)->build_state != NULL)

static void _init_state(ArtState * state);
static ArtNodeHeader * _get_node(ArtNodeEntry * nodeEntry);
static void _update_leaf_item(ArtState * state, ArtNodeEntry * leafEntry, ArtTuple * artTuple);
static ArtNodeEntry * _add_leaf(ArtState * state, ItemPointer parentIptr, ArtTuple * artTuple);
static ArtNodeHeader * _add_child_node4(ArtNode4 * n, uint8 key, 
									   ItemPointer iptr);
static ArtNodeHeader * _add_child_node16(ArtNode16 * n, uint8 key,
										 ItemPointer iptr);
static ArtNodeHeader * _add_child_node48(ArtNode48 * n, uint8 key,
										 ItemPointer itpr);
static ArtNodeHeader * _add_child_node256(ArtNode256 * n, uint8 key,
										  ItemPointer iptr);
static ArtNodeHeader * _add_child(ArtNodeHeader * node, uint8 key, ItemPointer iptr);
static void _replace_child_iptr(ArtNodeHeader *node, uint8 key, ItemPointer iptr);
static void _update_child_node_parent_iptr(ArtState * state, ItemPointer childIptr,
										   ItemPointer parentIptr);
static void _update_child_list_parent_iptr(ArtState * state, ArtNodeEntry * node);
static ArtNodeEntry * _get_node_from_iptr(ArtState * state, ItemPointer iptr);
static ArtPageEntry * _get_page_with_free_space(ArtState * state,
												uint8 pageType,
												Size itemSize);
static ArtNodeEntry * _page_add_node(ArtState * state, ArtPageEntry * pageEntry,
									 ArtNodeHeader * node);
static void _page_update_node(ArtNodeEntry * nodeEntry, ArtNodeHeader *node);
static ArtNodeEntry * _page_replace_node(ArtState * state,
										 ArtNodeEntry * oldNodeEntry,
										 Size oldNodeSize,
										 ArtNodeHeader * node,
										 uint8_t key);
static ItemPointer _node_insert_recursive(ArtState * state,
										 ArtNodeHeader * node,
					 					 ArtTuple * artTuple,
					 					 int depth);
static bool _node_insert(ArtState * state, ArtTuple * artTuple);
static void _node_release(ArtNodeEntry * node);
static void _node_release_list(ArtState * state);

void 
_init_state(ArtState * state)
{
	dlist_init(&state->pages);
	dlist_init(&state->art_nodes);
	state->node_last_page = NULL;
	state->leaf_last_page = NULL;
}

void
_art_init_page_hash(HTAB ** pageHashLookup)
{
	HASHCTL page_hash_lookup_hash;

	page_hash_lookup_hash.keysize = sizeof(BlockNumber);
	page_hash_lookup_hash.entrysize = sizeof(ArtPageEntryHashEntry);
	*pageHashLookup = hash_create("ART index build hash", 4096,
								  &page_hash_lookup_hash,
								  HASH_ELEM | HASH_BLOBS);
}

void
_art_add_page_hash(HTAB * pageHashLookup, BlockNumber blockNumber, ArtPageEntry * pageEntry)
{
	ArtPageEntryHashEntry * hash_entry;

	hash_entry =
		hash_search(pageHashLookup, &blockNumber, HASH_ENTER, NULL);
	
	hash_entry->page_entry_memory_pointer = pageEntry;
}

ArtPageEntry * 
_art_get_page_hash(HTAB * pageHashLookup, BlockNumber blockNumber)
{
	ArtPageEntry * page_entry = NULL;
	ArtPageEntryHashEntry * hash_page_entry;
	bool found;

	hash_page_entry = hash_search(pageHashLookup, &blockNumber, HASH_FIND, &found);

	if (found)
	{
		page_entry = (ArtPageEntry *) hash_page_entry->page_entry_memory_pointer;
		page_entry->ref_count++;
	}


	return page_entry;
}

ArtNodeHeader *
_get_node(ArtNodeEntry * nodeEntry)
{
	OffsetNumber off = ItemPointerGetOffsetNumber(&nodeEntry->iptr);
	ArtPageEntry * page_entry = dlist_container(ArtPageEntry, node, nodeEntry->page_entry);


	return (ArtNodeHeader *) 
		PageGetItem(page_entry->page, PageGetItemId(page_entry->page, off));
}

void
_update_leaf_item(ArtState * state, ArtNodeEntry * leafEntry, ArtTuple * artTuple)
{
	ArtPageEntry * init_leaf_page = 
		dlist_container(ArtPageEntry, node, leafEntry->page_entry);
	ArtNodeLeaf * init_leaf = (ArtNodeLeaf*) leafEntry->art_node;

	ArtNodeLeaf * leaf = NULL;
	ArtPageEntry * leaf_page = init_leaf_page;
	ArtNodeEntry * leaf_node_entry = leafEntry;
	Offset leaf_entry_off = ItemPointerGetOffsetNumber(&leaf_node_entry->iptr);

	ArtNodeLeaf * new_leaf = NULL;
	
	Size leaf_page_size;

	leaf = (ArtNodeLeaf *) leafEntry->art_node;

	for (;;)
	{
		if (ItemPointerIsValid(&leaf->last_leaf_iptr))
		{
			leaf_node_entry = _get_node_from_iptr(state, &leaf->last_leaf_iptr);
			leaf_entry_off = ItemPointerGetOffsetNumber(&leaf_node_entry->iptr);
			leaf_page = dlist_container(ArtPageEntry, node, leaf_node_entry->page_entry);
			leaf = (ArtNodeLeaf*) leaf_node_entry->art_node;
		}
		else
		{
			Size leaf_size = _art_node_size((ArtNodeHeader *) leaf);

			Size new_leaf_size = leaf_size + sizeof(ItemPointerData);

			leaf_page_size = PageGetFreeSpace(leaf_page->page);

			if (leaf_page_size > MAXALIGN(sizeof(ItemPointerData)))
			{
				ItemPointer new_leaf_iptr = NULL;

				new_leaf = (ArtNodeLeaf*) palloc0(new_leaf_size + sizeof(ItemPointerData));

				ItemPointerSetInvalid(&new_leaf->next_leaf_iptr);
				ItemPointerSetInvalid(&new_leaf->last_leaf_iptr);

				new_leaf->key_len = leaf->key_len;
				new_leaf->num_items = leaf->num_items + 1;

				if (update_parent_iptr)
				{
					ItemPointerCopy(&leaf->parent_iptr, &new_leaf->parent_iptr);
				}

				memcpy(new_leaf->data, leaf->data,
					   leaf->key_len + 
					   sizeof(ItemPointerData) * leaf->num_items);

				new_leaf_iptr = (ItemPointer)&new_leaf->data[new_leaf->key_len];

				ItemPointerCopy(&artTuple->iptr, &new_leaf_iptr[new_leaf->num_items-1]);

				START_CRIT_SECTION();
				PageIndexTupleOverwrite(leaf_page->page, leaf_entry_off,
										(Item) new_leaf, new_leaf_size);
				END_CRIT_SECTION();

				pfree(new_leaf);

				leaf_page->dirty = true;

				break;
			}
			else
			{
				ArtNodeEntry * new_leaf_entry = NULL;

				new_leaf_entry =
					_add_leaf(state, &leaf->parent_iptr, artTuple);
	
				ItemPointerCopy(&new_leaf_entry->iptr, &leaf->next_leaf_iptr);

				START_CRIT_SECTION();

				PageIndexTupleOverwrite(leaf_page->page, leaf_entry_off,
										(Item) leaf, leaf_size);

				END_CRIT_SECTION();

				leaf_page->dirty = true;

				// Update init leaf item to point to last leaf iptr
				ItemPointerCopy(&new_leaf_entry->iptr, &init_leaf->last_leaf_iptr);

				START_CRIT_SECTION();

				PageIndexTupleOverwrite(init_leaf_page->page, 
										ItemPointerGetOffsetNumber(&leafEntry->iptr),
										(Item) init_leaf, _art_node_size((ArtNodeHeader*) init_leaf));
				
				init_leaf_page->dirty = true;

				END_CRIT_SECTION();

				break;
			}
		}
	}
}

ArtNodeEntry * 
_add_leaf(ArtState * state, ItemPointer parentIptr, ArtTuple * artTuple)
{
	ArtPageEntry * new_leaf_page_entry;
	ArtNodeEntry * new_leaf_node_entry;

	Size leaf_key_size = artTuple->key_len;
	ArtNodeLeaf * leaf = (ArtNodeLeaf*) palloc0(sizeof(ArtNodeLeaf) + leaf_key_size + sizeof(ItemPointerData));

	leaf->key_len = leaf_key_size;
	leaf->num_items = 1;
	
	memcpy(leaf->data, artTuple->key, leaf_key_size);
	memcpy(leaf->data + artTuple->key_len, &artTuple->iptr, sizeof(ItemPointerData));

	if (parentIptr && update_parent_iptr)
	{
		ItemPointerCopy(parentIptr, &leaf->parent_iptr);
	}

	new_leaf_page_entry = 
		_get_page_with_free_space(state,
								  ART_LEAF_PAGE,
								  _art_node_size((ArtNodeHeader*) leaf));

	new_leaf_node_entry = _page_add_node(state, new_leaf_page_entry, (ArtNodeHeader*) leaf);

	return new_leaf_node_entry;
}

ArtNodeHeader *
_add_child_node4(ArtNode4 * n, uint8_t key, ItemPointer iptr)
{
	if (n->node.num_children < 4)
	{
		int idx;
		
		for (idx=0; idx < n->node.num_children; idx++)
			if (key < n->keys[idx]) break;

		// Shift to make room
		memmove(n->keys + idx + 1, n->keys+idx, n->node.num_children - idx);
		memmove(n->children + idx + 1, n->children + idx,
				(n->node.num_children - idx) * sizeof(ItemPointerData));

		n->keys[idx] = key;
		ItemPointerCopy(iptr, &(n->children[idx]));
		n->node.num_children++;

		return NULL;
	}
	else
	{
		ArtNode16 * new_node = (ArtNode16*) _art_alloc_node(NODE_16);

		memcpy(new_node->children, n->children,
			   sizeof(ItemPointerData) * n->node.num_children);
		memcpy(new_node->keys, n->keys,
			   sizeof(uint8_t) * n->node.num_children);

		_art_copy_header((ArtNodeHeader*) new_node, (ArtNodeHeader*) n);
		_add_child_node16(new_node, key, iptr);

		return (ArtNodeHeader *) new_node;
	}
}

ArtNodeHeader * 
_add_child_node16(ArtNode16 * n, uint8_t key, ItemPointer iptr)
{
	if (n->node.num_children < 16)
	{
		int idx;
		
		for (idx = 0; idx < n->node.num_children; idx++)
			if (key < n->keys[idx]) break;

		// Shift to make room
		memmove(n->keys + idx + 1, n->keys+idx, n->node.num_children - idx);
		memmove(n->children+idx + 1, n->children+idx,
				(n->node.num_children - idx) * sizeof(ItemPointerData));

		n->keys[idx] = key;
		ItemPointerCopy(iptr, &(n->children[idx]));
		n->node.num_children++;

		return NULL;
	}
	else
	{
		ArtNode48 * new_node = (ArtNode48*) _art_alloc_node(NODE_48);

		// Copy the child pointers and populate the key map
		memcpy(new_node->children, n->children, 
			   sizeof(ItemPointerData) * n->node.num_children);

		for (int i = 0; i <n ->node.num_children; i++)
			new_node->keys[n->keys[i]] = i + 1;

		_art_copy_header((ArtNodeHeader*)new_node, (ArtNodeHeader*)n);
		_add_child_node48(new_node, key, iptr);

		return (ArtNodeHeader*) new_node;
	}
}

ArtNodeHeader *
_add_child_node48(ArtNode48 * n, uint8_t key, ItemPointer iptr)
{
	if (n->node.num_children < 48) 
	{
		int idx = 0;

		while (ItemPointerIsValid(&n->children[idx]))
			idx++;

		n->keys[key] = idx + 1;
		ItemPointerCopy(iptr, &(n->children[idx]));
		n->node.num_children++;

		return NULL;
	}
	else
	{
		ArtNode256 *new_node = (ArtNode256*) _art_alloc_node(NODE_256);

		for (int i = 0; i < 256; i++)
		{
			if (n->keys[i])
				ItemPointerCopy(&n->children[n->keys[i] - 1], &new_node->children[i]);
		}

		_art_copy_header((ArtNodeHeader*)new_node, (ArtNodeHeader*)n);
		_add_child_node256(new_node, key, iptr);

		return (ArtNodeHeader*) new_node;
	}
}

ArtNodeHeader * 
_add_child_node256(ArtNode256 * n, uint8_t key, ItemPointer iptr)
{
	ItemPointerCopy(iptr, &(n->children[key]));
	n->node.num_children++;
	return NULL;
}

ArtNodeHeader *
_add_child(ArtNodeHeader *node, uint8 key, ItemPointer iptr)
{
	switch (node->node_type)
	{
		case NODE_4:
			return _add_child_node4((ArtNode4*) node, key, iptr);

		case NODE_16:
			return _add_child_node16((ArtNode16*) node, key, iptr);

		case NODE_48:
			return _add_child_node48((ArtNode48*) node, key, iptr);

		case NODE_256:
			return _add_child_node256((ArtNode256*) node, key, iptr);
		
		default:
			elog(ERROR, "Unknown node type");
	}
}

void
_replace_child_iptr(ArtNodeHeader * node, uint8 key, ItemPointer iptr)
{
	ItemPointer child_iptr = _art_find_child_equal(node, key);
	ItemPointerCopy(iptr, child_iptr);
}

void
_update_child_node_parent_iptr(ArtState * state, ItemPointer childIptr,
							   ItemPointer parentIptr)
{

	ArtNodeHeader * child_node;
	Buffer child_node_buffer = InvalidBuffer;

	BlockNumber node_block_number = ItemPointerGetBlockNumber(childIptr);
	BlockNumber node_offset = ItemPointerGetOffsetNumber(childIptr);

	if (IS_MEMORY_BUILD(state))
	{
		ArtPageEntry * node_page = 
			_art_get_page_hash(state->build_state->page_lookup_hash, node_block_number);

		if (!node_page)
		{
			node_page = _art_copy_page(state->index, node_block_number);
			dlist_push_head(&state->pages, &node_page->node);
			_art_add_page_hash(state->build_state->page_lookup_hash,
							   node_block_number,
							   node_page);
		}

		child_node =
			(ArtNodeHeader *) PageGetItem(node_page->page,
									PageGetItemId(node_page->page, node_offset));
	}
	else
	{
		dlist_iter iter;
		ArtPageEntry * page_entry;
		bool page_found = false;

		dlist_foreach(iter, &state->pages)
		{
			page_entry = dlist_container(ArtPageEntry, node, iter.cur);
			
			if (page_entry->blk_num == node_block_number)
			{
				page_found = true;
				break;
			}
		}

		if (page_found)
		{
			child_node =
				(ArtNodeHeader *) PageGetItem(page_entry->page,
											  PageGetItemId(page_entry->page, node_offset));
		}
		else
		{
			child_node = 
				_art_get_node_from_iptr(state->index, childIptr, 
										&child_node_buffer, BUFFER_LOCK_EXCLUSIVE);
		}
	}

	if (update_parent_iptr)
	{
		ItemPointerCopy(parentIptr, &child_node->parent_iptr);
	}

	if (!IS_MEMORY_BUILD(state) && child_node_buffer != InvalidBuffer)
	{
		UnlockReleaseBuffer(child_node_buffer);
	}
}

void
_update_child_list_parent_iptr(ArtState * state, ArtNodeEntry * nodeEntry)
{
	ArtNodeHeader * node = (ArtNodeHeader *) nodeEntry->art_node;
	int i;

	switch(node->node_type)
	{
		case NODE_4:
		{
			ArtNode4 * node4 = (ArtNode4 *) node;
			for (i = 0 ; i < node4->node.num_children; i++)
			{
				_update_child_node_parent_iptr(state, &node4->children[i],
											   &nodeEntry->iptr);
			}
			break;
		}
		case NODE_16:
		{
			ArtNode16 * node16 = (ArtNode16 *) node;
			for (i = 0 ; i < node16->node.num_children; i++)
			{
				_update_child_node_parent_iptr(state, &node16->children[i],
											   &nodeEntry->iptr);
			}
			break;
		}
		case NODE_48:
		{
			ArtNode48 * node48 = (ArtNode48 *) node;
			for (i = 0 ; i < node48->node.num_children; i++)
			{
				_update_child_node_parent_iptr(state, &node48->children[i],
											   &nodeEntry->iptr);
			}
			break;
		}
		case NODE_256:
		{
			ArtNode256 * node256 = (ArtNode256 *) node;
			for (i = 0 ; i < 256; i++)
			{
				if (ItemPointerIsValid(&node256->children[i]))
				{
					_update_child_node_parent_iptr(state, &node256->children[i],
												   &nodeEntry->iptr);
				}
			}
		}
	}
}

ArtNodeEntry *
_get_node_from_iptr(ArtState * state, ItemPointer iptr)
{
	BlockNumber blk_num = ItemPointerGetBlockNumber(iptr);
	ArtNodeEntry * node_entry = (ArtNodeEntry *) palloc0(sizeof(ArtNodeEntry));
	ArtPageEntry * page_entry;
	bool is_new_page_entry = false;

	if (IS_MEMORY_BUILD(state))
	{
		page_entry = _art_get_page_hash(state->build_state->page_lookup_hash, blk_num);

		if (!page_entry)
		{
			page_entry = _art_copy_page(state->index, blk_num);
			dlist_push_head(&state->pages, &page_entry->node);
			_art_add_page_hash(state->build_state->page_lookup_hash,
							   page_entry->blk_num,
							   page_entry);
		}
	}
	else
	{
		page_entry = 
			dlist_container(ArtPageEntry, node,
							_art_load_page(state->index, &state->pages, blk_num,
										   BUFFER_LOCK_EXCLUSIVE, &is_new_page_entry));
		if (is_new_page_entry)
			dlist_push_head(&state->pages, &page_entry->node);
	}

	ItemPointerCopy(iptr, &node_entry->iptr);

	node_entry->page_entry = &page_entry->node;
	node_entry->art_node = _get_node(node_entry);
	node_entry->memory_node = false;

	dlist_push_head(&state->art_nodes, &node_entry->node);

	return node_entry;
}


ArtPageEntry * 
_get_page_with_free_space(ArtState * state, uint8 pageType, Size itemsz)
{
	ArtPageEntry * last_page_entry = NULL;
	ArtPageEntry * new_page_entry = NULL;
	dlist_head metadata_page_head;
	ArtPageEntry * metadata_page_entry = NULL;
	ArtMetaDataPageOpaque metadata_opaque = NULL;

	Size page_freespace;
	ArtDataPageOpaque opaque;

	bool is_new_page_entry = false;
	bool empty_last_page;

	dlist_node *last_page = NULL;
	BlockNumber last_page_blk_num;

	if (pageType == ART_NODE_PAGE)
	{
		last_page = state->node_last_page;
	}
	else
	{
		last_page = state->leaf_last_page;
	}

	empty_last_page = last_page == NULL;

	if (!IS_MEMORY_BUILD(state) && !last_page)
	{
		dlist_init(&metadata_page_head);

		metadata_page_entry = _art_get_metadata_page(state->index);

		dlist_push_head(&metadata_page_head, &metadata_page_entry->node);
	
		metadata_opaque =
			(ArtMetaDataPageOpaque) PageGetSpecialPointer(metadata_page_entry->page);

		if (pageType == ART_NODE_PAGE)
		{
			last_page_blk_num = metadata_opaque->last_internal_node_blk_num;
		}
		else
		{
			last_page_blk_num = metadata_opaque->last_leaf_blk_num;
		}

		last_page =
			_art_load_page(state->index, &state->pages,
						   last_page_blk_num,
						   BUFFER_LOCK_EXCLUSIVE,
						   &is_new_page_entry);

		if (is_new_page_entry)
			dlist_push_tail(&state->pages, last_page);

		_art_page_release(metadata_page_entry);
	}

	last_page_entry = dlist_container(ArtPageEntry, node, last_page);

	page_freespace = PageGetFreeSpace(last_page_entry->page);

	// Keep some freespace for LEAF pages
	if (pageType == ART_LEAF_PAGE)
		page_freespace *= page_leaf_insert_treshold; 

	// Check if tail pages have enough free space
	if (page_freespace > MAXALIGN(itemsz))
	{
		if (metadata_page_entry == NULL && !IS_MEMORY_BUILD(state) && 
			last_page)
		{
			last_page_entry->ref_count++;
		}
		return last_page_entry;
	}

	// check cached page information for freespace
	
	if (IS_MEMORY_BUILD(state))
	{
		new_page_entry = _art_new_page(pageType == ART_NODE_PAGE ? ART_NODE_PAGE : ART_LEAF_PAGE);
		new_page_entry->blk_num = state->build_state->num_allocated_pages++;
		_art_add_page_hash(state->build_state->page_lookup_hash,
						   new_page_entry->blk_num,
						   new_page_entry);
	}
	else
	{
		dlist_init(&metadata_page_head);

		metadata_page_entry = _art_get_metadata_page(state->index);

		metadata_opaque =
			(ArtMetaDataPageOpaque) PageGetSpecialPointer(metadata_page_entry->page);

		dlist_push_head(&metadata_page_head, &metadata_page_entry->node);

		new_page_entry = 
			_art_get_buffer(state->index,
							pageType == ART_NODE_PAGE ? ART_NODE_PAGE : ART_LEAF_PAGE);
	}

	opaque = (ArtDataPageOpaque) PageGetSpecialPointer(last_page_entry->page);
	opaque->right_link = new_page_entry->blk_num;

	dlist_push_tail(&state->pages, &new_page_entry->node);

	if (IS_MEMORY_BUILD(state))
	{
		if (pageType == ART_NODE_PAGE)
		{
			state->build_state->metadata.last_internal_node_blk_num =
				new_page_entry->blk_num;
		}
		else
		{
			state->build_state->metadata.last_leaf_blk_num =
				new_page_entry->blk_num;
		}
	}
	else
	{
		if (pageType == ART_NODE_PAGE)
		{
			metadata_opaque->last_internal_node_blk_num = new_page_entry->blk_num;
		}
		else
		{
			metadata_opaque->last_leaf_blk_num = new_page_entry->blk_num;
		}

		metadata_page_entry->dirty = true;

		// release if we last_data_page
		if (empty_last_page)
			_art_page_release(last_page_entry);

		_art_page_release(metadata_page_entry);
	}

	if (pageType == ART_NODE_PAGE)
	{
		state->node_last_page = &new_page_entry->node;
	}
	else
	{
		state->leaf_last_page =  &new_page_entry->node;
	}

	return new_page_entry;
}

ArtNodeEntry *
_page_add_node(ArtState * state, ArtPageEntry * pageEntry, 
			   ArtNodeHeader * node)
{
	ArtNodeEntry * new_node_entry;
	ItemOffset page_node_offset;
	ArtDataPageOpaque opaque;

	new_node_entry = (ArtNodeEntry *) palloc0(sizeof(ArtNodeEntry));
	opaque = (ArtDataPageOpaque) PageGetSpecialPointer(pageEntry->page);

	new_node_entry->page_entry = &pageEntry->node;
	new_node_entry->art_node = node;
	new_node_entry->memory_node = true;

	START_CRIT_SECTION();
	page_node_offset = 
		PageAddItem(pageEntry->page, (Item) node, _art_node_size(node), 0, false, false);
	END_CRIT_SECTION();

	ItemPointerSetOffsetNumber(&new_node_entry->iptr, page_node_offset);
	ItemPointerSetBlockNumber(&new_node_entry->iptr, pageEntry->blk_num);

	dlist_push_head(&state->art_nodes, &new_node_entry->node);
	pageEntry->dirty = true;

	// page cache

	opaque->n_total++;

	return new_node_entry;
}

void
_page_update_node(ArtNodeEntry * nodeEntry, ArtNodeHeader * node)
{
	Offset off = ItemPointerGetOffsetNumber(&nodeEntry->iptr);
	Size nodeSize = _art_node_size(node);
	ArtPageEntry * page_entry = dlist_container(ArtPageEntry, node, nodeEntry->page_entry);

	START_CRIT_SECTION();

	PageIndexTupleOverwrite(page_entry->page, off, (Item) node, nodeSize);
	page_entry->dirty = true;
	
	END_CRIT_SECTION();
}

ArtNodeEntry *
_page_replace_node(ArtState * state,
				   ArtNodeEntry * oldNodeEntry,
				   Size oldNodeSize,
				   ArtNodeHeader * node,
				   uint8_t key)
{
	ArtNodeEntry * new_node_entry;
	ArtPageEntry * new_page_entry;
	
	ArtNodeHeader * parent_node;
	ArtNodeEntry * parent_node_entry = 
		dlist_container(ArtNodeEntry, node, dlist_next_node(&state->art_nodes, &oldNodeEntry->node));

	ArtPageEntry * old_page_entry = dlist_container(ArtPageEntry, node, oldNodeEntry->page_entry);

	Offset old_off = ItemPointerGetOffsetNumber(&oldNodeEntry->iptr);
	ArtDataPageOpaque opaque;

	if (PageGetExactFreeSpace(old_page_entry->page) >= 
		 MAXALIGN(_art_node_size(node) - oldNodeSize))
	{
		START_CRIT_SECTION();
		PageIndexTupleOverwrite(old_page_entry->page, old_off, (Item) node, _art_node_size(node));
		END_CRIT_SECTION();

		return NULL;
	}
	else
	{
		START_CRIT_SECTION();
		PageIndexTupleDeleteNoCompact(old_page_entry->page, old_off);
		END_CRIT_SECTION();

		old_page_entry->dirty = true;

		opaque = (ArtDataPageOpaque) PageGetSpecialPointer(old_page_entry->page);
		opaque->n_deleted++;
		opaque->deleted_item_size += oldNodeSize;

		new_page_entry = _get_page_with_free_space(state,
												   ART_NODE_PAGE,
												   _art_node_size((ArtNodeHeader*) node));

		new_node_entry = _page_add_node(state, new_page_entry, (ArtNodeHeader*) node);

		parent_node = _get_node(parent_node_entry);

		_replace_child_iptr(parent_node, key, &new_node_entry->iptr);

		_page_update_node(parent_node_entry, parent_node);

		return new_node_entry;
	}
}


ItemPointer
_node_insert_recursive(ArtState * state,
					   ArtNodeHeader * node,
					   ArtTuple *artTuple,
					   int depth)
{
	ArtNodeEntry * node_entry = dlist_container(ArtNodeEntry, node, dlist_head_node(&state->art_nodes));
	ArtNodeEntry * parent_node_entry = NULL;
	ArtNodeHeader * parent_node = NULL;

	if (dlist_has_next(&state->art_nodes, &node_entry->node))
	{
		parent_node_entry = 
			dlist_container(ArtNodeEntry, node, 
							dlist_next_node(&state->art_nodes, &node_entry->node));

		parent_node = parent_node_entry->art_node;

		/* We can now discard grand parent node (and release page if needed) */
		if (!IS_MEMORY_BUILD(state) &&
			dlist_has_next(&state->art_nodes, &parent_node_entry->node))
		{
			ArtNodeEntry * grand_parent_node = 
				dlist_container(ArtNodeEntry,
								node,
								dlist_next_node(&state->art_nodes, &parent_node_entry->node));

			dlist_delete(&grand_parent_node->node);
			_node_release(dlist_container(ArtNodeEntry, node, &grand_parent_node->node));
		}
	}

	if (node->node_type == NODE_LEAF)
	{
		ArtNodeLeaf * leaf = (ArtNodeLeaf *) node;
	
		ArtNodeEntry * leaf_node_entry = node_entry;
		
		ArtNodeLeaf * new_leaf = NULL;
		ArtNodeEntry * new_leaf_node_entry = NULL;

		ArtNode4 * new_node4 = NULL;
		ArtPageEntry * new_node4_page_entry = NULL;
		ArtNodeEntry * new_node4_node_entry = NULL;

		int longest_prefix = 0;

		// Check if we are updating an existing value
		if (!_art_leaf_matches(leaf, artTuple->key, artTuple->key_len))
		{
			_update_leaf_item(state, leaf_node_entry, artTuple);
			return NULL;
		}

		// Create a new leaf
		new_leaf_node_entry = _add_leaf(state, NULL, artTuple);
		new_leaf =  (ArtNodeLeaf *) new_leaf_node_entry->art_node;
	
		// Determine longest prefix
		longest_prefix = _art_longest_common_prefix(leaf, new_leaf, depth);

		// New value, we must split the leaf into a node4
		new_node4 = (ArtNode4 *) _art_alloc_node(NODE_4);
		new_node4->node.prefix_key_len = longest_prefix;
		memcpy(new_node4->node.prefix, artTuple->key + depth, 
			  Min(MAX_PREFIX_KEY_LEN, longest_prefix));

		_add_child((ArtNodeHeader *) new_node4, leaf->data[depth+longest_prefix],
				   &leaf_node_entry->iptr);

		_add_child((ArtNodeHeader *) new_node4, new_leaf->data[depth+longest_prefix],
				   &new_leaf_node_entry->iptr);

		// Update new node4 parent iptr
		if (update_parent_iptr)
		{
			ItemPointerCopy(&parent_node_entry->iptr, &new_node4->node.parent_iptr);
		}

		new_node4_page_entry = 
			_get_page_with_free_space(state,
									  ART_NODE_PAGE,
									 _art_node_size((ArtNodeHeader*) new_node4));

		new_node4_node_entry = _page_add_node(state, new_node4_page_entry,
											  (ArtNodeHeader*) new_node4);

		// Update leaf to parent node
		if (update_parent_iptr)
		{
			ItemPointerCopy(&new_node4_node_entry->iptr, &new_leaf->parent_iptr);
			_page_update_node(new_leaf_node_entry, (ArtNodeHeader*) new_leaf);
		}

		// update to point to node4
		_replace_child_iptr(parent_node, artTuple->key[depth - 1],
							&new_node4_node_entry->iptr);

		_page_update_node(parent_node_entry, (ArtNodeHeader*) parent_node);

		return NULL;
	}

	if (node->prefix_key_len)
	{
		ArtNode4 * new_node4 = NULL;
		ArtPageEntry * new_node4_page_entry = NULL;
		ArtNodeEntry * new_node4_node_entry = NULL;

		ArtNodeLeaf * leaf = NULL;
		ArtNodeEntry * leaf_node_entry = NULL;
		
		ArtNodeLeaf * minimum_leaf = NULL;

		// Determine if the prefixes differ, since we need to split

		uint8_t prefix_diff = 0;

		if(IS_MEMORY_BUILD(state))
		{
			prefix_diff = 
				_art_prefix_mismatch(state->index, node,
									 state->build_state->page_lookup_hash,
									 &state->pages,
								 	 artTuple->key, artTuple->key_len, depth);
		}
		else
		{
			prefix_diff = 
				_art_prefix_mismatch(state->index, node,
									 NULL,
									 NULL,
								 	 artTuple->key, artTuple->key_len, depth);
		}

		if (prefix_diff >= node->prefix_key_len)
		{
			depth += node->prefix_key_len;
			goto RECURSE_SEARCH;
		}

		// Create a new node
		new_node4 = (ArtNode4*) _art_alloc_node(NODE_4);

		new_node4->node.prefix_key_len = prefix_diff;
		memcpy(new_node4->node.prefix, node->prefix, Min(MAX_PREFIX_KEY_LEN, prefix_diff));

		// Adjust the prefix of the old node
		if (node->prefix_key_len <= MAX_PREFIX_KEY_LEN)
		{
			_add_child((ArtNodeHeader *) new_node4, node->prefix[prefix_diff], &node_entry->iptr);
			node->prefix_key_len -= (prefix_diff+1);
			memmove(node->prefix, node->prefix + prefix_diff + 1,
					Min(MAX_PREFIX_KEY_LEN, node->prefix_key_len));
			memset(node->prefix + node->prefix_key_len, 0, 
				   MAX_PREFIX_KEY_LEN - node->prefix_key_len);
		} 
		else
		{
			node->prefix_key_len -= (prefix_diff+1);

			if (IS_MEMORY_BUILD(state))
			{
				minimum_leaf =
					_art_minimum_leaf(state->index, node, 
									  state->build_state->page_lookup_hash,
									  &state->pages);
			}
			else
			{
				minimum_leaf = _art_minimum_leaf(state->index, node, NULL, NULL);
			}

			_add_child((ArtNodeHeader *) new_node4, 
					   minimum_leaf->data[depth+prefix_diff], &node_entry->iptr);

			memcpy(node->prefix, minimum_leaf->data + depth + prefix_diff + 1,
					Min(MAX_PREFIX_KEY_LEN, node->prefix_key_len));
		}

		// Update node for new prefix changes
		_page_update_node(node_entry, node);

		// Leaf for new new node4
		leaf_node_entry = _add_leaf(state, NULL, artTuple);
		leaf = (ArtNodeLeaf*) leaf_node_entry->art_node;

		_add_child((ArtNodeHeader*) new_node4, artTuple->key[depth+prefix_diff],
				   &leaf_node_entry->iptr);

		// Update new node4 parent iptr
		ItemPointerCopy(&node_entry->iptr, &new_node4->node.parent_iptr);

		// persist node4 to index
		new_node4_page_entry = 
			_get_page_with_free_space(state,
									  ART_NODE_PAGE,
									  _art_node_size((ArtNodeHeader*) new_node4));


		new_node4_node_entry = _page_add_node(state, new_node4_page_entry, (ArtNodeHeader*) new_node4);

		// Update leaf to new node4
		if (update_parent_iptr)
		{
			ItemPointerCopy(&new_node4_node_entry->iptr, &leaf->parent_iptr);
			_page_update_node(leaf_node_entry, (ArtNodeHeader*) leaf);
		}

		// Update parent to point to new node4
		_replace_child_iptr(parent_node, artTuple->key[depth-1], &new_node4_node_entry->iptr);

		_page_update_node(parent_node_entry, parent_node);

		return NULL;
	}

RECURSE_SEARCH:;

	{
		ItemPointer iptr;
		ArtNodeEntry * leaf_node_entry;
		ArtNodeHeader * replaced_node;

		iptr = _art_find_child_equal(node, artTuple->key[depth]);

		if (ItemPointerIsValid(iptr))
		{
			ArtNodeEntry * child_node_entry = _get_node_from_iptr(state, iptr);
			return _node_insert_recursive(state, child_node_entry->art_node, artTuple, depth + 1);
		}

		leaf_node_entry = _add_leaf(state, &node_entry->iptr, artTuple);

		replaced_node = _add_child(node, artTuple->key[depth], &leaf_node_entry->iptr);

		if (replaced_node)
		{
			ArtNodeEntry * new_item_node_entry = NULL;
			new_item_node_entry =
				_page_replace_node(state, node_entry,
								   _art_node_size(node),
								   replaced_node, artTuple->key[depth - node->prefix_key_len -1]);
		
			if (new_item_node_entry && update_parent_iptr)
			{
				_update_child_list_parent_iptr(state, new_item_node_entry);
			}

		}
		else
		{
			_page_update_node(node_entry, node);
		}
	}

	return NULL;
}

bool
_node_insert(ArtState * state, ArtTuple * artTuple)
{
	ArtNodeEntry * art_node_entry = NULL;
	ItemPointerData root_itemptr;

	ItemPointerSetBlockNumber(&root_itemptr, ART_ROOT_NODE_BLKNO);
	ItemPointerSetOffsetNumber(&root_itemptr, ART_ROOT_NODE_ITEM);

	// Root node
	art_node_entry = _get_node_from_iptr(state, &root_itemptr);

	_node_insert_recursive(state, art_node_entry->art_node, artTuple, 0);

	return true;
}


void
_node_release(ArtNodeEntry * node)
{
	if (node->memory_node)
		pfree(node->art_node);

	_art_page_release(dlist_container(ArtPageEntry, node, node->page_entry));
	
	pfree(node);
}

void
_node_release_list(ArtState * state)
{
	dlist_mutable_iter iter;
	ArtNodeEntry * node_entry;

	dlist_foreach_modify(iter, &state->art_nodes)
	{
		node_entry = dlist_container(ArtNodeEntry, node, iter.cur);
		_node_release(node_entry);
	}

	dlist_init(&state->art_nodes);
}

static void
_art_build_callback(Relation index, ItemPointer tid, Datum * values,
					bool *isnull, bool tupleIsAlive, void * _state)
{
	ArtState * state = (ArtState *) _state;
	ArtTuple * art_tuple;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(state->build_ctx);

	art_tuple = _art_form_key(index, tid, values, isnull);

	if (art_tuple->key_len == 0)
	{
		pfree(art_tuple);
		return;
	}

	if (art_tuple->key_len >= ART_PAGE_SIZE)
	{
		elog(WARNING, "Row (%d, %d) column value exceeds size (%d)", 
			 ItemPointerGetBlockNumber(&art_tuple->iptr), ItemPointerGetOffsetNumber(&art_tuple->iptr),
			 art_tuple->key_len);
		pfree(art_tuple);
		return;
	}

	_node_insert(state, art_tuple);

	pfree(art_tuple);

	_node_release_list(state);

	if (CurrentMemoryContext->mem_allocated > build_max_memory)
	{
		ArtPageEntry * node_last_page = NULL;
		ArtPageEntry * leaf_last_page = NULL;
		ArtMetaDataPageOpaqueData art_metadata;
		BlockNumber number_allocated_pages = state->build_state->num_allocated_pages;
		uint64 n_tuples = state->build_state->n_tuples;

		_art_flush_pages(index, &state->pages);
		hash_destroy(state->build_state->page_lookup_hash);

		art_metadata.last_internal_node_blk_num = 
			state->build_state->metadata.last_internal_node_blk_num;

		art_metadata.last_leaf_blk_num =
			state->build_state->metadata.last_leaf_blk_num;

		memcpy(art_metadata.page_cache, 
			   state->build_state->metadata.page_cache, 
			   sizeof(ArtPageCache) * ART_CACHED_PAGES);

		// Reset now complete build memory context
		MemoryContextReset(state->build_ctx);

		state->build_state = (ArtBuildState *) palloc0(sizeof(ArtBuildState));

		_init_state(state);
		_art_init_page_hash(&state->build_state->page_lookup_hash);

		// Restore previous metadata

		state->build_state->metadata.last_internal_node_blk_num = 
			art_metadata.last_internal_node_blk_num;

		state->build_state->metadata.last_leaf_blk_num =
			art_metadata.last_leaf_blk_num;

		memcpy(state->build_state->metadata.page_cache, 
			   art_metadata.page_cache, 
			   sizeof(ArtPageCache) * ART_CACHED_PAGES);

		state->build_state->n_tuples = n_tuples;
		state->build_state->num_allocated_pages = number_allocated_pages;

		/* Adding last internal node page */
		node_last_page = _art_copy_page(state->index, state->build_state->metadata.last_internal_node_blk_num);
		dlist_push_head(&state->pages, &node_last_page->node);
		_art_add_page_hash(state->build_state->page_lookup_hash,
						   node_last_page->blk_num,
						   node_last_page);
		state->node_last_page = &node_last_page->node;

		/* Adding last leaf page */
		leaf_last_page = _art_copy_page(state->index, state->build_state->metadata.last_leaf_blk_num);
		dlist_push_head(&state->pages, &leaf_last_page->node);
		_art_add_page_hash(state->build_state->page_lookup_hash,
						   leaf_last_page->blk_num,
						   leaf_last_page);
		state->leaf_last_page = &leaf_last_page->node;
	}

	state->build_state->n_tuples += 1;

	MemoryContextSwitchTo(old_ctx);
}


IndexBuildResult *
artbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	double reltuples;
	ArtState state;
	ArtNodeHeader * root_art_node;
	ArtNodeEntry * root_node_entry;
	MemoryContext old_ctx;
	ArtPageEntry * node_page_entry;
	ArtPageEntry * leaf_page_entry;
	ArtPageEntry * metadata_page_entry;
	Page metadata_page;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "cannot initialize non-empty art index \"%s\"",
				RelationGetRelationName(index));

	state.build_ctx = AllocSetContextCreate(CurrentMemoryContext,
											"ART build context",
											ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(state.build_ctx);

	state.index = index;
	_init_state(&state);

	state.build_state = (ArtBuildState *) palloc0(sizeof(ArtBuildState));

	_art_init_page_hash(&state.build_state->page_lookup_hash);

	metadata_page = (Page) palloc(BLCKSZ);
	_art_init_metadata_page(metadata_page);
	state.build_state->num_allocated_pages++;

	/* Write metadata page*/
	_art_update_metadata_page(metadata_page, &(state.build_state->metadata));
	smgrextend(RelationGetSmgr(index), MAIN_FORKNUM, ART_METADATA_NODE_BLKNO,
			  (char *) metadata_page, true);
	pfree(metadata_page);

	node_page_entry = _art_new_page(ART_NODE_PAGE);
	node_page_entry->blk_num = ART_ROOT_NODE_BLKNO;
	_art_add_page_hash(state.build_state->page_lookup_hash, ART_ROOT_NODE_BLKNO,
					   node_page_entry);
	state.build_state->metadata.last_internal_node_blk_num = ART_ROOT_NODE_BLKNO;
	dlist_push_tail(&state.pages, &node_page_entry->node);
	state.node_last_page = dlist_tail_node(&state.pages);
	state.build_state->num_allocated_pages++;

	root_art_node = _art_alloc_node(NODE_256);
	root_node_entry = _page_add_node(&state, node_page_entry, root_art_node);
	dlist_delete(dlist_head_node(&state.art_nodes));
	_node_release(root_node_entry);

	leaf_page_entry = _art_new_page(ART_LEAF_PAGE);
	leaf_page_entry->blk_num = ART_LEAF_NODE_BLKNO;
	_art_add_page_hash(state.build_state->page_lookup_hash,ART_LEAF_NODE_BLKNO,
					   leaf_page_entry);
	state.build_state->metadata.last_leaf_blk_num = ART_LEAF_NODE_BLKNO;
	dlist_push_tail(&state.pages, &leaf_page_entry->node);
	state.build_state->num_allocated_pages++;
	state.leaf_last_page = dlist_tail_node(&state.pages);

	MemoryContextSwitchTo(old_ctx);

	reltuples = table_index_build_scan(heap, index, indexInfo, false, true,
									   _art_build_callback,
									   (void *) &state,
									   NULL);

	old_ctx = MemoryContextSwitchTo(state.build_ctx);

	metadata_page_entry = _art_get_metadata_page(index);
	_art_update_metadata_page(metadata_page_entry->page, &state.build_state->metadata);
	metadata_page_entry->dirty = true;
	dlist_push_head(&state.pages, &metadata_page_entry->node);
	_art_page_release(metadata_page_entry);

	/* Flush pages */ 
	_art_flush_pages(state.index, &state.pages);

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(state.build_ctx);

	/*
	 * We didn't write WAL records as we built the index, so if WAL-logging is
	 * required, write all pages to the WAL now.
	 */
	if (RelationNeedsWAL(index))
	{
		log_newpage_range(index, MAIN_FORKNUM,
						  0, RelationGetNumberOfBlocks(index),
						  true);
	}


	result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = state.build_state->n_tuples;

	return result;
}


void
artbuildempty(Relation index)
{
	Buffer metadata_buffer, root_buffer, leaf_buffer;
	ArtNodeHeader * init_art_node;

	metadata_buffer = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(metadata_buffer, BUFFER_LOCK_EXCLUSIVE);

	root_buffer = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(root_buffer, BUFFER_LOCK_EXCLUSIVE);

	leaf_buffer = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(leaf_buffer, BUFFER_LOCK_EXCLUSIVE);

	// Root Node
	init_art_node = _art_alloc_node(NODE_256);

	START_CRIT_SECTION();

	_art_init_metadata_page(BufferGetPage(metadata_buffer));
	_art_init_data_page(BufferGetPage(root_buffer), ART_NODE_PAGE);
	_art_init_data_page(BufferGetPage(leaf_buffer), ART_LEAF_PAGE);

	PageAddItem(BufferGetPage(root_buffer),
				(Item) init_art_node, _art_node_size(init_art_node),
				0, false, false);

	MarkBufferDirty(metadata_buffer);
	log_newpage_buffer(metadata_buffer, false);
	MarkBufferDirty(root_buffer);
	log_newpage_buffer(root_buffer, false);
	MarkBufferDirty(leaf_buffer);
	log_newpage_buffer(leaf_buffer, false);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(metadata_buffer);
	UnlockReleaseBuffer(root_buffer);
	UnlockReleaseBuffer(leaf_buffer);

	pfree(init_art_node);
}

bool
artinsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  IndexInfo *indexInfo)
{
	ArtState  * state = (ArtState *) indexInfo->ii_AmCache;
	ArtTuple * art_tuple;
	MemoryContext old_ctx;

	if (state == NULL)
	{
		old_ctx = MemoryContextSwitchTo(indexInfo->ii_Context);
		state = (ArtState *) palloc0(sizeof(ArtState));

		state->index = index;

		state->build_ctx = AllocSetContextCreate(CurrentMemoryContext,
												 "ART build temporary context",
												 ALLOCSET_DEFAULT_SIZES);

		indexInfo->ii_AmCache = (void *) state;

		MemoryContextSwitchTo(old_ctx);
	}

	/* Work in temp context, and reset it after each tuple */
	old_ctx = MemoryContextSwitchTo(state->build_ctx);

	_init_state(state);

	art_tuple = _art_form_key(index, ht_ctid, values, isnull);

	if (art_tuple->key_len == 0)
	{
		return false;
	}

	if (art_tuple->key_len >= ART_PAGE_SIZE)
	{
		elog(WARNING, "Row (%d, %d) column value exceeds size (%d)", 
			 ItemPointerGetBlockNumber(&art_tuple->iptr), ItemPointerGetOffsetNumber(&art_tuple->iptr),
			 art_tuple->key_len);
		pfree(art_tuple);
		return false;
	}

	_node_insert(state, art_tuple);

	_node_release_list(state);

	pfree(art_tuple);

	MemoryContextSwitchTo(old_ctx);

	MemoryContextReset(state->build_ctx);

	return true;
}
