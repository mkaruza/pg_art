#include "postgres.h"
#include "utils/fmgrprotos.h"

#include "storage/bufmgr.h"

#include "art.h"

#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
    #include <emmintrin.h>
#endif
#endif


/**
 * Allocate ART node
 */
ArtNodeHeader *
_art_alloc_node(uint8 type)
{
	ArtNodeHeader * n;

	switch (type)
	{
	case NODE_4:
		n = (ArtNodeHeader*) palloc0(sizeof(ArtNode4));
		break;
	case NODE_16:
		n = (ArtNodeHeader*) palloc0(sizeof(ArtNode16));
		break;
	case NODE_48:
		n = (ArtNodeHeader*) palloc0(sizeof(ArtNode48));
		break;
	case NODE_256:
		n = (ArtNodeHeader*) palloc0(sizeof(ArtNode256));
		break;
	default:
		elog(ERROR, "Invalid ART NODE");
	}

	n->node_type = type;
	return n;
}


/**
 * Get ART node size
 */
Size
_art_node_size(ArtNodeHeader * node)
{
	switch (node->node_type)
	{
	case NODE_LEAF:
		{
			ArtNodeLeaf * leaf = (ArtNodeLeaf *) node;
			return sizeof(ArtNodeLeaf) + 
					leaf->key_len + 
					leaf->num_items * sizeof(ItemPointerData);
		}
	case NODE_4:
		return sizeof(ArtNode4);
	case NODE_16:
		return sizeof(ArtNode16);
	case NODE_48:
		return sizeof(ArtNode48);
	case NODE_256:
		return sizeof(ArtNode256);
	}

	return 0;
}


void
_art_copy_header(ArtNodeHeader *dest, ArtNodeHeader *src)
{
	dest->num_children = src->num_children;
	dest->prefix_key_len = src->prefix_key_len;
	memcpy(dest->prefix, src->prefix, Min(MAX_PREFIX_KEY_LEN, src->prefix_key_len));
	ItemPointerCopy(&dest->parent_iptr, &src->parent_iptr);
}

void
_art_add_queue_itemptr(pairingheap * queue, ItemPointer iptr, bool compare)
{
	ArtQueueItemPointer * item = palloc0(sizeof(ArtQueueItemPointer));
	ItemPointerCopy(iptr, &item->iptr);
	item->compare = compare;
	pairingheap_add(queue, &item->ph_node);
}

ItemPointer
_art_find_child_equal(ArtNodeHeader * n, uint8 key)
{
	int i;
	switch (n->node_type)
	{
		case NODE_4:
		{
			ArtNode4 *node4 = (ArtNode4 *) n;
			for (i = 0 ; i < n->num_children; i++)
			{
				if (((unsigned char*)node4->keys)[i] == key)
					return &node4->children[i];
			}
		}
		break;

		case NODE_16:
		{
			ArtNode16 *node16 = (ArtNode16 *) n;
			int mask, bitfield;

// support non-86 architectures
#ifdef __i386__
			// Compare the key to all 16 stored keys
			__m128i cmp;
			cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
					_mm_loadu_si128((__m128i*)p.p2->keys));
			
			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
			// Compare the key to all 16 stored keys
			__m128i cmp;
			cmp = _mm_cmpeq_epi8(_mm_set1_epi8(key),
					_mm_loadu_si128((__m128i*)node16->keys));

			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield = _mm_movemask_epi8(cmp) & mask;
#else
			// Compare the key to all 16 stored keys
			bitfield = 0;
			for (i = 0; i < 16; ++i) {
				if (p.p2->keys[i] == c)
					bitfield |= (1 << i);
			}

			// Use a mask to ignore children that don't exist
			mask = (1 << n->num_children) - 1;
			bitfield &= mask;
#endif
#endif
			/*
			 * If we have a match (any bit set) then we can
			 * return the pointer match using ctz to get
			 * the index.
			 */
			if (bitfield)
				return &node16->children[__builtin_ctz(bitfield)];
		}
		break;

		case NODE_48:
		{
			ArtNode48 *node48 = (ArtNode48 *) n;
			if (node48->keys[key])
				return &node48->children[node48->keys[key]-1];
		}
		break;

		case NODE_256:
		{
			ArtNode256 *node256 = (ArtNode256 *) n;
			if (ItemPointerIsValid(&node256->children[key]))
				return &node256->children[key];

		}
		break;
	}

	return NULL;
}


void
_art_find_child_range(ArtNodeHeader * n, uint8 key,
					  StrategyNumber skStrategy,
					  pairingheap * childrenQueue,
					  bool compare)
{
	int i;
	switch (n->node_type)
	{
		case NODE_4:
		{
			ArtNode4 *node4 = (ArtNode4 *) n;
			for (i = 0 ; i < n->num_children; i++)
			{
				uint8 child_key = node4->keys[i];
				int32 cmp = 0;

				if (!compare)
				{
					_art_add_queue_itemptr(childrenQueue, &node4->children[i], false);
					continue;
				}

 				cmp = _art_compare_key(child_key, key);

				if (skStrategy == BTLessStrategyNumber ||
					skStrategy == BTLessEqualStrategyNumber)
				{
					if (cmp < 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node4->children[i], false);
					}
					else if (cmp == 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node4->children[i], true);
					}
					else
					{
						break;
					}
				}

				else if (skStrategy == BTGreaterStrategyNumber ||
						 skStrategy == BTGreaterEqualStrategyNumber)
				{
					if (cmp > 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node4->children[i], false);
					}
					else if (cmp == 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node4->children[i], true);
					}
					else
					{
						break;
					}
				}
			}
		}
		break;

		case NODE_16:
		{
			ArtNode16 *node16 = (ArtNode16 *) n;

			for (i = 0 ; i < n->num_children; i++)
			{
				uint8 child_key = node16->keys[i];
				int32 cmp = 0;

				if (!compare)
				{
					_art_add_queue_itemptr(childrenQueue, &node16->children[i], false);
					continue;
				}

				cmp = _art_compare_key(child_key, key);

				if (skStrategy == BTLessStrategyNumber ||
					skStrategy == BTLessEqualStrategyNumber)
				{
					if (cmp < 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node16->children[i], false);
					}
					else if (cmp == 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node16->children[i], true);
					}
					else
					{
						break;
					}
				}

				else if (skStrategy == BTGreaterStrategyNumber ||
						 skStrategy == BTGreaterEqualStrategyNumber)
				{
					if (cmp > 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node16->children[i], false);
					}
					else if (cmp == 0)
					{
						_art_add_queue_itemptr(childrenQueue, &node16->children[i], true);
					}
					else
					{
						break;
					}
				}
			}
		}
		break;

		case NODE_48:
		{
			ArtNode48 *node48 = (ArtNode48 *) n;
			uint32 start = 0;
			uint32 end = 256;

			if (compare)
			{
				switch (skStrategy)
				{
					case BTLessStrategyNumber:
					case BTLessEqualStrategyNumber:
					{
						end = key + 1;
					}
					break;

					case BTGreaterStrategyNumber:
					case BTGreaterEqualStrategyNumber:
					{
						start = key;
					}
					break;
				}
			}

			for (i = start; i < end; i++)
			{
				if (node48->keys[i])
					_art_add_queue_itemptr(childrenQueue, &node48->children[node48->keys[i]-1],
										   compare ? i == key : false);
			}
		}
		break;

		case NODE_256:
		{
			ArtNode256 *node256 = (ArtNode256 *) n;
			uint32 start = 0;
			uint32 end = 256;

			if (compare)
			{
				switch (skStrategy)
				{
					case BTLessStrategyNumber:
					case BTLessEqualStrategyNumber:
					{
						end = key + 1;
					}
					break;

					case BTGreaterStrategyNumber:
					case BTGreaterEqualStrategyNumber:
					{
						start = key;
					}
					break;
				}
			}

			for (i = start; i < end; i++)
			{
				if (ItemPointerIsValid(&node256->children[i]))
					_art_add_queue_itemptr(childrenQueue, &node256->children[i],
										   compare ? i == key : false);
			}
		}
		break;
	}
}

ArtNodeHeader *
_art_get_node_from_iptr(Relation index, ItemPointer iptr,
					    Buffer * nodeBuffer, int bufferLockMode)
{
	Page page;
	Offset off;

	*nodeBuffer = ReadBuffer(index, ItemPointerGetBlockNumber(iptr));
	LockBuffer(*nodeBuffer, bufferLockMode);
	page = BufferGetPage(*nodeBuffer);
	off = ItemPointerGetOffsetNumber(iptr);

	return (ArtNodeHeader *) 
		PageGetItem(page, PageGetItemId(page, off));
}

ArtNodeLeaf *
_art_minimum_leaf(Relation index, ArtNodeHeader * n,
				  HTAB * pageHashLookup, dlist_head * pageListHead)
{
	int idx = 0;
	ItemPointer iptr = NULL;
	ArtNodeHeader * min_child_node;
	Buffer next_node_buffer;

	switch (n->node_type)
	{
		case NODE_LEAF:
		{
			return (ArtNodeLeaf *) n;
		}

		case NODE_4:
		{
			iptr = &(((ArtNode4*)n)->children[0]);
			break;
		}

		case NODE_16:
		{
			iptr = &(((ArtNode16*)n)->children[0]);
			break;
		}

		case NODE_48:
		{
			while (!((const ArtNode48*)n)->keys[idx])
				idx++;

			idx = ((const ArtNode48*)n)->keys[idx] - 1;
			iptr = &(((ArtNode48*)n)->children[idx]);
			break;
		}

		case NODE_256:
		{
			while (!ItemPointerIsValid(&((const ArtNode256 *)n)->children[idx])) 
				idx++;

			iptr = &(((ArtNode256*)n)->children[idx]);
			break;
		}
	}

	if (pageHashLookup)
	{
		BlockNumber node_block_number = ItemPointerGetBlockNumber(iptr);
		BlockNumber node_offset = ItemPointerGetOffsetNumber(iptr);

		ArtPageEntry * node_page = 
			_art_get_page_hash(pageHashLookup, node_block_number);

		if (!node_page)
		{
			node_page = _art_copy_page(index, node_block_number);
			dlist_push_head(pageListHead, &node_page->node);
			_art_add_page_hash(pageHashLookup,
							   node_block_number,
							   node_page);
		}

		min_child_node =
			(ArtNodeHeader *) PageGetItem(node_page->page,
									PageGetItemId(node_page->page, node_offset));
	}
	else
	{
		min_child_node = 
			_art_get_node_from_iptr(index, iptr, &next_node_buffer, BUFFER_LOCK_SHARE);
	}

	return _art_minimum_leaf(index, min_child_node, pageHashLookup, pageListHead);
}


int
_art_leaf_matches(const ArtNodeLeaf * n, const uint8 * key, uint16 key_len)
{
	if (n->key_len != key_len )
		return 1;

	return memcmp(n->data, key, key_len);
}

int
_art_longest_common_prefix(ArtNodeLeaf *l1, ArtNodeLeaf *l2, int depth)
{
	int max_cmp = Min(l1->key_len, l2->key_len) - depth;
	int idx;

	for (idx = 0; idx < max_cmp; idx++)
	{
		if (l1->data[depth + idx] != l2->data[depth + idx])
			return idx;
	}

	return idx;
}


int
_art_prefix_mismatch(Relation index, ArtNodeHeader *node,
					 HTAB * pageHashLookup, dlist_head * pageHeadList,
					 const uint8 *key, uint32 key_len, int depth)
{

	int max_cmp = Min(Min(MAX_PREFIX_KEY_LEN, node->prefix_key_len), key_len - depth);
	int idx;
	for (idx = 0; idx < max_cmp; idx++)
	{
		if (node->prefix[idx] != key[depth+idx])
			return idx;
	}

	if (node->prefix_key_len > MAX_PREFIX_KEY_LEN)
	{
		ArtNodeLeaf *l = _art_minimum_leaf(index, node, pageHashLookup, pageHeadList);
		max_cmp = Min(l->key_len, key_len)- depth;
		for (; idx < max_cmp; idx++)
		{
			if (l->data[idx+depth] != key[depth+idx])
				return idx;
		}
	}
	return idx;
}


int
_art_check_prefix(const ArtNodeHeader *n, const uint8 *key, int key_len, int depth)
{
	int max_cmp = Min(Min(n->prefix_key_len, MAX_PREFIX_KEY_LEN), key_len - depth);
	int idx;

	for (idx = 0; idx < max_cmp; idx++)
	{
		if (n->prefix[idx] != key[depth+idx])
			return idx;
	}

	return idx;
}
