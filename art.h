/*-------------------------------------------------------------------------
 *
 * art.h
 *    Header for art index.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _ART_H_
#define _ART_H_

#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"
#include "utils/hsearch.h"

/* GUC */
extern double page_leaf_insert_treshold;
extern bool update_parent_iptr;
extern int build_max_memory;

/* ART page information */

#define ART_METADATA_NODE_BLKNO (0)
#define ART_ROOT_NODE_BLKNO (1)
#define ART_LEAF_NODE_BLKNO (2)
#define ART_ROOT_NODE_ITEM (1)

#define ART_NODE_PAGE (1 << 0)
#define ART_LEAF_PAGE (1 << 1)

typedef struct ArtDataPageOpaqueData
{
	uint8 page_flags;			/* page flags */
	uint16 n_total; 			/* total number of items on page */
	uint16 n_deleted;			/* number of deleted items */
	uint16 deleted_item_size;	/* size of deleted items */
	BlockNumber right_link;		/* next page if any */
} ArtDataPageOpaqueData;

typedef ArtDataPageOpaqueData *ArtDataPageOpaque;

typedef struct ArtPageCache
{
	BlockNumber blk_num; /* block number, or InvalidBlockNumber */
	int	free_space; /* page's free space (could be obsolete!) */
} ArtPageCache;

/* Note: indexes in cachedPage[] match flag assignments for SpGistGetBuffer */
#define ART_CACHED_PAGES 8

typedef struct ArtMetaDataPageOpaqueData
{
	ArtPageCache page_cache[ART_CACHED_PAGES];
	BlockNumber last_internal_node_blk_num; /* Last internal node block number */
	BlockNumber last_leaf_blk_num; 			/* Last leaf block number */
} ArtMetaDataPageOpaqueData;

typedef ArtMetaDataPageOpaqueData *ArtMetaDataPageOpaque;

typedef struct ArtItemList
{
	OffsetNumber num;						/* number of items */
	ItemPointerData next_item_iptr;			/* if item list doesn't fit item pointer 
												next part of list */
	ItemPointerData last_item_iptr;			/* Point to last iitem list */
	ItemPointerData iptr[FLEXIBLE_ARRAY_MEMBER];
} ArtItemList;

typedef struct ArtTuple
{
	uint32_t key_len;
	uint8_t *key;
	ItemPointerData iptr;
} ArtTuple;

/* ART nodes */

typedef enum ArtNodeType {
	NODE_LEAF = 0,
	NODE_4,
	NODE_16,
	NODE_48,
	NODE_256,
} ArtNodeType;

#define MAX_PREFIX_KEY_LEN 8

typedef struct ArtNodeHeader
{
	uint8 node_type;				// Should be first header member to match leaf structure
	ItemPointerData parent_iptr;	// Should be second header member to match leaf structure
	uint8 num_children;
	uint8 prefix_key_len;
	uint8 prefix[MAX_PREFIX_KEY_LEN];
} ArtNodeHeader;

typedef struct ArtNodeLeaf
{
	uint8 node_type;
	ItemPointerData parent_iptr;
	ItemPointerData next_leaf_iptr;
	ItemPointerData last_leaf_iptr;
	uint16 key_len;
	uint16 num_items;
	uint8 data[FLEXIBLE_ARRAY_MEMBER];
} ArtNodeLeaf;

typedef struct ArtNode4
{
	ArtNodeHeader node;
	uint8 keys[4];
	ItemPointerData children[4];
} ArtNode4;

typedef struct ArtNode16
{
	ArtNodeHeader node;
	uint8 keys[16];
	ItemPointerData children[16];
} ArtNode16;

typedef struct ArtNode48
{
	ArtNodeHeader node;
	uint8 keys[256];
	ItemPointerData children[48];
} ArtNode48;

typedef struct ArtNode256 
{
	ArtNodeHeader node;
	ItemPointerData children[256];
} ArtNode256;


typedef struct ArtPageEntry
{
	dlist_node node;
	BlockNumber blk_num;
	Buffer buffer;
	Page page;
	uint8 ref_count;			/* number of distinct nodes that point to same page.
								 * Not used during in-memory build */
	bool dirty;					/* page dirty, keep and flus */
	bool is_copy;				/* copy of page */
} ArtPageEntry;


typedef struct ArtQueueItemPointer
{
	pairingheap_node ph_node;
	ItemPointerData iptr;
	bool compare;	// used only during scan fetching
} ArtQueueItemPointer;

/* art.c */
extern void _PG_init(void);
extern ArtTuple * _art_form_key(Relation index, ItemPointer iptr,
								Datum *values, bool *isnull);
extern int32 _art_compare_key(uint8 a, uint8 b);


/* art_insert.c */

extern void _art_init_page_hash(HTAB ** pageHashLookup);
extern void _art_add_page_hash(HTAB * pageHashLookup, BlockNumber blockNumber, ArtPageEntry * pageEntry);
extern ArtPageEntry * _art_get_page_hash(HTAB * pageHashLookup, BlockNumber blockNumber);

/* art_utils.c */
extern ArtNodeHeader * _art_alloc_node(uint8 type);
extern Size _art_node_size(ArtNodeHeader * node);
extern void _art_add_queue_itemptr(pairingheap * queue, ItemPointer iptr, bool checkRange);
extern ItemPointer _art_find_child_equal(ArtNodeHeader * n, uint8 key);
extern void _art_find_child_range(ArtNodeHeader * n, uint8 key,
								  StrategyNumber skStrategy,
								  pairingheap * childrenQueue,
								  bool checkRange);
extern ArtNodeLeaf * _art_minimum_leaf(Relation index, ArtNodeHeader * n, 
				  					   HTAB * pageHashLookup, dlist_head * pagListHead);
extern ArtNodeHeader * _art_get_node_from_iptr(Relation index, ItemPointer iptr, 
											   Buffer * nodeBuffer, int bufferLockMode);
extern void _art_copy_header(ArtNodeHeader *dest, ArtNodeHeader *src);
extern int _art_leaf_matches(const ArtNodeLeaf * n, const uint8 * key, uint16 key_len);
extern int _art_longest_common_prefix(ArtNodeLeaf *l1, ArtNodeLeaf *l2, int depth);
extern int _art_prefix_mismatch(Relation index, ArtNodeHeader *node,
								HTAB * pageHashLookup, dlist_head * pageHeadList,
								const uint8 *key, uint32 key_len, int depth);
extern int _art_check_prefix(const ArtNodeHeader *n, const uint8 *key, int key_len,
							 int depth);

/* art_pageops.c */
extern void _art_init_data_page(Page page, uint8 flags);
extern void _art_init_metadata_page(Page page);
extern ArtPageEntry * _art_get_metadata_page(Relation index);
extern void _art_update_metadata_page(Page page, ArtMetaDataPageOpaque metadata);
extern void _art_page_release(ArtPageEntry * pageEntry);
extern ArtPageEntry * _art_new_page(uint8 flags);
extern ArtPageEntry * _art_get_buffer(Relation index, uint8 flags);
extern dlist_node * _art_load_page(Relation index, dlist_head * pageListHead,
									 BlockNumber blockNum, int bufferLockMode, 
									 bool * isNewPageEntry);
extern ArtPageEntry * _art_copy_page(Relation index, BlockNumber blockNum);
extern void _art_flush_pages(Relation index, dlist_head * pageListHead);

/* index access method interface functions */
extern IndexBuildResult *artbuild(Relation heap, Relation index,
								  struct IndexInfo *indexInfo);
extern void artbuildempty(Relation index);
extern bool artinsert(Relation index, Datum *values, bool *isnull,
					  ItemPointer ht_ctid, Relation heapRel,
					  IndexUniqueCheck checkUnique,
					  bool indexUnchanged,
					  struct IndexInfo *indexInfo);
extern IndexBulkDeleteResult *artbulkdelete(IndexVacuumInfo *info,
											IndexBulkDeleteResult *stats,
											IndexBulkDeleteCallback callback,
											void *callback_state);
extern IndexBulkDeleteResult *artvacuumcleanup(IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats);
extern void artcostestimate(PlannerInfo *root, IndexPath *path,
							double loop_count, Cost *indexStartupCost,
							Cost *indexTotalCost, Selectivity *indexSelectivity,
							double *indexCorrelation, double *indexPages);
extern bytea * artoptions(Datum reloptions, bool validate);
extern bool artvalidate(Oid opclassoid);
extern IndexScanDesc artbeginscan(Relation r, int nkeys, int norderbys);
extern void artrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
					  ScanKey orderbys, int norderbys);
extern bool artgettuple(IndexScanDesc scan, ScanDirection dir);
extern void artendscan(IndexScanDesc scan);

#endif
