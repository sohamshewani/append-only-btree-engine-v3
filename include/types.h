#ifndef APPEND_ONLY_BTREE_TYPES_H
#define APPEND_ONLY_BTREE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>

/* ============================================================================
 * CONSTANTS & SYSTEM LIMITS
 * ============================================================================ */
#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024
#define INVALID_PAGE_ID 0xFFFFFFFFFFFFFFFFULL
#define INVALID_LSN 0ULL
#define INVALID_TXN_ID 0ULL
#define BUFFER_POOL_DEFAULT_FRAMES 1024

/* ============================================================================
 * BASIC IDENTIFIERS & ENUMERATIONS
 * ============================================================================ */
typedef uint64_t PageID;
typedef uint64_t LSN;
typedef uint64_t TxnID;

typedef enum {
    PAGE_TYPE_EMPTY = 0,
    PAGE_TYPE_BTREE_INTERNAL = 1,
    PAGE_TYPE_BTREE_LEAF = 2,
    PAGE_TYPE_OVERFLOW = 3
} PageType;

typedef enum {
    WAL_BEGIN = 1,
    WAL_INSERT = 2,
    WAL_UPDATE = 3,
    WAL_DELETE = 4,
    WAL_COMMIT = 5,
    WAL_ABORT = 6,
    WAL_CHECKPOINT = 7,
    WAL_CLR = 8
} WALRecordType;

typedef enum {
    TXN_ACTIVE = 0,
    TXN_COMMITTED = 1,
    TXN_ABORTED = 2
} TxnStatus;

/* ============================================================================
 * 1. DATA STRUCTURES & MEMORY LAYOUT (4KB PAGES)
 * ============================================================================ */
typedef struct {
    LSN lsn;                     /* Log Sequence Number of last modification */
    uint32_t flags;              /* Page flags (PageType, dirty, etc.) */
    uint16_t item_count;         /* Number of keys/slots in the page */
    uint16_t free_space_offset;  /* Byte offset to the start of free space */
} __attribute__((packed)) PageHeader;

typedef struct {
    uint16_t offset;
    uint16_t length;
} __attribute__((packed)) SlottedItem;

typedef struct {
    PageHeader header;
    /* Payload contains slotted page data or B-Tree internal/leaf arrays.
     * Total size strictly bounded to PAGE_SIZE (4096 bytes). */
    uint8_t data[PAGE_SIZE - sizeof(PageHeader)];
} __attribute__((packed)) Page;

typedef struct {
    PageID page_id;
    int32_t frame_id;
    int32_t pin_count;
    bool is_dirty;
    bool reference_bit;          /* Used for CLOCK / LRU-K eviction */
    pthread_rwlock_t rwlock;     /* Fine-grained reader-writer lock per frame */
    pthread_spinlock_t spinlock; /* Spinlock for latching metadata changes */
} BufferDescriptor;

typedef struct {
    uint32_t key_len;
    uint32_t val_len;
    PageID child_page_id;        /* Used for internal nodes; 0 for leaves */
    uint8_t data[MAX_KEY_SIZE + MAX_VAL_SIZE];
} __attribute__((packed)) BTreeNodeItem;

/* ============================================================================
 * WRITE-AHEAD LOGGING (WAL) STRUCTURES
 * ============================================================================ */
typedef struct {
    uint32_t crc32;              /* Checksum of the record */
    LSN lsn;                     /* Current record LSN */
    LSN prev_lsn;                /* Chain pointer for previous LSN of same txn */
    TxnID txn_id;                /* Transaction ID */
    WALRecordType type;          /* Record type */
    PageID page_id;              /* Target page ID */
    uint32_t payload_len;        /* Length of variable-length payload delta */
    /* Variable-length payload follows immediately in the log stream:
     * [Undo Data] [Redo Data] or Key/Value deltas. */
} __attribute__((packed)) WALRecordHeader;

/* ============================================================================
 * 2. CONCURRENCY PRIMITIVES & BUFFER POOL MANAGER
 * ============================================================================ */
typedef struct {
    Page *frames;                /* Array of PAGE_SIZE blocks */
    BufferDescriptor *descriptors;/* Metadata descriptors for each frame */
    uint32_t pool_size;          /* Total number of frames */
    uint32_t clock_hand;         /* Current hand position for CLOCK eviction */
    pthread_mutex_t mutex;       /* Global buffer pool mutex */
} BufferPoolManager;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    LSN flushed_lsn;             /* Maximum LSN safely persisted to disk */
    int log_fd;                  /* File descriptor for WAL file */
} WALManager;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool shutdown;
    uint32_t flush_interval_ms;
    BufferPoolManager *bpm;
    WALManager *wm;
} BackgroundFlusher;

/* ============================================================================
 * 4. RECOVERY (ARIES-Lite) & TRANSACTION CONTEXT
 * ============================================================================ */
typedef struct {
    TxnID txn_id;
    TxnStatus status;
    LSN last_lsn;
    LSN first_lsn;
} TransactionTableEntry;

typedef struct {
    PageID page_id;
    LSN rec_lsn;                 /* First LSN that dirtied this page */
} DirtyPageTableEntry;

typedef struct {
    TransactionTableEntry *txn_table;
    uint32_t txn_count;
    DirtyPageTableEntry *dirty_pages;
    uint32_t dirty_page_count;
    LSN checkpoint_lsn;
} MasterRecord;

#endif /* APPEND_ONLY_BTREE_TYPES_H */