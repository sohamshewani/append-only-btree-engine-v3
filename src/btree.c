#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>

#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024
#define BTREE_FANOUT 64
#define BUFFER_POOL_SIZE 128

typedef uint64_t PageID;
typedef uint64_t LSN;
typedef uint64_t TxnID;

enum PageFlags {
    PAGE_FLAG_NONE = 0,
    PAGE_FLAG_LEAF = 1 << 0,
    PAGE_FLAG_INTERNAL = 1 << 1,
    PAGE_FLAG_OVERFLOW = 1 << 2
};

typedef struct __attribute__((__packed__)) PageHeader {
    LSN lsn;
    uint32_t flags;
    uint16_t item_count;
    uint16_t free_space_offset;
    PageID next_page_id;
    PageID prev_page_id;
} PageHeader;

typedef struct __attribute__((__packed__)) CellHeader {
    uint16_t offset;
    uint16_t key_len;
    uint16_t val_len;
    uint8_t flags;
} CellHeader;

typedef struct __attribute__((__packed__)) BTreeCell {
    CellHeader header;
    uint8_t data[];
} BTreeCell;

typedef struct Page {
    PageHeader header;
    uint8_t data[PAGE_SIZE - sizeof(PageHeader)];
} Page;

typedef struct BufferDescriptor {
    PageID page_id;
    int32_t pin_count;
    bool is_dirty;
    bool ref_bit;
    LSN page_lsn;
    pthread_rwlock_t rwlock;
    pthread_spinlock_t frame_lock;
    int frame_id;
} BufferDescriptor;

typedef struct BufferPoolManager {
    Page *frames;
    BufferDescriptor *descriptors;
    int pool_size;
    int clock_hand;
    int db_fd;
    pthread_mutex_t bpool_mutex;
} BufferPoolManager;

typedef enum WALRecordType {
    WAL_BEGIN = 1,
    WAL_INSERT,
    WAL_UPDATE,
    WAL_COMMIT,
    WAL_ABORT,
    WAL_CHECKPOINT,
    WAL_CLR
} WALRecordType;

typedef struct __attribute__((__packed__)) WALHeader {
    uint32_t crc32;
    LSN lsn;
    TxnID tx_id;
    uint32_t type;
    PageID page_id;
    uint32_t payload_len;
} WALHeader;

typedef struct WALRecord {
    WALHeader header;
    uint8_t payload[];
} WALRecord;

typedef struct WALManager {
    int wal_fd;
    LSN current_lsn;
    LSN flushed_lsn;
    pthread_mutex_t wal_mutex;
} WALManager;

typedef struct BTreeEngine {
    BufferPoolManager bpm;
    WALManager wm;
    PageID root_page_id;
    pthread_rwlock_t root_lock;
    bool is_open;
} BTreeEngine;

static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return ~crc;
}

static int bpm_init(BufferPoolManager *bpm, int fd, int pool_size) {
    bpm->pool_size = pool_size;
    bpm->db_fd = fd;
    bpm->clock_hand = 0;
    if (pthread_mutex_init(&bpm->bpool_mutex, NULL) != 0) {
        return -1;
    }
    bpm->frames = (Page *)aligned_alloc(PAGE_SIZE, (size_t)pool_size * sizeof(Page));
    if (!bpm->frames) {
        pthread_mutex_destroy(&bpm->bpool_mutex);
        return -1;
    }
    bpm->descriptors = (BufferDescriptor *)malloc((size_t)pool_size * sizeof(BufferDescriptor));
    if (!bpm->descriptors) {
        free(bpm->frames);
        pthread_mutex_destroy(&bpm->bpool_mutex);
        return -1;
    }
    for (int i = 0; i < pool_size; i++) {
        bpm->descriptors[i].page_id = (PageID)-1;
        bpm->descriptors[i].pin_count = 0;
        bpm->descriptors[i].is_dirty = false;
        bpm->descriptors[i].ref_bit = false;
        bpm->descriptors[i].page_lsn = 0;
        bpm->descriptors[i].frame_id = i;
        pthread_rwlock_init(&bpm->descriptors[i].rwlock, NULL);
        pthread_spin_init(&bpm->descriptors[i].frame_lock, PTHREAD_PROCESS_PRIVATE);
        memset(&bpm->frames[i], 0, PAGE_SIZE);
    }
    return 0;
}

static void bpm_destroy(BufferPoolManager *bpm) {
    for (int i = 0; i < bpm->pool_size; i++) {
        pthread_rwlock_destroy(&bpm->descriptors[i].rwlock);
        pthread_spin_destroy(&bpm->descriptors[i].frame_lock);
    }
    free(bpm->frames);
    free(bpm->descriptors);
    pthread_mutex_destroy(&bpm->bpool_mutex);
}

static int wal_init(WALManager *wm, const char *wal_path) {
    wm->wal_fd = open(wal_path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (wm->wal_fd < 0) {
        return -1;
    }
    wm->current_lsn = 1;
    wm->flushed_lsn = 0;
    if (pthread_mutex_init(&wm->wal_mutex, NULL) != 0) {
        close(wm->wal_fd);
        return -1;
    }
    return 0;
}

static void wal_destroy(WALManager *wm) {
    pthread_mutex_lock(&wm->wal_mutex);
    fsync(wm->wal_fd);
    close(wm->wal_fd);
    pthread_mutex_unlock(&wm->wal_mutex);
    pthread_mutex_destroy(&wm->wal_mutex);
}

static LSN wal_append(WALManager *wm, TxnID tx_id, WALRecordType type, PageID page_id, const uint8_t *payload, uint32_t payload_len) {
    pthread_mutex_lock(&wm->wal_mutex);
    LSN lsn = wm->current_lsn++;
    
    size_t total_size = sizeof(WALHeader) + payload_len;
    WALRecord *record = (WALRecord *)malloc(total_size);
    if (!record) {
        pthread_mutex_unlock(&wm->wal_mutex);
        return 0;
    }
    
    record->header.lsn = lsn;
    record->header.tx_id = tx_id;
    record->header.type = (uint32_t)type;
    record->header.page_id = page_id;
    record->header.payload_len = payload_len;
    
    if (payload && payload_len > 0) {
        memcpy(record->payload, payload, payload_len);
    }
    
    record->header.crc32 = calculate_crc32((uint8_t *)&record->header.lsn, total_size - offsetof(WALHeader, lsn));
    
    ssize_t written = write(wm->wal_fd, record, total_size);
    free(record);
    
    pthread_mutex_unlock(&wm->wal_mutex);
    if (written != (ssize_t)total_size) {
        return 0;
    }
    return lsn;
}

static void wal_flush_up_to(WALManager *wm, LSN lsn) {
    pthread_mutex_lock(&wm->wal_mutex);
    if (lsn > wm->flushed_lsn) {
        fsync(wm->wal_fd);
        wm->flushed_lsn = lsn;
    }
    pthread_mutex_unlock(&wm->wal_mutex);
}

static int bpm_evict(BufferPoolManager *bpm) {
    int scanned = 0;
    while (scanned < bpm->pool_size * 2) {
        int idx = bpm->clock_hand;
        bpm->clock_hand = (bpm->clock_hand + 1) % bpm->pool_size;
        
        pthread_spin_lock(&bpm->descriptors[idx].frame_lock);
        if (bpm->descriptors[idx].pin_count > 0) {
            pthread_spin_unlock(&bpm->descriptors[idx].frame_lock);
            scanned++;
            continue;
        }
        
        if (bpm->descriptors[idx].ref_bit) {
            bpm->descriptors[idx].ref_bit = false;
            pthread_spin_unlock(&bpm->descriptors[idx].frame_lock);
            scanned++;
            continue;
        }
        
        if (bpm->descriptors[idx].is_dirty) {
            pthread_spin_unlock(&bpm->descriptors[idx].frame_lock);
            
            pthread_rwlock_wrlock(&bpm->descriptors[idx].rwlock);
            if (bpm->descriptors[idx].pin_count == 0) {
                off_t offset = (off_t)bpm->descriptors[idx].page_id * PAGE_SIZE;
                pwrite(bpm->db_fd, &bpm->frames[idx], PAGE_SIZE, offset);
                bpm->descriptors[idx].is_dirty = false;
            }
            pthread_rwlock_unlock(&bpm->descriptors[idx].rwlock);
            
            pthread_spin_lock(&bpm->descriptors[idx].frame_lock);
            if (bpm->descriptors[idx].pin_count == 0 && !bpm->descriptors[idx].is_dirty) {
                int frame_id = bpm->descriptors[idx].frame_id;
                pthread_spin_unlock(&bpm->descriptors[idx].frame_lock);
                return frame_id;
            }
        } else {
            int frame_id = bpm->descriptors[idx].frame_id;
            pthread_spin_unlock(&bpm->descriptors[idx].frame_lock);
            return frame_id;
        }
        scanned++;
    }
    return -1;
}

static BufferDescriptor* bpm_fetch_page(BufferPoolManager *bpm, PageID page_id) {
    pthread_mutex_lock(&bpm->bpool_mutex);
    
    for (int i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].frame_lock);
        if (bpm->descriptors[i].page_id == page_id) {
            bpm->descriptors[i].pin_count++;
            bpm->descriptors[i].ref_bit = true;
            pthread_spin_unlock(&bpm->descriptors[i].frame_lock);
            pthread_mutex_unlock(&bpm->bpool_mutex);
            return &bpm->descriptors[i];
        }
        pthread_spin_unlock(&bpm->descriptors[i].frame_lock);
    }
    
    int victim = bpm_evict(bpm);
    if (victim < 0) {
        pthread_mutex_unlock(&bpm->bpool_mutex);
        return NULL;
    }
    
    BufferDescriptor *desc = &bpm->descriptors[victim];
    pthread_spin_lock(&desc->frame_lock);
    
    desc->page_id = page_id;
    desc->pin_count = 1;
    desc->is_dirty = false;
    desc->ref_bit = true;
    
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t bytes = pread(bpm->db_fd, &bpm->frames[victim], PAGE_SIZE, offset);
    if (bytes < (ssize_t)PAGE_SIZE) {
        memset(&bpm->frames[victim], 0, PAGE_SIZE);
        ((Page *)&bpm->frames[victim])->header.free_space_offset = PAGE_SIZE;
    }
    
    desc->page_lsn = ((Page *)&bpm->frames[victim])->header.lsn;
    pthread_spin_unlock(&desc->frame_lock);
    pthread_mutex_unlock(&bpm->bpool_mutex);
    
    return desc;
}

static void bpm_unpin_page(BufferPoolManager *bpm, PageID page_id, bool is_dirty) {
    pthread_mutex_lock(&bpm->bpool_mutex);
    for (int i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].frame_lock);
        if (bpm->descriptors[i].page_id == page_id) {
            if (is_dirty) {
                bpm->descriptors[i].is_dirty = true;
            }
            if (bpm->descriptors[i].pin_count > 0) {
                bpm->descriptors[i].pin_count--;
            }
            pthread_spin_unlock(&bpm->descriptors[i].frame_lock);
            pthread_mutex_unlock(&bpm->bpool_mutex);
            return;
        }
        pthread_spin_unlock(&bpm->descriptors[i].frame_lock);
    }
    pthread_mutex_unlock(&bpm->bpool_mutex);
}

static PageID bpm_allocate_page(BufferPoolManager *bpm, BTreeEngine *engine) {
    static PageID next_new_page = 1;
    pthread_mutex_lock(&bpm->bpool_mutex);
    PageID new_pid = next_new_page++;
    pthread_mutex_unlock(&bpm->bpool_mutex);
    
    BufferDescriptor *desc = bpm_fetch_page(bpm, new_pid);
    if (!desc) return (PageID)-1;
    
    pthread_rwlock_wrlock(&desc->rwlock);
    Page *p = &bpm->frames[desc->frame_id];
    memset(p, 0, PAGE_SIZE);
    p->header.free_space_offset = PAGE_SIZE;
    p->header.lsn = engine->wm.current_lsn;
    desc->page_lsn = p->header.lsn;
    desc->is_dirty = true;
    pthread_rwlock_unlock(&desc->rwlock);
    
    bpm_unpin_page(bpm, new_pid, true);
    return new_pid;
}

static int cell_compare(const uint8_t *key1, uint16_t len1, const uint8_t *key2, uint16_t len2) {
    size_t min_len = len1 < len2 ? len1 : len2;
    int cmp = memcmp(key1, key2, min_len);
    if (cmp != 0) return cmp;
    if (len1 < len2) return -1;
    if (len1 > len2) return 1;
    return 0;
}

static int btree_binary_search(Page *page, const uint8_t *key, uint16_t key_len, bool *found) {
    int low = 0;
    int high = (int)page->header.item_count - 1;
    CellHeader *headers = (CellHeader *)page->data;
    
    while (low <= high) {
        int mid = low + (high - low) / 2;
        CellHeader *h = &headers[mid];
        uint8_t *k = page->data + h->offset - sizeof(CellHeader) - h->val_len - h->key_len + sizeof(CellHeader);