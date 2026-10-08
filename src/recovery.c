#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#define PAGE_SIZE 4096
#define CRC32_POLYNOMIAL 0xEDB88320

typedef uint64_t PageID;
typedef uint64_t LSN;
typedef uint64_t TxnID;

typedef enum {
    LOG_BEGIN = 1,
    LOG_INSERT,
    LOG_UPDATE,
    LOG_COMMIT,
    LOG_ABORT,
    LOG_CHECKPOINT,
    LOG_CLR
} LogType;

typedef struct __attribute__((__packed__)) {
    LSN lsn;
    TxnID txid;
    LogType type;
    PageID page_id;
    uint32_t payload_len;
    LSN undo_next_lsn;
    uint32_t crc32;
} WALRecordHeader;

typedef struct {
    WALRecordHeader header;
    uint8_t *payload;
} WALRecord;

typedef struct {
    LSN lsn;
    uint32_t flags;
    uint16_t item_count;
    uint16_t free_space_offset;
} PageHeader;

typedef struct {
    PageHeader header;
    uint8_t data[PAGE_SIZE - sizeof(PageHeader)];
} Page;

typedef struct {
    PageID page_id;
    int pin_count;
    bool is_dirty;
    bool ref_bit;
    pthread_rwlock_t rwlock;
    pthread_spinlock_t spinlock;
    int frame_id;
} BufferDescriptor;

typedef struct {
    int fd;
    BufferDescriptor *descriptors;
    Page *pages;
    size_t pool_size;
} BufferPoolManager;

typedef struct {
    TxnID txid;
    LSN last_lsn;
    bool committed;
    bool aborted;
} TxnTableEntry;

typedef struct {
    PageID page_id;
    LSN rec_lsn;
} DirtyPageTableEntry;

typedef struct {
    TxnTableEntry *tx_entries;
    size_t tx_count;
    size_t tx_capacity;
    DirtyPageTableEntry *dpt_entries;
    size_t dpt_count;
    size_t dpt_capacity;
} AnalysisResults;

static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (size_t j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ CRC32_POLYNOMIAL;
            } else {
                crc >>= 1;
            }
        }
    }
    return ~crc;
}

static AnalysisResults* analysis_results_create(void) {
    AnalysisResults *res = (AnalysisResults*)malloc(sizeof(AnalysisResults));
    if (!res) return NULL;
    res->tx_capacity = 64;
    res->tx_count = 0;
    res->tx_entries = (TxnTableEntry*)malloc(res->tx_capacity * sizeof(TxnTableEntry));
    
    res->dpt_capacity = 64;
    res->dpt_count = 0;
    res->dpt_entries = (DirtyPageTableEntry*)malloc(res->dpt_capacity * sizeof(DirtyPageTableEntry));

    if (!res->tx_entries || !res->dpt_entries) {
        free(res->tx_entries);
        free(res->dpt_entries);
        free(res);
        return NULL;
    }
    return res;
}

static void analysis_results_free(AnalysisResults *res) {
    if (!res) return;
    free(res->tx_entries);
    free(res->dpt_entries);
    free(res);
}

static TxnTableEntry* find_or_add_txn(AnalysisResults *res, TxnID txid, LSN lsn) {
    for (size_t i = 0; i < res->tx_count; i++) {
        if (res->tx_entries[i].txid == txid) {
            if (lsn > res->tx_entries[i].last_lsn) {
                res->tx_entries[i].last_lsn = lsn;
            }
            return &res->tx_entries[i];
        }
    }
    if (res->tx_count >= res->tx_capacity) {
        size_t new_cap = res->tx_capacity * 2;
        TxnTableEntry *new_entries = (TxnTableEntry*)realloc(res->tx_entries, new_cap * sizeof(TxnTableEntry));
        if (!new_entries) return NULL;
        res->tx_entries = new_entries;
        res->tx_capacity = new_cap;
    }
    TxnTableEntry *entry = &res->tx_entries[res->tx_count++];
    entry->txid = txid;
    entry->last_lsn = lsn;
    entry->committed = false;
    entry->aborted = false;
    return entry;
}

static void add_or_update_dpt(AnalysisResults *res, PageID page_id, LSN rec_lsn) {
    for (size_t i = 0; i < res->dpt_count; i++) {
        if (res->dpt_entries[i].page_id == page_id) {
            if (rec_lsn < res->dpt_entries[i].rec_lsn) {
                res->dpt_entries[i].rec_lsn = rec_lsn;
            }
            return;
        }
    }
    if (res->dpt_count >= res->dpt_capacity) {
        size_t new_cap = res->dpt_capacity * 2;
        DirtyPageTableEntry *new_entries = (DirtyPageTableEntry*)realloc(res->dpt_entries, new_cap * sizeof(DirtyPageTableEntry));
        if (!new_entries) return;
        res->dpt_entries = new_entries;
        res->dpt_capacity = new_cap;
    }
    DirtyPageTableEntry *entry = &res->dpt_entries[res->dpt_count++];
    entry->page_id = page_id;
    entry->rec_lsn = rec_lsn;
}

static void remove_dpt(AnalysisResults *res, PageID page_id) {
    for (size_t i = 0; i < res->dpt_count; i++) {
        if (res->dpt_entries[i].page_id == page_id) {
            res->dpt_entries[i] = res->dpt_entries[res->dpt_count - 1];
            res->dpt_count--;
            break;
        }
    }
}

static bool read_wal_record(int wal_fd, WALRecord *record, off_t *offset) {
    ssize_t n = pread(wal_fd, &record->header, sizeof(WALRecordHeader), *offset);
    if (n <= 0) return false;

    if (record->header.payload_len > 0) {
        record->payload = (uint8_t*)malloc(record->header.payload_len);
        if (!record->payload) return false;
        n = pread(wal_fd, record->payload, record->header.payload_len, *offset + sizeof(WALRecordHeader));
        if (n <= 0) {
            free(record->payload);
            record->payload = NULL;
            return false;
        }
        
        size_t total_size = sizeof(WALRecordHeader) - sizeof(uint32_t);
        uint8_t *chk_buf = (uint8_t*)malloc(total_size + record->header.payload_len);
        if (!chk_buf) {
            free(record->payload);
            record->payload = NULL;
            return false;
        }
        memcpy(chk_buf, &record->header, total_size);
        memcpy(chk_buf + total_size, record->payload, record->header.payload_len);
        uint32_t computed_crc = calculate_crc32(chk_buf, total_size + record->header.payload_len);
        free(chk_buf);

        if (computed_crc != record->header.crc32) {
            free(record->payload);
            record->payload = NULL;
            return false;
        }
    } else {
        record->payload = NULL;
        size_t total_size = sizeof(WALRecordHeader) - sizeof(uint32_t);
        uint32_t computed_crc = calculate_crc32((uint8_t*)&record->header, total_size);
        if (computed_crc != record->header.crc32) {
            return false;
        }
    }

    *offset += sizeof(WALRecordHeader) + record->header.payload_len;
    return true;
}

AnalysisResults* recovery_analysis_phase(int wal_fd, off_t start_offset) {
    AnalysisResults *res = analysis_results_create();
    if (!res) return NULL;

    off_t current_offset = start_offset;
    WALRecord record;

    while (read_wal_record(wal_fd, &record, &current_offset)) {
        TxnTableEntry *tte = find_or_add_txn(res, record.header.txid, record.header.lsn);
        if (!tte) {
            if (record.payload) free(record.payload);
            analysis_results_free(res);
            return NULL;
        }

        switch (record.header.type) {
            case LOG_BEGIN:
                tte->committed = false;
                tte->aborted = false;
                break;
            case LOG_COMMIT:
                tte->committed = true;
                break;
            case LOG_ABORT:
                tte->aborted = true;
                break;
            case LOG_INSERT:
            case LOG_UPDATE:
            case LOG_CLR:
                if (record.header.page_id != 0) {
                    add_or_update_dpt(res, record.header.page_id, record.header.lsn);
                }
                break;
            case LOG_CHECKPOINT:
                break;
        }

        if (record.payload) {
            free(record.payload);
            record.payload = NULL;
        }
    }

    return res;
}

static bool fetch_page_from_disk(BufferPoolManager *bpm, PageID page_id, Page *page_out) {
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t n = pread(bpm->fd, page_out, PAGE_SIZE, offset);
    if (n < 0) return false;
    if (n == 0) {
        memset(page_out, 0, PAGE_SIZE);
        page_out->header.lsn = 0;
    }
    return true;
}

static bool write_page_to_disk(BufferPoolManager *bpm, PageID page_id, const Page *page) {
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t n = pwrite(bpm->fd, page, PAGE_SIZE, offset);
    return n == PAGE_SIZE;
}

bool recovery_redo_phase(int wal_fd, off_t start_offset, BufferPoolManager *bpm, AnalysisResults *analysis) {
    if (analysis->dpt_count == 0) {
        return true;
    }

    LSN min_rec_lsn = UINT64_MAX;
    for (size_t i = 0; i < analysis->dpt_count; i++) {
        if (analysis->dpt_entries[i].rec_lsn < min_rec_lsn) {
            min_rec_lsn = analysis->dpt_entries[i].rec_lsn;
        }
    }

    off_t current_offset = start_offset;
    WALRecord record;
    bool found_start = false;

    while (read_wal_record(wal_fd, &record, &current_offset)) {
        if (!found_start) {
            if (record.header.lsn >= min_rec_lsn) {
                found_start = true;
            } else {
                if (record.payload) free(record.payload);
                continue;
            }
        }

        bool needs_redo = false;
        if (record.header.type == LOG_INSERT || record.header.type == LOG_UPDATE || record.header.type == LOG_CLR) {
            if (record.header.page_id != 0) {
                for (size_t i = 0; i < analysis->dpt_count; i++) {
                    if (analysis->dpt_entries[i].page_id == record.header.page_id) {
                        if (record.header.lsn >= analysis->dpt_entries[i].rec_lsn) {
                            needs_redo = true;
                        }
                        break;
                    }
                }
            }
        }

        if (needs_redo) {
            Page page;
            if (!fetch_page_from_disk(bpm, record.header.page_id, &page)) {
                if (record.payload) free(record.payload);
                return false;
            }

            if (page.header.lsn < record.header.lsn) {
                if (record.payload && record.header.payload_len > 0) {
                    size_t copy_len = record.header.payload_len;
                    if (copy_len > sizeof(page.data)) {
                        copy_len = sizeof(page.data);
                    }
                    memcpy(page.data, record.payload, copy_len);
                }
                page.header.lsn = record.header.lsn;
                if (!write_page_to_disk(bpm, record.header.page_id, &page)) {
                    if (record.payload) free(record.payload);
                    return false;
                }
            }
        }

        if (record.payload) {
            free(record.payload);
            record.payload = NULL;
        }
    }

    return true;
}

static off_t find_wal_record_by_lsn(int wal_fd, LSN target_lsn, off_t start_offset) {
    off_t current_offset = start_offset;
    WALRecord record;
    while (read_wal_record(wal_fd, &record, &current_offset)) {
        LSN current_lsn = record.header.lsn;
        if (record.payload) {
            free(record.payload);
            record.payload = NULL;
        }
        if (current_lsn == target_lsn) {
            return current_offset - (sizeof(WALRecordHeader) + record.header.payload_len);
        }
    }
    return -1;
}

static LSN get_next_lsn(int wal_fd) {
    off_t current_offset = 0;
    WALRecord record;
    LSN max_lsn = 0;
    while (read_wal_record(wal_fd, &record, &current_offset)) {
        if (record.header.lsn > max_lsn) {
            max_lsn = record.header.lsn;
        }
        if (record.payload) {
            free(record.payload);
            record.payload = NULL;
        }
    }
    return max_lsn + 1;
}

static bool append_clr_to_wal(int wal_fd, WALRecord *clr) {
    size_t total_size = sizeof(WALRecordHeader) - sizeof(uint32_t);
    uint8_t *chk_buf = (uint8_t*)malloc(total_size + clr->header.payload_len);
    if (!chk_buf) return false;