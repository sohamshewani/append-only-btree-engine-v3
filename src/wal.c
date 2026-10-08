#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#define WAL_PAGE_SIZE 4096
#define WAL_MAGIC 0x57414C31     // "WAL1"
#define WAL_BUFFER_COUNT 64

typedef uint64_t LSN;
typedef uint64_t TxnID;
typedef uint32_t PageID;

typedef enum {
    WAL_BEGIN = 1,
    WAL_INSERT,
    WAL_UPDATE,
    WAL_DELETE,
    WAL_COMMIT,
    WAL_ABORT,
    WAL_CHECKPOINT,
    WAL_CLR
} WALRecordType;

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint32_t checksum;
    LSN lsn;
    LSN prev_lsn;
    TxnID txid;
    uint32_t type;
    PageID page_id;
    uint32_t payload_len;
} WALRecordHeader;
#pragma pack(pop)

typedef struct {
    int fd;
    char filepath[1024];
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    
    LSN next_lsn;
    LSN flushed_lsn;
    
    uint8_t *buffer;
    size_t buffer_capacity;
    size_t buffer_len;
    
    int is_shutdown;
    pthread_t flusher_thread;
} WALManager;

static WALManager *g_wal_mgr = NULL;

static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

int wal_init(const char *wal_filepath) {
    if (!wal_filepath) return -1;

    WALManager *mgr = (WALManager *)malloc(sizeof(WALManager));
    if (!mgr) return -ENOMEM;

    memset(mgr, 0, sizeof(WALManager));
    strncpy(mgr->filepath, wal_filepath, sizeof(mgr->filepath) - 1);

    mgr->fd = open(mgr->filepath, O_CREAT | O_RDWR | O_APPEND, 0666);
    if (mgr->fd < 0) {
        free(mgr);
        return -errno;
    }

    if (pthread_mutex_init(&mgr->mutex, NULL) != 0) {
        close(mgr->fd);
        free(mgr);
        return -EAGAIN;
    }

    if (pthread_cond_init(&mgr->cond, NULL) != 0) {
        pthread_mutex_destroy(&mgr->mutex);
        close(mgr->fd);
        free(mgr);
        return -EAGAIN;
    }

    mgr->buffer_capacity = WAL_PAGE_SIZE * WAL_BUFFER_COUNT;
    mgr->buffer = (uint8_t *)malloc(mgr->buffer_capacity);
    if (!mgr->buffer) {
        pthread_cond_destroy(&mgr->cond);
        pthread_mutex_destroy(&mgr->mutex);
        close(mgr->fd);
        free(mgr);
        return -ENOMEM;
    }

    mgr->buffer_len = 0;
    mgr->next_lsn = 1;
    mgr->flushed_lsn = 0;
    mgr->is_shutdown = 0;

    struct stat st;
    if (fstat(mgr->fd, &st) == 0 && st.st_size > 0) {
        off_t current_pos = lseek(mgr->fd, 0, SEEK_END);
        if (current_pos > 0) {
            mgr->next_lsn = (LSN)(current_pos / sizeof(WALRecordHeader)) + 1;
            mgr->flushed_lsn = mgr->next_lsn - 1;
        }
    }

    g_wal_mgr = mgr;
    return 0;
}

LSN wal_append(WALRecordType type, TxnID txid, PageID page_id, LSN prev_lsn, const uint8_t *payload, uint32_t payload_len) {
    if (!g_wal_mgr) return 0;

    WALManager *mgr = g_wal_mgr;

    size_t total_record_size = sizeof(WALRecordHeader) + payload_len;
    if (total_record_size > WAL_PAGE_SIZE) {
        return 0;
    }

    pthread_mutex_lock(&mgr->mutex);

    if (mgr->buffer_len + total_record_size > mgr->buffer_capacity) {
        ssize_t written = write(mgr->fd, mgr->buffer, mgr->buffer_len);
        if (written < 0) {
            pthread_mutex_unlock(&mgr->mutex);
            return 0;
        }
        fsync(mgr->fd);
        mgr->flushed_lsn = mgr->next_lsn - 1;
        mgr->buffer_len = 0;
    }

    LSN assigned_lsn = mgr->next_lsn++;

    WALRecordHeader *header = (WALRecordHeader *)(mgr->buffer + mgr->buffer_len);
    header->magic = WAL_MAGIC;
    header->lsn = assigned_lsn;
    header->prev_lsn = prev_lsn;
    header->txid = txid;
    header->type = (uint32_t)type;
    header->page_id = page_id;
    header->payload_len = payload_len;

    if (payload_len > 0 && payload) {
        memcpy(mgr->buffer + mgr->buffer_len + sizeof(WALRecordHeader), payload, payload_len);
    }

    header->checksum = calculate_crc32((const uint8_t *)header + 8, sizeof(WALRecordHeader) - 8 + payload_len);

    mgr->buffer_len += total_record_size;

    if (type == WAL_COMMIT || type == WAL_ABORT) {
        ssize_t written = write(mgr->fd, mgr->buffer, mgr->buffer_len);
        if (written >= 0) {
            fsync(mgr->fd);
            mgr->flushed_lsn = assigned_lsn;
            mgr->buffer_len = 0;
        }
    }

    pthread_mutex_unlock(&mgr->mutex);
    return assigned_lsn;
}

int wal_flush(LSN target_lsn) {
    if (!g_wal_mgr) return -EINVAL;

    WALManager *mgr = g_wal_mgr;

    pthread_mutex_lock(&mgr->mutex);

    if (target_lsn <= mgr->flushed_lsn) {
        pthread_mutex_unlock(&mgr->mutex);
        return 0;
    }

    if (mgr->buffer_len > 0) {
        ssize_t written = write(mgr->fd, mgr->buffer, mgr->buffer_len);
        if (written < 0) {
            pthread_mutex_unlock(&mgr->mutex);
            return -errno;
        }
        if (fsync(mgr->fd) < 0) {
            pthread_mutex_unlock(&mgr->mutex);
            return -errno;
        }
        mgr->flushed_lsn = mgr->next_lsn - 1;
        mgr->buffer_len = 0;
    }

    pthread_mutex_unlock(&mgr->mutex);
    return 0;
}

void wal_shutdown(void) {
    if (!g_wal_mgr) return;

    WALManager *mgr = g_wal_mgr;

    pthread_mutex_lock(&mgr->mutex);
    mgr->is_shutdown = 1;
    pthread_cond_signal(&mgr->cond);

    if (mgr->buffer_len > 0) {
        ssize_t written = write(mgr->fd, mgr->buffer, mgr->buffer_len);
        if (written >= 0) {
            fsync(mgr->fd);
        }
        mgr->buffer_len = 0;
    }

    pthread_mutex_unlock(&mgr->mutex);

    pthread_mutex_destroy(&mgr->mutex);
    pthread_cond_destroy(&mgr->cond);

    if (mgr->fd >= 0) {
        close(mgr->fd);
    }

    if (mgr->buffer) {
        free(mgr->buffer);
    }

    free(mgr);
    g_wal_mgr = NULL;
}