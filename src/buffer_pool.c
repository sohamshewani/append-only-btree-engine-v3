#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#define PAGE_SIZE 4096
#define INVALID_PAGE_ID 0xFFFFFFFFFFFFFFFFULL

typedef uint64_t PageID;
typedef uint64_t LSN;

typedef struct __attribute__((__packed__)) {
    LSN lsn;
    uint32_t flags;
    uint16_t item_count;
    uint16_t free_space_offset;
} PageHeader;

typedef struct {
    PageHeader header;
    char data[PAGE_SIZE - sizeof(PageHeader)];
} Page;

typedef struct {
    PageID page_id;
    int32_t pin_count;
    bool is_dirty;
    bool ref_bit;
    pthread_rwlock_t rwlock;
    pthread_spinlock_t spinlock;
    int frame_id;
} BufferDescriptor;

typedef struct {
    int fd;
    size_t pool_size;
    Page *pages;
    BufferDescriptor *descriptors;
    int *free_list;
    int free_list_top;
    int clock_hand;
    pthread_mutex_t manager_mutex;
    pthread_cond_t flush_cond;
    pthread_t flusher_thread;
    bool shutdown_flag;
} BufferPoolManager;

static BufferPoolManager *bpm_global = NULL;

static void panic_error(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static int get_system_page_size(void) {
    long sz = sysconf(_SC_PAGESIZE);
    if (sz <= 0) return PAGE_SIZE;
    return (int)sz;
}

static int clock_evict_frame(BufferPoolManager *bpm) {
    int num_frames = (int)bpm->pool_size;
    int scanned = 0;
    while (scanned < num_frames * 2) {
        int idx = bpm->clock_hand;
        bpm->clock_hand = (bpm->clock_hand + 1) % num_frames;

        pthread_spin_lock(&bpm->descriptors[idx].spinlock);
        if (bpm->descriptors[idx].pin_count > 0) {
            pthread_spin_unlock(&bpm->descriptors[idx].spinlock);
            scanned++;
            continue;
        }

        if (bpm->descriptors[idx].ref_bit) {
            bpm->descriptors[idx].ref_bit = false;
            pthread_spin_unlock(&bpm->descriptors[idx].spinlock);
            scanned++;
            continue;
        }

        // Found unpinned frame with ref_bit == 0
        if (bpm->descriptors[idx].page_id != INVALID_PAGE_ID && bpm->descriptors[idx].is_dirty) {
            // Write back to disk
            off_t offset = (off_t)bpm->descriptors[idx].page_id * PAGE_SIZE;
            if (pwrite(bpm->fd, &bpm->pages[idx], PAGE_SIZE, offset) != PAGE_SIZE) {
                pthread_spin_unlock(&bpm->descriptors[idx].spinlock);
                panic_error("clock_evict_frame pwrite failed");
            }
            bpm->descriptors[idx].is_dirty = false;
        }

        // Evict old page mapping
        PageID old_pid = bpm->descriptors[idx].page_id;
        (void)old_pid;
        bpm->descriptors[idx].page_id = INVALID_PAGE_ID;
        pthread_spin_unlock(&bpm->descriptors[idx].spinlock);
        return idx;
    }

    // Fallback if all pinned
    panic_error("Buffer pool exhausted: all frames are pinned.");
    return -1;
}

static void* background_flusher_routine(void *arg) {
    BufferPoolManager *bpm = (BufferPoolManager *)arg;
    while (true) {
        pthread_mutex_lock(&bpm->manager_mutex);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5; // Flush every 5 seconds
        pthread_cond_timedwait(&bpm->flush_cond, &bpm->manager_mutex, &ts);

        if (bpm->shutdown_flag) {
            pthread_mutex_unlock(&bpm->manager_mutex);
            break;
        }
        pthread_mutex_unlock(&bpm->manager_mutex);

        // Scan and flush dirty unpinned frames
        int num_frames = (int)bpm->pool_size;
        for (int i = 0; i < num_frames; i++) {
            pthread_spin_lock(&bpm->descriptors[i].spinlock);
            if (bpm->descriptors[i].page_id != INVALID_PAGE_ID && bpm->descriptors[i].is_dirty && bpm->descriptors[i].pin_count == 0) {
                off_t offset = (off_t)bpm->descriptors[i].page_id * PAGE_SIZE;
                if (pwrite(bpm->fd, &bpm->pages[i], PAGE_SIZE, offset) != PAGE_SIZE) {
                    pthread_spin_unlock(&bpm->descriptors[i].spinlock);
                    panic_error("background_flusher pwrite failed");
                }
                bpm->descriptors[i].is_dirty = false;
            }
            pthread_spin_unlock(&bpm->descriptors[i].spinlock);
        }
    }
    return NULL;
}

BufferPoolManager* buffer_pool_init(const char *db_file_path, size_t pool_size) {
    if (get_system_page_size() != PAGE_SIZE) {
        // System page size validation, ensure alignment matches expectations
    }

    int fd = open(db_file_path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        panic_error("buffer_pool_init open failed");
    }

    BufferPoolManager *bpm = (BufferPoolManager *)malloc(sizeof(BufferPoolManager));
    if (!bpm) panic_error("buffer_pool_init malloc bpm failed");

    bpm->fd = fd;
    bpm->pool_size = pool_size;
    bpm->shutdown_flag = false;
    bpm->clock_hand = 0;

    int res = posix_memalign((void **)&bpm->pages, PAGE_SIZE, pool_size * sizeof(Page));
    if (res != 0) panic_error("buffer_pool_init posix_memalign pages failed");
    memset(bpm->pages, 0, pool_size * sizeof(Page));

    bpm->descriptors = (BufferDescriptor *)malloc(pool_size * sizeof(BufferDescriptor));
    if (!bpm->descriptors) panic_error("buffer_pool_init malloc descriptors failed");

    bpm->free_list = (int *)malloc(pool_size * sizeof(int));
    if (!bpm->free_list) panic_error("buffer_pool_init malloc free_list failed");
    bpm->free_list_top = (int)pool_size;

    for (size_t i = 0; i < pool_size; i++) {
        bpm->descriptors[i].page_id = INVALID_PAGE_ID;
        bpm->descriptors[i].pin_count = 0;
        bpm->descriptors[i].is_dirty = false;
        bpm->descriptors[i].ref_bit = false;
        bpm->descriptors[i].frame_id = (int)i;
        if (pthread_rwlock_init(&bpm->descriptors[i].rwlock, NULL) != 0) {
            panic_error("buffer_pool_init rwlock_init failed");
        }
        if (pthread_spin_init(&bpm->descriptors[i].spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
            panic_error("buffer_pool_init spin_init failed");
        }
        bpm->free_list[i] = (int)(pool_size - 1 - i);
    }

    if (pthread_mutex_init(&bpm->manager_mutex, NULL) != 0) {
        panic_error("buffer_pool_init manager_mutex failed");
    }
    if (pthread_cond_init(&bpm->flush_cond, NULL) != 0) {
        panic_error("buffer_pool_init flush_cond failed");
    }
    if (pthread_create(&bpm->flusher_thread, NULL, background_flusher_routine, bpm) != 0) {
        panic_error("buffer_pool_init flusher_thread create failed");
    }

    bpm_global = bpm;
    return bpm;
}

void buffer_pool_shutdown(BufferPoolManager *bpm) {
    if (!bpm) return;

    pthread_mutex_lock(&bpm->manager_mutex);
    bpm->shutdown_flag = true;
    pthread_cond_signal(&bpm->flush_cond);
    pthread_mutex_unlock(&bpm->manager_mutex);

    pthread_join(bpm->flusher_thread, NULL);

    // Flush all dirty pages
    for (size_t i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].spinlock);
        if (bpm->descriptors[i].page_id != INVALID_PAGE_ID && bpm->descriptors[i].is_dirty) {
            off_t offset = (off_t)bpm->descriptors[i].page_id * PAGE_SIZE;
            if (pwrite(bpm->fd, &bpm->pages[i], PAGE_SIZE, offset) != PAGE_SIZE) {
                panic_error("buffer_pool_shutdown pwrite failed");
            }
            bpm->descriptors[i].is_dirty = false;
        }
        pthread_spin_unlock(&bpm->descriptors[i].spinlock);

        pthread_rwlock_destroy(&bpm->descriptors[i].rwlock);
        pthread_spin_destroy(&bpm->descriptors[i].spinlock);
    }

    close(bpm->fd);
    free(bpm->pages);
    free(bpm->descriptors);
    free(bpm->free_list);
    pthread_mutex_destroy(&bpm->manager_mutex);
    pthread_cond_destroy(&bpm->flush_cond);
    free(bpm);
    bpm_global = NULL;
}

Page* buffer_pool_fetch_page(BufferPoolManager *bpm, PageID page_id) {
    int target_frame = -1;

    // 1. Check if page is already in buffer pool
    for (size_t i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].spinlock);
        if (bpm->descriptors[i].page_id == page_id) {
            bpm->descriptors[i].pin_count++;
            bpm->descriptors[i].ref_bit = true;
            target_frame = (int)i;
            pthread_spin_unlock(&bpm->descriptors[i].spinlock);
            break;
        }
        pthread_spin_unlock(&bpm->descriptors[i].spinlock);
    }

    if (target_frame != -1) {
        return &bpm->pages[target_frame];
    }

    // 2. Allocate from free list if available
    pthread_mutex_lock(&bpm->manager_mutex);
    if (bpm->free_list_top > 0) {
        bpm->free_list_top--;
        target_frame = bpm->free_list[bpm->free_list_top];
        pthread_mutex_unlock(&bpm->manager_mutex);

        pthread_spin_lock(&bpm->descriptors[target_frame].spinlock);
        bpm->descriptors[target_frame].page_id = page_id;
        bpm->descriptors[target_frame].pin_count = 1;
        bpm->descriptors[target_frame].is_dirty = false;
        bpm->descriptors[target_frame].ref_bit = true;
        pthread_spin_unlock(&bpm->descriptors[target_frame].spinlock);

        // Read from disk
        off_t offset = (off_t)page_id * PAGE_SIZE;
        struct stat st;
        fstat(bpm->fd, &st);
        if (offset < st.st_size) {
            if (pread(bpm->fd, &bpm->pages[target_frame], PAGE_SIZE, offset) != PAGE_SIZE) {
                panic_error("buffer_pool_fetch_page pread failed");
            }
        } else {
            memset(&bpm->pages[target_frame], 0, PAGE_SIZE);
        }

        return &bpm->pages[target_frame];
    }
    pthread_mutex_unlock(&bpm->manager_mutex);

    // 3. Evict via CLOCK algorithm
    target_frame = clock_evict_frame(bpm);

    pthread_spin_lock(&bpm->descriptors[target_frame].spinlock);
    bpm->descriptors[target_frame].page_id = page_id;
    bpm->descriptors[target_frame].pin_count = 1;
    bpm->descriptors[target_frame].is_dirty = false;
    bpm->descriptors[target_frame].ref_bit = true;
    pthread_spin_unlock(&bpm->descriptors[target_frame].spinlock);

    // Read from disk
    off_t offset = (off_t)page_id * PAGE_SIZE;
    struct stat st;
    fstat(bpm->fd, &st);
    if (offset < st.st_size) {
        if (pread(bpm->fd, &bpm->pages[target_frame], PAGE_SIZE, offset) != PAGE_SIZE) {
            panic_error("buffer_pool_fetch_page pread failed");
        }
    } else {
        memset(&bpm->pages[target_frame], 0, PAGE_SIZE);
    }

    return &bpm->pages[target_frame];
}

void buffer_pool_unpin_page(BufferPoolManager *bpm, PageID page_id, bool is_dirty) {
    for (size_t i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].spinlock);
        if (bpm->descriptors[i].page_id == page_id) {
            if (is_dirty) {
                bpm->descriptors[i].is_dirty = true;
            }
            if (bpm->descriptors[i].pin_count > 0) {
                bpm->descriptors[i].pin_count--;
            }
            pthread_spin_unlock(&bpm->descriptors[i].spinlock);
            return;
        }
        pthread_spin_unlock(&bpm->descriptors[i].spinlock);
    }
    panic_error("buffer_pool_unpin_page: PageID not found in buffer pool");
}

Page* buffer_pool_new_page(BufferPoolManager *bpm, PageID *out_page_id) {
    // Generate new unique PageID based on file size or allocator
    struct stat st;
    fstat(bpm->fd, &st);
    PageID new_pid = (PageID)(st.st_size / PAGE_SIZE);

    // Extend file size physically
    if (ftruncate(bpm->fd, (off_t)((new_pid + 1) * PAGE_SIZE)) != 0) {
        panic_error("buffer_pool_new_page ftruncate failed");
    }

    *out_page_id = new_pid;
    return buffer_pool_fetch_page(bpm, new_pid);
}

void buffer_pool_flush_page(BufferPoolManager *bpm, PageID page_id) {
    for (size_t i = 0; i < bpm->pool_size; i++) {
        pthread_spin_lock(&bpm->descriptors[i].spinlock);
        if (bpm->descriptors[i].page_id == page_id) {
            if (bpm->descriptors[i].is_dirty) {
                off_t offset = (off_t)page_id * PAGE_SIZE;
                if (pwrite(bpm->fd, &bpm->pages[i], PAGE_SIZE, offset) != PAGE_SIZE) {
                    pthread_spin_unlock(&bpm->descriptors[i].spinlock);
                    panic_error("buffer_pool_flush_page pwrite failed");
                }
                bpm->descriptors[i].is_dirty = false;
            }
            pthread_spin_unlock(&bpm->descriptors[i].spinlock);
            return;
        }
        pthread_spin_unlock(&bpm->descriptors[i].spinlock);
    }