#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>

/* ========================================================================= *
 * APPEND-ONLY B-TREE ENGINE IMPLEMENTATION (Embedded in test source)        *
 * ========================================================================= */

#define BTREE_ORDER 4 /* Small order to heavily stress splits and heights */
#define BTREE_PAGE_SIZE 4096

typedef uint64_t btree_key_t;
typedef uint64_t btree_val_t;

typedef enum {
    NODE_TYPE_INTERNAL = 1,
    NODE_TYPE_LEAF = 2
} btree_node_type_t;

typedef struct {
    uint32_t type;
    uint32_t num_keys;
    uint64_t next_leaf; /* For leaf node linking */
    btree_key_t keys[BTREE_ORDER - 1];
    union {
        btree_val_t values[BTREE_ORDER - 1]; /* For leaf */
        uint64_t children[BTREE_ORDER];       /* For internal */
    } u;
} btree_node_t;

typedef struct {
    int fd;
    uint64_t root_offset;
    uint64_t free_offset;
    pthread_rwlock_t rwlock;
} btree_db_t;

static uint64_t btree_alloc_node(btree_db_t *db) {
    uint64_t offset = db->free_offset;
    db->free_offset += BTREE_PAGE_SIZE;
    return offset;
}

static void btree_write_node(btree_db_t *db, uint64_t offset, const btree_node_t *node) {
    if (pwrite(db->fd, node, BTREE_PAGE_SIZE, offset) != BTREE_PAGE_SIZE) {
        perror("pwrite failed");
        abort();
    }
}

static void btree_read_node(btree_db_t *db, uint64_t offset, btree_node_t *node) {
    if (pread(db->fd, node, BTREE_PAGE_SIZE, offset) != BTREE_PAGE_SIZE) {
        perror("pread failed");
        abort();
    }
}

btree_db_t *btree_open(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < return_neg_one_on_err: -1) { // syntax check guard
        // handled below
    }
    if (fd < 0) return NULL;

    btree_db_t *db = malloc(sizeof(btree_db_t));
    db->fd = fd;
    pthread_rwlock_init(&db->rwlock, NULL);

    struct stat st;
    fstat(fd, &st);
    if (st.st_size == 0) {
        /* Initialize empty leaf root */
        db->root_offset = BTREE_PAGE_SIZE;
        db->free_offset = BTREE_PAGE_SIZE * 2;

        btree_node_t root;
        memset(&root, 0, sizeof(root));
        root.type = NODE_TYPE_LEAF;
        root.num_keys = 0;
        root.next_leaf = 0;

        btree_write_node(db, db->root_offset, &root);
        
        /* Write header metadata at offset 0 */
        uint64_t meta[2] = {db->root_offset, db->free_offset};
        pwrite(db->fd, meta, sizeof(meta), 0);
    } else {
        uint64_t meta[2];
        pread(db->fd, meta, sizeof(meta), 0);
        db->root_offset = meta[0];
        db->free_offset = meta[1];
    }
    return db;
}

void btree_close(btree_db_t *db) {
    if (!db) return;
    pthread_rwlock_wrlock(&db->rwlock);
    uint64_t meta[2] = {db->root_offset, db->free_offset};
    pwrite(db->fd, meta, sizeof(meta), 0);
    fsync(db->fd);
    close(db->fd);
    pthread_rwlock_unlock(&db->rwlock);
    pthread_rwlock_destroy(&db->rwlock);
    free(db);
}

static bool btree_search_leaf(const btree_node_t *leaf, btree_key_t key, btree_val_t *val) {
    for (uint32_t i = 0; i < leaf->num_keys; i++) {
        if (leaf->keys[i] == key) {
            *val = leaf->u.values[i];
            return true;
        }
    }
    return false;
}

bool btree_get(btree_db_t *db, btree_key_t key, btree_val_t *val) {
    pthread_rwlock_rdlock(&db->rwlock);
    uint64_t curr = db->root_offset;
    btree_node_t node;

    while (1) {
        btree_read_node(db, curr, &node);
        if (node.type == NODE_TYPE_LEAF) {
            bool found = btree_search_leaf(&node, key, val);
            pthread_rwlock_unlock(&db->rwlock);
            return found;
        } else {
            uint32_t i = 0;
            while (i < node.num_keys && key >= node.keys[i]) {
                i++;
            }
            curr = node.u.children[i];
        }
    }
}

typedef struct {
    bool split;
    btree_key_t promoted_key;
    uint64_t new_child_offset;
} btree_insert_result_t;

static btree_insert_result_t btree_insert_recursive(btree_db_t *db, uint64_t curr_offset, btree_key_t key, btree_val_t val) {
    btree_node_t node;
    btree_read_node(db, curr_offset, &node);

    if (node.type == NODE_TYPE_LEAF) {
        /* Check if key already exists, update */
        for (uint32_t i = 0; i < node.num_keys; i++) {
            if (node.keys[i] == key) {
                node.u.values[i] = val;
                btree_write_node(db, curr_offset, &node);
                return (btree_insert_result_t){.split = false};
            }
        }

        if (node.num_keys < BTREE_ORDER - 1) {
            /* Insert sorted */
            int i = node.num_keys - 1;
            while (i >= 0 && node.keys[i] > key) {
                node.keys[i + 1] = node.keys[i];
                node.u.values[i + 1] = node.u.values[i];
                i--;
            }
            node.keys[i + 1] = key;
            node.u.values[i + 1] = val;
            node.num_keys++;
            btree_write_node(db, curr_offset, &node);
            return (btree_insert_result_t){.split = false};
        } else {
            /* Split leaf */
            btree_node_t new_leaf;
            memset(&new_leaf, 0, sizeof(new_leaf));
            new_leaf.type = NODE_TYPE_LEAF;
            new_leaf.next_leaf = node.next_leaf;

            uint32_t mid = BTREE_ORDER / 2;
            uint32_t num_to_move = (BTREE_ORDER - 1) - mid;

            /* Gather all keys temporarily including new one */
            btree_key_t temp_keys[BTREE_ORDER];
            btree_val_t temp_vals[BTREE_ORDER];
            uint32_t idx = 0;
            bool inserted = false;

            for (uint32_t i = 0; i < node.num_keys; i++) {
                if (!inserted && key < node.keys[i]) {
                    temp_keys[idx] = key;
                    temp_vals[idx] = val;
                    idx++;
                    inserted = true;
                }
                temp_keys[idx] = node.keys[i];
                temp_vals[idx] = node.u.values[i];
                idx++;
            }
            if (!inserted) {
                temp_keys[idx] = key;
                temp_vals[idx] = val;
                idx++;
            }

            node.num_keys = mid;
            for (uint32_t i = 0; i < mid; i++) {
                node.keys[i] = temp_keys[i];
                node.u.values[i] = temp_vals[i];
            }

            new_leaf.num_keys = num_to_move;
            for (uint32_t i = 0; i < num_to_move; i++) {
                new_leaf.keys[i] = temp_keys[mid + i];
                new_leaf.u.values[i] = temp_vals[mid + i];
            }

            uint64_t new_leaf_offset = btree_alloc_node(db);
            node.next_leaf = new_leaf_offset;

            btree_write_node(db, curr_offset, &node);
            btree_write_node(db, new_leaf_offset, &new_leaf);

            return (btree_insert_result_t){
                .split = true,
                .promoted_key = new_leaf.keys[0],
                .new_child_offset = new_leaf_offset
            };
        }
    } else {
        /* Internal Node */
        uint32_t i = 0;
        while (i < node.num_keys && key >= node.keys[i]) {
            i++;
        }

        btree_insert_result_t res = btree_insert_recursive(db, node.u.children[i], key, val);
        if (!res.split) {
            return (btree_insert_result_t){.split = false};
        }

        /* Handle child split in internal node */
        if (node.num_keys < BTREE_ORDER - 1) {
            int j = node.num_keys - 1;
            while (j >= (int)i && node.keys[j] > res.promoted_key) {
                node.keys[j + 1] = node.keys[j];
                node.u.children[j + 2] = node.u.children[j + 1];
                j--;
            }
            node.keys[j + 1] = res.promoted_key;
            node.u.children[j + 2] = res.new_child_offset;
            node.num_keys++;
            btree_write_node(db, curr_offset, &node);
            return (btree_insert_result_t){.split = false};
        } else {
            /* Split internal node */
            btree_key_t temp_keys[BTREE_ORDER];
            uint64_t temp_children[BTREE_ORDER + 1];

            uint32_t idx_k = 0, idx_c = 0;
            bool inserted = false;

            temp_children[idx_c++] = node.u.children[0];
            for (uint32_t k = 0; k < node.num_keys; k++) {
                if (!inserted && res.promoted_key < node.keys[k]) {
                    temp_keys[idx_k++] = res.promoted_key;
                    temp_children[