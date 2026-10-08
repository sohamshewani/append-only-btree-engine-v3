#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#define BTREE_FANOUT 64
#define KEY_SIZE 8
#define VALUE_SIZE 8
#define WARMUP_OPS 10000
#define MEASURE_OPS 100000

typedef struct BTreeNode {
    bool is_leaf;
    int num_keys;
    uint64_t keys[BTREE_FANOUT - 1];
    union {
        struct BTreeNode *children[BTREE_FANOUT];
        uint64_t values[BTREE_FANOUT - 1];
    };
    struct BTreeNode *next; // For leaf node linking
} BTreeNode;

typedef struct {
    BTreeNode *root;
    BTreeNode *first_leaf;
} AppendOnlyBTree;

BTreeNode* create_node(bool is_leaf) {
    BTreeNode *node = (BTreeNode*)calloc(1, sizeof(BTreeNode));
    node->is_leaf = is_leaf;
    return node;
}

AppendOnlyBTree* btree_create() {
    AppendOnlyBTree *tree = (AppendOnlyBTree*)malloc(sizeof(AppendOnlyBTree));
    tree->root = create_node(true);
    tree->first_leaf = tree->root;
    return tree;
}

static inline void split_child(BTreeNode *parent, int i, BTreeNode *child) {
    BTreeNode *z = create_node(child->is_leaf);
    int t = BTREE_FANOUT / 2;
    z->num_keys = t - 1;
    
    for (int j = 0; j < t - 1; j++) {
        z->keys[j] = child->keys[j + t];
    }
    
    if (!child->is_leaf) {
        for (int j = 0; j < t; j++) {
            z->children[j] = child->children[j + t];
        }
    } else {
        for (int j = 0; j < t - 1; j++) {
            z->values[j] = child->values[j + t];
        }
        z->next = child->next;
        child->next = z;
    }
    
    child->num_keys = t - 1;
    
    for (int j = parent->num_keys; j >= i + 1; j--) {
        parent->children[j + 1] = parent->children[j];
    }
    
    parent->children[i + 1] = z;
    
    for (int j = parent->num_keys - 1; j >= i; j--) {
        parent->keys[j + 1] = parent->keys[j];
    }
    
    parent->keys[i] = child->keys[t - 1];
    parent->num_keys++;
}

static inline void insert_non_full(BTreeNode *node, uint64_t key, uint64_t val) {
    int i = node->num_keys - 1;
    
    if (node->is_leaf) {
        while (i >= 0 && node->keys[i] > key) {
            node->keys[i + 1] = node->keys[i];
            node->values[i + 1] = node->values[i];
            i--;
        }
        node->keys[i + 1] = key;
        node->values[i + 1] = val;
        node->num_keys++;
    } else {
        while (i >= 0 && node->keys[i] > key) {
            i--;
        }
        i++;
        if (node->children[i]->num_keys == BTREE_FANOUT - 1) {
            split_child(node, i, node->children[i]);
            if (node->keys[i] < key) {
                i++;
            }
        }
        insert_non_full(node->children[i], key, val);
    }
}

void btree_append(AppendOnlyBTree *tree, uint64_t key, uint64_t val) {
    BTreeNode *root = tree->root;
    if (root->num_keys == BTREE_FANOUT - 1) {
        BTreeNode *s = create_node(false);
        tree->root = s;
        s->children[0] = root;
        split_child(s, 0, root);
        insert_non_full(s, key, val);
    } else {
        insert_non_full(root, key, val);
    }
}

bool btree_get(AppendOnlyBTree *tree, uint64_t key, uint64_t *val) {
    BTreeNode *curr = tree->first_leaf;
    while (curr != NULL) {
        for (int i = 0; i < curr->num_keys; i++) {
            if (curr->keys[i] == key) {
                *val = curr->values[i];
                return true;
            }
        }
        curr = curr->next;
    }
    return false;
}

static inline uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

int cmp_uint64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t*)a;
    uint64_t vb = *(const uint64_t*)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

void free_tree(BTreeNode *node) {
    if (!node) return;
    if (!node->is_leaf) {
        for (int i = 0; i <= node->num_keys; i++) {
            free_tree(node->children[i]);
        }
    }
    free(node);
}

int main(void) {
    AppendOnlyBTree *tree = btree_create();
    
    uint64_t *latencies_insert = (uint64_t*)malloc(MEASURE_OPS * sizeof(uint64_t));
    uint64_t *latencies_get = (uint64_t*)malloc(MEASURE_OPS * sizeof(uint64_t));

    // Warmup Insert
    for (uint64_t i = 0; i < WARMUP_OPS; i++) {
        btree_append(tree, i, i + 100);
    }

    // Measure Insert (Append-only: monotonic keys)
    uint64_t start_total = get_nanos();
    for (uint64_t i = 0; i < MEASURE_OPS; i++) {
        uint64_t key = WARMUP_OPS + i;
        uint64_t start = get_nanos();
        btree_append(tree, key, key + 100);
        uint64_t end = get_nanos();
        latencies_insert[i] = end - start;
    }
    uint64_t end_total = get_nanos();
    uint64_t insert_time_ns = end_total - start_total;

    // Measure Get (Point Lookup)
    start_total = get_nanos();
    for (uint64_t i = 0; i < MEASURE_OPS; i++) {
        uint64_t key = WARMUP_OPS + (i * 37) % MEASURE_OPS; // pseudo-random distribution over inserted keys
        uint64_t val = 0;
        uint64_t start = get_nanos();
        volatile bool found = btree_get(tree, key, &val);
        (void)found;
        uint64_t end = get_nanos();
        latencies_get[i] = end - start;
    }
    end_total = get_nanos();
    uint64_t get_time_ns = end_total - start_total;

    qsort(latencies_insert, MEASURE_OPS, sizeof(uint64_t), cmp_uint64);
    qsort(latencies_get, MEASURE_OPS, sizeof(uint64_t), cmp_uint64);

    uint64_t ins_p50 = latencies_insert[(int)(MEASURE_OPS * 0.50)];
    uint64_t ins_p99 = latencies_insert[(int)(MEASURE_OPS * 0.99)];
    uint64_t ins_p999 = latencies_insert[(int)(MEASURE_OPS * 0.999)];

    uint64_t get_p50 = latencies_get[(int)(MEASURE_OPS * 0.50)];
    uint64_t get_p99 = latencies_get[(int)(MEASURE_OPS * 0.99)];
    uint64_t get_p999 = latencies_get[(int)(MEASURE_OPS * 0.999)];

    double insert_ops_sec = (double)MEASURE_OPS / ((double)insert_time_ns / 1000000000.0);
    double get_ops_sec = (double)MEASURE_OPS / ((double)get_time_ns / 1000000000.0);

    printf("+------------------+-----------------+---------------+---------------+----------------+\n");
    printf("| Operation        | Throughput      | p50 Latency   | p99 Latency   | p99.9 Latency  |\n");
    printf("+------------------+-----------------+---------------+---------------+----------------+\n");
    printf("| Append (Insert)  | %10.2f ops/s | %7. fós ns  | %7. ns        | %8. ns         |\n", 
           insert_ops_sec, (double)ins_p50, (double)ins_p