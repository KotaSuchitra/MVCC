// mvcc_alt.c
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

#define MAX_KEY_LEN 64
#define MAX_VAL_LEN 256
#define MAX_OPS_PER_TX 128
#define MAX_LOGS 4096
#define MAX_THREADS 5

typedef enum { OP_SET = 1, OP_DEL = 2 } OpType;

/* Version in the MVCC chain (newest-first) */
typedef struct Version {
    char value[MAX_VAL_LEN];
    unsigned long ts;           /* begin timestamp */
    struct Version *next;
} Version;

/* A key/node in the store */
typedef struct KVNode {
    char key[MAX_KEY_LEN];
    Version *versions;          /* newest-first */
    pthread_mutex_t lock;       /* protects versions list */
    struct KVNode *next;
} KVNode;

/* Single operation recorded in transaction */
typedef struct Op {
    OpType type;
    char key[MAX_KEY_LEN];
    char value[MAX_VAL_LEN];    /* empty for delete */
    unsigned long apply_ts;     /* filled at commit time */
} Op;

/* Transaction container */
typedef struct Transaction {
    int tid;
    unsigned long start_ts;     /* snapshot timestamp */
    Op ops[MAX_OPS_PER_TX];
    int op_count;
} Transaction;

/* Global log entry (committed ops) */
typedef struct LogEntry {
    int tid;
    OpType type;
    char key[MAX_KEY_LEN];
    char value[MAX_VAL_LEN];
    unsigned long ts;
} LogEntry;

/* Globals */
static KVNode *store = NULL;
static pthread_mutex_t store_global_lock = PTHREAD_MUTEX_INITIALIZER;
static LogEntry glog[MAX_LOGS];
static int glog_index = 0;
static pthread_mutex_t glog_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_ulong global_ts = 1;
static atomic_int tid_counter = 1;

/* helper: find or create node (protected by store_global_lock) */
static KVNode *get_or_create_node(const char *key) {
    pthread_mutex_lock(&store_global_lock);
    KVNode *cur = store;
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            pthread_mutex_unlock(&store_global_lock);
            return cur;
        }
        cur = cur->next;
    }
    KVNode *node = calloc(1, sizeof(KVNode));
    strncpy(node->key, key, MAX_KEY_LEN-1);
    node->versions = NULL;
    pthread_mutex_init(&node->lock, NULL);
    node->next = store;
    store = node;
    pthread_mutex_unlock(&store_global_lock);
    return node;
}

/* Begin a new transaction */
Transaction *begin_tx(void) {
    Transaction *tx = calloc(1, sizeof(Transaction));
    tx->tid = atomic_fetch_add(&tid_counter, 1);
    tx->start_ts = atomic_fetch_add(&global_ts, 1);
    tx->op_count = 0;
    printf("Transaction %d started (snapshot=%lu)\n", tx->tid, tx->start_ts);
    return tx;
}

/* Read consistent value according to tx snapshot (no locks needed for readers beyond per-node lock) */
char *kv_get(Transaction *tx, const char *key) {
    KVNode *node = get_or_create_node(key);
    pthread_mutex_lock(&node->lock);
    Version *v = node->versions;
    while (v) {
        if (v->ts <= tx->start_ts) {
            /* return a pointer to the version value (note: caller must copy if it needs it later) */
            char *res = strdup(v->value);
            pthread_mutex_unlock(&node->lock);
            return res;
        }
        v = v->next;
    }
    pthread_mutex_unlock(&node->lock);
    return NULL;
}

/* Buffer a set in the transaction (no global effect until commit) */
int kv_set(Transaction *tx, const char *key, const char *value) {
    if (!tx) return -1;
    if (tx->op_count >= MAX_OPS_PER_TX) return -1;
    Op *op = &tx->ops[tx->op_count++];
    op->type = OP_SET;
    strncpy(op->key, key, MAX_KEY_LEN-1);
    strncpy(op->value, value, MAX_VAL_LEN-1);
    op->apply_ts = 0;
    printf("[T%d] buffered SET %s = %s\n", tx->tid, key, value);
    return 0;
}

/* Buffer a delete in the transaction */
int kv_del(Transaction *tx, const char *key) {
    if (!tx) return -1;
    if (tx->op_count >= MAX_OPS_PER_TX) return -1;
    Op *op = &tx*
