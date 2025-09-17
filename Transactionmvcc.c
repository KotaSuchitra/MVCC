#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_KEYS 16
#define MAX_KEYNAME 16
#define MAX_TRANSACTIONS 32
#define MAX_WRITESET 8

typedef int txid_t;
typedef int commit_ts_t;

// ===== Versioned Value =====
typedef struct Version {
    commit_ts_t commit_ts;     // commit timestamp
    char *value;               // stored value
    struct Version *next;      // newer -> older
} Version;

// ===== Key =====
typedef struct Key {
    char name[MAX_KEYNAME];
    Version *versions;         // head = newest version
    txid_t lock_owner;         // 0 = no lock
} Key;

// ===== Transaction =====
typedef enum {TX_ACTIVE, TX_ABORTED, TX_COMMITTED} tx_state_t;

typedef struct KVPair {
    char key[MAX_KEYNAME];
    char value[128];
} KVPair;

typedef struct Transaction {
    txid_t id;
    commit_ts_t start_ts;
    tx_state_t state;
    KVPair write_set[MAX_WRITESET];
    int write_count;
} Transaction;

// ===== Global Store =====
Key store[MAX_KEYS];
int store_count = 0;
commit_ts_t global_commit_ts = 1;
txid_t global_tx_seq = 1;
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;

// ===== Helpers =====
Key* create_key(const char *k, const char *initial) {
    Key *key = &store[store_count++];
    strncpy(key->name, k, MAX_KEYNAME-1);
    key->lock_owner = 0;
    Version *v = malloc(sizeof(Version));
    v->commit_ts = 0;
    v->value = strdup(initial);
    v->next = NULL;
    key->versions = v;
    return key;
}

Key* get_key(const char *k) {
    for (int i=0;i<store_count;i++) {
        if (strcmp(store[i].name, k)==0) return &store[i];
    }
    return NULL;
}

void add_version(Key *k, commit_ts_t ts, const char *val) {
    Version *v = malloc(sizeof(Version));
    v->commit_ts = ts;
    v->value = strdup(val);
    v->next = k->versions;
    k->versions = v;
}

// ===== Transaction API =====
Transaction* tx_begin() {
    Transaction *tx = calloc(1,sizeof(Transaction));
    pthread_mutex_lock(&global_lock);
    tx->id = global_tx_seq++;
    tx->start_ts = global_commit_ts; // snapshot timestamp
    pthread_mutex_unlock(&global_lock);
    tx->state = TX_ACTIVE;
    printf("[TX %d] BEGIN (snapshot=%d)\n", tx->id, tx->start_ts);
    return tx;
}

void tx_read(Transaction *tx, const char *keyname) {
    Key *k = get_key(keyname);
    if (!k) { printf("[TX %d] READ %s -> NULL\n", tx->id,keyname); return; }
    Version *v = k->versions;
    while (v) {
        if (v->commit_ts <= tx->start_ts) {
            printf("[TX %d] READ %s -> %s (as of ts=%d)\n", tx->id, keyname, v->value, v->commit_ts);
            return;
        }
        v = v->next;
    }
    printf("[TX %d] READ %s -> NULL\n", tx->id,keyname);
}

// Explicit versioned read
void tx_read_versioned(const char *keyname, commit_ts_t ts) {
    Key *k = get_key(keyname);
    if (!k) { printf("[Versioned] %s at ts=%d -> NULL\n", keyname, ts); return; }
    Version *v = k->versions;
    while (v) {
        if (v->commit_ts <= ts) {
            printf("[Versioned] %s at ts=%d -> %s (commit_ts=%d)\n", keyname, ts, v->value, v->commit_ts);
            return;
        }
        v = v->next;
    }
    printf("[Versioned] %s at ts=%d -> NULL\n", keyname, ts);
}

void tx_write(Transaction *tx, const char *key, const char *val) {
    strncpy(tx->write_set[tx->write_count].key,key,MAX_KEYNAME-1);
    strncpy(tx->write_set[tx->write_count].value,val,127);
    tx->write_count++;
    printf("[TX %d] WRITE buffered %s=%s\n", tx->id, key,val);
}

void tx_commit(Transaction *tx) {
    pthread_mutex_lock(&global_lock);
    commit_ts_t new_ts = ++global_commit_ts;
    for (int i=0;i<tx->write_count;i++) {
        Key *k = get_key(tx->write_set[i].key);
        if (!k) k = create_key(tx->write_set[i].key,"");
        add_version(k,new_ts,tx->write_set[i].value);
        printf("[TX %d] COMMIT %s=%s (ts=%d)\n", tx->id,
               tx->write_set[i].key, tx->write_set[i].value,new_ts);
    }
    tx->state = TX_COMMITTED;
    pthread_mutex_unlock(&global_lock);
}

// Print all versions of a key
void print_versions(const char *keyname) {
    Key *k = get_key(keyname);
    if (!k) return;
    printf("Versions of %s:\n", keyname);
    Version *v = k->versions;
    while(v) {
        printf("  ts=%d -> %s\n", v->commit_ts, v->value);
        v = v->next;
    }
}

int main() {
    create_key("A","initA");
    create_key("B","initB");

    Transaction *t1 = tx_begin();
    tx_read(t1,"A");
    tx_write(t1,"A","100");
    tx_commit(t1);

    Transaction *t2 = tx_begin();
    tx_read(t2,"A");
    tx_write(t2,"A","200");
    tx_commit(t2);

    Transaction *t3 = tx_begin();
    tx_read(t3,"A");

    printf("\n=== Versioned Reads ===\n");
    tx_read_versioned("A",0);
    tx_read_versioned("A",1);
    tx_read_versioned("A",2);

    printf("\n=== All Versions of A ===\n");
    print_versions("A");

    return 0;
}
