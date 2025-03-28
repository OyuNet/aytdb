//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include "kv_store.h"
#include "entry.h"
#include "hash_util.h"
#include "storage.h"
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>

static Entry table[TABLE_SIZE];
bool logging_enabled = true;
pthread_t cleanup_thread;
bool cleanup_running = false;

static void* cleanup_loop(void* arg) {
    while (cleanup_running) {
        kv_purge_expired();
        sleep(1);
    }
    return NULL;
}

void kv_purge_expired() {
    time_t now = time(NULL);
    for (int i = 0; i < TABLE_SIZE; ++i) {
        if (table[i].in_use && table[i].expire_at > 0 && now > table[i].expire_at) {
            kv_del(table[i].key);
        }
    }
}

void kv_cleanup() {
    cleanup_running = false;
    pthread_join(cleanup_thread, NULL);
}

void kv_init() {
    for (int i = 0; i < TABLE_SIZE; ++i) {
        table[i].in_use = 0;
        table[i].expire_at = 0;
    }

    logging_enabled = false;
    kv_load_from_file();
    logging_enabled = true;

    cleanup_running = true;
    pthread_create(&cleanup_thread, NULL, cleanup_loop, NULL);
}

void kv_load_from_file() {
    storage_load();
}

static int find_slot(const char* key, int find_existing) {
    unsigned int index = hash_key(key);

    for (int i = 0; i < TABLE_SIZE; ++i) {
        unsigned int try = (index + i) % TABLE_SIZE;

        if (table[try].in_use) {
            if (strcmp(table[try].key, key) == 0) {
                return try;
            }
        } else if (!find_existing) {
            return try;
        }
    }

    return -1;
}

void kv_set(const char* key, const char* value) {
    kv_set_with_ttl(key, value, 0);
}

void kv_set_with_ttl(const char* key, const char* value, int ttl_seconds) {
    int slot = find_slot(key, 0);

    if (slot != -1) {
        strncpy(table[slot].key, key, MAX_KEY_SIZE);
        strncpy(table[slot].value, value, MAX_VALUE_SIZE);

        table[slot].in_use = 1;
        if (ttl_seconds <= 0) {
            table[slot].expire_at = 0;  // TTL yok
        } else {
            table[slot].expire_at = time(NULL) + ttl_seconds;
        }

        if (logging_enabled) {
            storage_append_set(key, value, ttl_seconds);
            if (storage_file_size() > MAX_STORAGE_SIZE) {
                storage_compact();
            }
        }
    }
}

const char* kv_get(const char* key) {
    int slot = find_slot(key, 1);

    if (slot != -1 && table[slot].in_use) {
        time_t now = time(NULL);
        if (table[slot].expire_at > 0 && now > table[slot].expire_at) {
            kv_del(key);
            return NULL;
        }
        return table[slot].value;
    }

    return NULL;
}

void kv_del(const char* key) {
    int slot = find_slot(key, 1);
    if (slot != -1 && table[slot].in_use) {

        table[slot].in_use = 0;
        table[slot].expire_at = 0;

        if (logging_enabled) {
            storage_append_del(key);

            if (storage_file_size() > MAX_STORAGE_SIZE) {
                storage_compact();
            }
        }
    }
}

const Entry* kv_get_all_entries() {
    return table;
}
