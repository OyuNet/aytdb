//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include "kv_store.h"
#include "entry.h"
#include "hash_util.h"
#include "storage.h"
#include <string.h>
#include <stdbool.h>

static Entry table[TABLE_SIZE];
bool logging_enabled = true;

void kv_init() {
    for (int i = 0; i < TABLE_SIZE; ++i) {
        table[i].in_use = 0;
    }

    logging_enabled = false;
    kv_load_from_file();
    logging_enabled = true;
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
    int slot = find_slot(key, 0);

    if (slot != -1) {
        strncpy(table[slot].key, key, MAX_KEY_SIZE);
        strncpy(table[slot].value, value, MAX_VALUE_SIZE);

        table[slot].in_use = 1;

        if (logging_enabled) {
            storage_append_set(key, value);
        }
    }
}

const char* kv_get(const char* key) {
    int slot = find_slot(key, 1);

    if (slot != -1 && table[slot].in_use) {
        return table[slot].value;
    }

    return NULL;
}

void kv_del(const char* key) {
    int slot = find_slot(key, 1);
    if (slot != -1 && table[slot].in_use) {
        table[slot].in_use = 0;

        if (logging_enabled) {
            storage_append_del(key);
        }
    }
}

const Entry* kv_get_all_entries() {
    return table;
}
