//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include "storage.h"
#include "kv_store.h"
#include <stdio.h>
#include <string.h>

#include "entry.h"

#define STORAGE_FILE "AytDB.aof"
#define TEMP_STORAGE_FILE "AytDB.aof.compact"

extern Entry table[TABLE_SIZE];

void storage_append_set(const char* key, const char* value, const int ttl) {
    FILE* f = fopen(STORAGE_FILE, "a");

    if (f) {
        fprintf(f, "SET %s %s %d\n", key, value, ttl);
        fclose(f);
    }
}

void storage_append_del(const char* key) {
    FILE* f = fopen(STORAGE_FILE, "a");

    if (f) {
        fprintf(f, "DEL %s\n", key);
        fclose(f);
    }
}

void storage_load() {
    FILE* f = fopen(STORAGE_FILE, "r");

    if (!f) return;

    char line[MAX_LINE_SIZE];
    char cmd[4];
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    time_t ttl;

    while (fgets(line, sizeof(line), f)) {
        if (sscanf(line, "%3s %255s %1023s %ld", cmd, key, value, &ttl) >= 2) {
            if (strcmp(cmd, "SET") == 0) {
                kv_set_with_ttl(key, value, ttl);
            } else if (strcmp(cmd, "DEL") == 0) {
                kv_del(key);
            }
        }
    }

    fclose(f);
}

void storage_compact() {
    FILE* f = fopen(TEMP_STORAGE_FILE, "w");

    if (!f) return;

    const Entry* entries = kv_get_all_entries();

    time_t now = time(NULL);

    for (int i = 0; i < TABLE_SIZE; ++i) {
        if (entries[i].in_use && (entries[i].expire_at == 0 || entries[i].expire_at > now)) {
            time_t ttl = entries[i].expire_at == 0 ? 0 : entries[i].expire_at - now;
            fprintf(f, "SET %s %s %ld\n", entries[i].key, entries[i].value, ttl);
        }
    }

    fclose(f);

    remove(STORAGE_FILE);
    rename(TEMP_STORAGE_FILE, STORAGE_FILE);
}

long storage_file_size() {
    FILE* f = fopen(STORAGE_FILE, "r");

    if (!f) return 0;

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fclose(f);
    return size;
}