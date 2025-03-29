//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include "storage.h"
#include "kv_store.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "entry.h"

#define STORAGE_FILE "AytDB.aof"
#define TEMP_STORAGE_FILE "AytDB.aof.compact"

extern Entry table[TABLE_SIZE];

static char* parse_quoted_string(char* str, char* result) {
    if (*str != '"') return NULL;
    str++;
    
    while (*str && *str != '"') {
        *result++ = *str++;
    }
    if (*str == '"') str++;
    *result = '\0';
    
    return str;
}

static int parse_storage_line(char* line, char* cmd, char* key, char* value, time_t* ttl) {
    char* current = line;
    
    while (*current == ' ' || *current == '\t') current++;
    char* cmd_start = current;
    while (*current && *current != ' ' && *current != '\t') current++;
    int cmd_len = current - cmd_start;
    if (cmd_len >= 4) cmd_len = 3;
    strncpy(cmd, cmd_start, cmd_len);
    cmd[cmd_len] = '\0';
    
    while (*current == ' ' || *current == '\t') current++;
    
    char* key_start = current;
    while (*current && *current != ' ' && *current != '\t') current++;
    int key_len = current - key_start;
    if (key_len >= MAX_KEY_SIZE) key_len = MAX_KEY_SIZE - 1;
    strncpy(key, key_start, key_len);
    key[key_len] = '\0';
    
    while (*current == ' ' || *current == '\t') current++;
    
    if (*current == '"') {
        current = parse_quoted_string(current, value);
        if (!current) return 0;
    } else {
        char* value_start = current;
        while (*current && *current != ' ' && *current != '\t') current++;
        int value_len = current - value_start;
        if (value_len >= MAX_VALUE_SIZE) value_len = MAX_VALUE_SIZE - 1;
        strncpy(value, value_start, value_len);
        value[value_len] = '\0';
    }
    
    while (*current == ' ' || *current == '\t') current++;
    
    if (*current) {
        *ttl = atol(current);
    } else {
        *ttl = 0;
    }
    
    return 1;
}

void storage_append_set(const char* key, const char* value, const int ttl) {
    FILE* f = fopen(STORAGE_FILE, "a");

    if (f) {
        fprintf(f, "SET %s \"%s\" %d\n", key, value, ttl);
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
        line[strcspn(line, "\n")] = 0;
        
        if (parse_storage_line(line, cmd, key, value, &ttl)) {
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
            fprintf(f, "SET %s \"%s\" %ld\n", entries[i].key, entries[i].value, ttl);
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