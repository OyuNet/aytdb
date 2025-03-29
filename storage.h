//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#ifndef STORAGE_H
#define STORAGE_H

#include <stdbool.h>
#include <time.h>
#include <stdio.h>

#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 1024
#define MAX_LINE_SIZE (4 + MAX_KEY_SIZE + MAX_VALUE_SIZE + 4)
#define MAX_STORAGE_SIZE 1024 * 1024 // 1 MB

typedef struct {
    char* file_path;
    FILE* file;
} Storage;

// Storage yönetimi
Storage* storage_init(void);
void storage_free(Storage* storage);

// Temel operasyonlar
bool storage_set(Storage* storage, const char* key, const char* value);
bool storage_set_with_ttl(Storage* storage, const char* key, const char* value, int ttl);
char* storage_get(Storage* storage, const char* key);
bool storage_delete(Storage* storage, const char* key);

// Dosya işlemleri
void storage_append_set(const char* key, const char* value, const int ttl);
void storage_append_del(const char* key);
void storage_load();
void storage_compact();
long storage_file_size();

// Snapshot işlemleri
bool storage_save_snapshot();
bool storage_load_snapshot();
void storage_schedule_snapshot(int interval_seconds);

#endif //STORAGE_H
