//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include <stdbool.h>

#ifndef KV_STORE_H
#define KV_STORE_H

#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 1024
#define TABLE_SIZE 1024

void kv_init();
void kv_set(const char *key, const char* value);
const char* kv_get(const char *key);
void kv_del(const char *key);
void kv_load_from_file();

extern bool logging_enabled;

#endif //KV_STORE_H
