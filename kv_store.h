//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include <stdbool.h>
#include <pthread.h>

#ifndef KV_STORE_H
#define KV_STORE_H

#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 1024
#define TABLE_SIZE 1024

#include "entry.h"

void kv_init();
void kv_set(const char *key, const char* value);
void kv_set_with_ttl(const char* key, const char* value, int ttl_seconds);
const char* kv_get(const char *key);
void kv_del(const char *key);
void kv_load_from_file();
const Entry* kv_get_all_entries();
void kv_purge_expired();
void kv_cleanup();

extern bool logging_enabled;
extern pthread_t cleanup_thread;
extern bool cleanup_running;

#endif //KV_STORE_H
