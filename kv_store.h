//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include <stdbool.h>
#include <pthread.h>

#ifndef KV_STORE_H
#define KV_STORE_H

#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 1024
#define INITIAL_TABLE_SIZE 8192  // 2x büyütüyorum başlangıç değerini
#define MAX_TABLE_SIZE 10000000  // Max tablo büyüklüğünü artırıyorum
#define GROWTH_FACTOR 2         // Büyüme faktörünü azaltıyorum daha sık resize etmek için
#define ENTRY_POOL_SIZE 1000000  // Entry pool boyutu - 1 milyon entry
#define ARENA_BLOCK_SIZE (4 * 1024 * 1024)  // 4MB blok boyutu
#define ARENA_MAX_BLOCKS 16     // Maksimum 16 blok (toplam 64MB)

#include "entry.h"

// Arena allocator yapısı
typedef struct {
    void* blocks[ARENA_MAX_BLOCKS];  // Bellek blokları
    size_t block_count;             // Toplam blok sayısı
    size_t current_offset;          // Şu anki blok içindeki pozisyon
    size_t current_block;           // Şu anki blok indeksi
    pthread_mutex_t mutex;          // Eşzamanlılık kilidi
} MemoryArena;

// Memory pool
typedef struct {
    Entry* entries;            // Önceden ayrılmış entry havuzu
    size_t size;              // Havuzdaki toplam giriş sayısı
    size_t used;              // Kullanılan giriş sayısı
    size_t* free_indices;     // Boş giriş indeksleri
    size_t free_count;        // Boş giriş sayısı
    pthread_mutex_t mutex;    // Havuz eşzamanlılık kilidi
} EntryPool;

// Tablo yapısı
typedef struct {
    Entry** entries;          // Entry pointer array
    size_t size;              // Tablo boyutu
    size_t count;             // Kayıt sayısı
    pthread_mutex_t mutex;    // Tablo kilidi
} HashTable;

// Arena allocator işlemleri
void arena_init();
void* arena_alloc(size_t size);
void arena_reset();
void arena_cleanup();

// Memory pool işlemleri
void pool_init();
Entry* pool_alloc();
void pool_free(Entry* entry);
void pool_cleanup();

// KV Store işlemleri
void kv_init();
void kv_set(const char *key, const char* value);
void kv_set_with_ttl(const char* key, const char* value, int ttl_seconds);
const char* kv_get(const char *key);
void kv_del(const char *key);
void kv_load_from_file();
void kv_purge_expired();
void kv_cleanup();

// Tablo yönetimi için fonksiyonlar
void kv_resize(size_t new_size);
size_t kv_get_size();
size_t kv_get_count();
double kv_get_load_factor();
HashTable* kv_get_table();

extern bool logging_enabled;
extern pthread_t cleanup_thread;
extern bool cleanup_running;
extern HashTable* table;
extern EntryPool* entry_pool;

#endif //KV_STORE_H
