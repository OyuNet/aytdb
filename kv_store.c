//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#include "kv_store.h"
#include "hash_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef __ARM_NEON
#include <arm_neon.h>
#define HAVE_SIMD 1
#elif defined(__SSE4_1__)
#include <smmintrin.h>
#define HAVE_SIMD 1
#else
#define HAVE_SIMD 0
#endif

// Global değişkenler
bool logging_enabled = false; // Debug mesajlarını kapatıyorum performans için
pthread_t cleanup_thread;
bool cleanup_running = false;
HashTable* table = NULL; // İsimlendirmeyi düzeltiyorum
EntryPool* entry_pool = NULL; // Entry pool
static __thread char value_buffer[MAX_VALUE_SIZE]; // Thread-local buffer ekleyerek thread güvenliği sağlıyorum
MemoryArena* global_arena = NULL; // Global arena allocator

// İleri tanımlamalar
static void check_and_resize(void);
static size_t find_slot(const char* key, bool* found);

// Memory pool işlemleri
void pool_init() {
    if (entry_pool) {
        pool_cleanup();
    }
    
    // Önce arena'yı başlat
    arena_init();
    
    // Entry pool'u arena allocator'dan al
    entry_pool = arena_alloc(sizeof(EntryPool));
    if (__builtin_expect(!entry_pool, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate entry pool\n");
        return;
    }
    
    entry_pool->size = ENTRY_POOL_SIZE;
    entry_pool->used = 0;
    entry_pool->free_count = 0;
    
    // Büyük bir entry dizisi oluştur - arena allocator'dan
    entry_pool->entries = arena_alloc(ENTRY_POOL_SIZE * sizeof(Entry));
    if (__builtin_expect(!entry_pool->entries, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate entry pool entries\n");
        entry_pool = NULL;
        return;
    }
    
    // Tüm entry'leri sıfırla
    memset(entry_pool->entries, 0, ENTRY_POOL_SIZE * sizeof(Entry));
    
    // Serbest indeks dizisi oluştur - arena allocator'dan
    entry_pool->free_indices = arena_alloc(ENTRY_POOL_SIZE * sizeof(size_t));
    if (__builtin_expect(!entry_pool->free_indices, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate entry pool free indices\n");
        entry_pool = NULL;
        return;
    }
    
    // Mutex başlat
    if (__builtin_expect(pthread_mutex_init(&entry_pool->mutex, NULL) != 0, 0)) {
        if (logging_enabled) printf("ERROR: Failed to initialize entry pool mutex\n");
        entry_pool = NULL;
        return;
    }
}

Entry* pool_alloc() {
    if (__builtin_expect(!entry_pool, 0)) {
        return NULL;
    }
    
    Entry* result = NULL;
    pthread_mutex_lock(&entry_pool->mutex);
    
    // İlk olarak serbest listeyi kontrol et
    if (entry_pool->free_count > 0) {
        size_t index = entry_pool->free_indices[--entry_pool->free_count];
        result = &entry_pool->entries[index];
    } 
    // Eğer serbest giriş yoksa ve havuzda yer varsa yeni bir girişi kullanıma al
    else if (entry_pool->used < entry_pool->size) {
        result = &entry_pool->entries[entry_pool->used++];
    }
    
    pthread_mutex_unlock(&entry_pool->mutex);
    
    // Yeni entry'yi sıfırla
    if (result) {
        memset(result, 0, sizeof(Entry));
    }
    
    return result;
}

void pool_free(Entry* entry) {
    if (__builtin_expect(!entry_pool || !entry, 0)) {
        return;
    }
    
    // Adres aralığında mı kontrol et
    if (entry < entry_pool->entries || 
        entry >= &entry_pool->entries[entry_pool->size]) {
        return;
    }
    
    // Entry indeksini hesapla
    size_t entry_index = entry - entry_pool->entries;
    
    pthread_mutex_lock(&entry_pool->mutex);
    // Serbest indeks listesine ekle
    if (entry_pool->free_count < entry_pool->size) {
        entry_pool->free_indices[entry_pool->free_count++] = entry_index;
    }
    pthread_mutex_unlock(&entry_pool->mutex);
}

void pool_cleanup() {
    if (__builtin_expect(!entry_pool, 0)) return;
    
    pthread_mutex_destroy(&entry_pool->mutex);
    entry_pool = NULL;
}

// Optimize edilmiş string karşılaştırma fonksiyonu
static inline int simd_strcmp(const char* a, const char* b) {
#if defined(__ARM_NEON) && HAVE_SIMD
    // ARM NEON için 16-byte blok karşılaştırma
    while (1) {
        // 16-byte veri bloklarını yükle
        uint8x16_t block_a = vld1q_u8((const uint8_t*)a);
        uint8x16_t block_b = vld1q_u8((const uint8_t*)b);
        
        // NULL byte kontrolü
        uint8x16_t zero_mask = vceqq_u8(block_a, vdupq_n_u8(0));
        if (vgetq_lane_u8(zero_mask, 0) || vgetq_lane_u8(zero_mask, 1) ||
            vgetq_lane_u8(zero_mask, 2) || vgetq_lane_u8(zero_mask, 3) ||
            vgetq_lane_u8(zero_mask, 4) || vgetq_lane_u8(zero_mask, 5) ||
            vgetq_lane_u8(zero_mask, 6) || vgetq_lane_u8(zero_mask, 7) ||
            vgetq_lane_u8(zero_mask, 8) || vgetq_lane_u8(zero_mask, 9) ||
            vgetq_lane_u8(zero_mask, 10) || vgetq_lane_u8(zero_mask, 11) ||
            vgetq_lane_u8(zero_mask, 12) || vgetq_lane_u8(zero_mask, 13) ||
            vgetq_lane_u8(zero_mask, 14) || vgetq_lane_u8(zero_mask, 15)) {
            // NULL byte bulundu, normal strcmp kullan
            return strcmp(a, b);
        }
        
        // Bloklar eşit mi karşılaştır
        uint8x16_t cmp_mask = vceqq_u8(block_a, block_b);
        uint64x2_t cmp_mask64 = vreinterpretq_u64_u8(cmp_mask);

        // Eşit değilse, normal strcmp kullan
        if (vgetq_lane_u64(cmp_mask64, 0) != UINT64_MAX || 
            vgetq_lane_u64(cmp_mask64, 1) != UINT64_MAX) {
            return strcmp(a, b);
        }
        
        // 16 byte ilerle
        a += 16;
        b += 16;
    }
#else
    // Normal strcmp kullan
    return strcmp(a, b);
#endif
}

// Optimize edilmiş string kopyalama fonksiyonu (strncpy yerine)
static inline void simd_strcpy(char* dst, const char* src, size_t max_len) {
#if defined(__ARM_NEON) && HAVE_SIMD
    size_t i = 0;
    
    // 16-byte (128-bit) bloklar ile kopyalama
    for (; i + 16 <= max_len; i += 16) {
        uint8x16_t block = vld1q_u8((const uint8_t*)(src + i));
        vst1q_u8((uint8_t*)(dst + i), block);
        
        // NULL byte kontrolü
        uint8x16_t zero_mask = vceqq_u8(block, vdupq_n_u8(0));
        if (vgetq_lane_u8(zero_mask, 0) || vgetq_lane_u8(zero_mask, 1) ||
            vgetq_lane_u8(zero_mask, 2) || vgetq_lane_u8(zero_mask, 3) ||
            vgetq_lane_u8(zero_mask, 4) || vgetq_lane_u8(zero_mask, 5) ||
            vgetq_lane_u8(zero_mask, 6) || vgetq_lane_u8(zero_mask, 7) ||
            vgetq_lane_u8(zero_mask, 8) || vgetq_lane_u8(zero_mask, 9) ||
            vgetq_lane_u8(zero_mask, 10) || vgetq_lane_u8(zero_mask, 11) ||
            vgetq_lane_u8(zero_mask, 12) || vgetq_lane_u8(zero_mask, 13) ||
            vgetq_lane_u8(zero_mask, 14) || vgetq_lane_u8(zero_mask, 15)) {
            return; // NULL byte bulundu
        }
    }
    
    // Kalan karakterleri normal şekilde kopyala
    for (; i < max_len && src[i]; i++) {
        dst[i] = src[i];
    }
    
    // NULL terminasyon garantile
    if (i < max_len) {
        dst[i] = '\0';
    } else if (max_len > 0) {
        dst[max_len - 1] = '\0';
    }
#else
    // Normal strncpy kullan
    strncpy(dst, src, max_len);
#endif
}

static void* cleanup_loop(void* arg) {
    while (cleanup_running) {
        kv_purge_expired();
        sleep(5); // TTL temizliği için süreyi 5 saniyeye çıkarıyorum
    }
    return NULL;
}

// Optimize edilmiş slot bulma fonksiyonu
static size_t find_slot(const char* key, bool* found) {
    if (__builtin_expect(!table || !key, 0)) {
        *found = false;
        return 0;
    }
    
    uint32_t key_hash = (uint32_t)hash(key);
    size_t original_index = key_hash % table->size;
    size_t index = original_index;
    size_t first_empty = table->size;
    bool empty_found = false;
    
    // Double hashing için ikinci hash değeri hesapla
    const size_t step = 1 + (key_hash % (table->size - 1));
    
    // Double hashing ile slot ara - maximum probe sayısı sınırlı
    const size_t max_probes = table->size > 1000 ? 100 : table->size / 10;
    size_t probe_count = 0;
    
    // Döngü unroll faktörü
    const int UNROLL_FACTOR = 4;
    const size_t unrolled_max = max_probes - (max_probes % UNROLL_FACTOR);
    
    // Ana bölüm - 4'lü unroll edilmiş
    while (probe_count < unrolled_max) {
        // İterasyon 1
        if (__builtin_expect(table->entries[index] == NULL, 0)) {
            if (!empty_found) {
                first_empty = index;
                empty_found = true;
            }
            if (!*found) {
                *found = false;
                return first_empty;
            }
        } else if (__builtin_expect((table->entries[index]->hash == key_hash), 0)) {
            if (__builtin_expect(simd_strcmp(table->entries[index]->key, key) == 0, 1)) {
                *found = true;
                return index;
            }
        }
        index = (index + step) % table->size;
        
        // İterasyon 2
        if (__builtin_expect(table->entries[index] == NULL, 0)) {
            if (!empty_found) {
                first_empty = index;
                empty_found = true;
            }
            if (!*found) {
                *found = false;
                return first_empty;
            }
        } else if (__builtin_expect((table->entries[index]->hash == key_hash), 0)) {
            if (__builtin_expect(simd_strcmp(table->entries[index]->key, key) == 0, 1)) {
                *found = true;
                return index;
            }
        }
        index = (index + step) % table->size;
        
        // İterasyon 3
        if (__builtin_expect(table->entries[index] == NULL, 0)) {
            if (!empty_found) {
                first_empty = index;
                empty_found = true;
            }
            if (!*found) {
                *found = false;
                return first_empty;
            }
        } else if (__builtin_expect((table->entries[index]->hash == key_hash), 0)) {
            if (__builtin_expect(simd_strcmp(table->entries[index]->key, key) == 0, 1)) {
                *found = true;
                return index;
            }
        }
        index = (index + step) % table->size;
        
        // İterasyon 4
        if (__builtin_expect(table->entries[index] == NULL, 0)) {
            if (!empty_found) {
                first_empty = index;
                empty_found = true;
            }
            if (!*found) {
                *found = false;
                return first_empty;
            }
        } else if (__builtin_expect((table->entries[index]->hash == key_hash), 0)) {
            if (__builtin_expect(simd_strcmp(table->entries[index]->key, key) == 0, 1)) {
                *found = true;
                return index;
            }
        }
        index = (index + step) % table->size;
        
        probe_count += UNROLL_FACTOR;
    }
    
    // Kalan problar için normal döngü
    while (probe_count < max_probes) {
        if (__builtin_expect(table->entries[index] == NULL, 0)) {
            if (!empty_found) {
                first_empty = index;
                empty_found = true;
            }
            if (!*found) {
                *found = false;
                return first_empty;
            }
        }
        
        if (__builtin_expect(table->entries[index] != NULL && table->entries[index]->hash == key_hash, 0)) {
            if (__builtin_expect(simd_strcmp(table->entries[index]->key, key) == 0, 1)) {
                *found = true;
                return index;
            }
        }
        
        probe_count++;
        index = (original_index + probe_count * step) % table->size;
    }
    
    // Aşırı probing durumunda uyarı logla (statik bir sayaç ile sınırla)
    static int warning_count = 0;
    if (warning_count < 10) {
        if (logging_enabled) printf("WARN: Forced resize due to excessive probing for key: %s\n", key);
        warning_count++;
    }
    
    // Sınırlı sayıda probeden sonra key bulunamadı
    *found = false;
    return empty_found ? first_empty : original_index;
}

void kv_purge_expired() {
    if (__builtin_expect(!table, 0)) return;
    
    time_t now = time(NULL);
    pthread_mutex_lock(&table->mutex);
    
    size_t purged = 0;
    for (size_t i = 0; i < table->size; i++) {
        if (__builtin_expect(table->entries[i] != NULL, 0)) {
            if (__builtin_expect(table->entries[i]->expire_at > 0 && now > table->entries[i]->expire_at, 0)) {
                // Entry'yi pool'a geri ver
                Entry* entry_to_free = table->entries[i];
                table->entries[i] = NULL;
                pool_free(entry_to_free);
                table->count--;
                purged++;
                
                // Her 1000 temizlemeden sonra kilidi geçici olarak bırak (diğer thread'lerin çalışmasına izin ver)
                if (__builtin_expect(purged % 1000 == 0, 0)) {
                    pthread_mutex_unlock(&table->mutex);
                    pthread_mutex_lock(&table->mutex);
                }
            }
        }
    }
    
    pthread_mutex_unlock(&table->mutex);
}

void kv_init() {
    // İlk olarak memory pool'u başlat
    pool_init();
    
    if (__builtin_expect(table != NULL, 0)) {
        kv_cleanup();
    }

    table = arena_alloc(sizeof(HashTable));
    if (__builtin_expect(!table, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate hash table\n");
        return;
    }

    // Şimdi pointer array olarak entries oluşturuyoruz
    table->entries = arena_alloc(INITIAL_TABLE_SIZE * sizeof(Entry*));
    if (__builtin_expect(!table->entries, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate table entries\n");
        table = NULL;
        return;
    }
    
    // Tüm entry'leri sıfırla
    memset(table->entries, 0, INITIAL_TABLE_SIZE * sizeof(Entry*));

    table->size = INITIAL_TABLE_SIZE;
    table->count = 0;
    
    if (__builtin_expect(pthread_mutex_init(&table->mutex, NULL) != 0, 0)) {
        if (logging_enabled) printf("ERROR: Failed to initialize mutex\n");
        table = NULL;
        return;
    }

    cleanup_running = true;
    if (__builtin_expect(pthread_create(&cleanup_thread, NULL, cleanup_loop, NULL) != 0, 0)) {
        if (logging_enabled) printf("ERROR: Failed to create cleanup thread\n");
        pthread_mutex_destroy(&table->mutex);
        table = NULL;
        return;
    }
}

void kv_set(const char* key, const char* value) {
    if (__builtin_expect(!table || !key || !value, 0)) return;

    pthread_mutex_lock(&table->mutex);
    
    bool found;
    size_t index = find_slot(key, &found);
    
    // Tablo doluluk oranını kontrol et
    // Sonsuz döngüden kaçınmak için resize işlemini sınırla
    static int resize_count = 0;
    const int MAX_CONSECUTIVE_RESIZES = 3;
    
    if (__builtin_expect(!found && (double)(table->count + 1) / table->size > 0.60, 0)) {
        resize_count++;
        if (resize_count <= MAX_CONSECUTIVE_RESIZES) {
            pthread_mutex_unlock(&table->mutex);
            kv_resize(table->size * GROWTH_FACTOR);
            pthread_mutex_lock(&table->mutex);
            index = find_slot(key, &found);
        } else {
            if (logging_enabled) printf("WARN: Too many consecutive resizes, skipping resize for key: %s\n", key);
            resize_count = 0; // Sayacı sıfırla
        }
    } else {
        resize_count = 0; // Başarılı bir işlem olduğunda sayacı sıfırla
    }
    
    // Hash'i hesaplayalım (find_slot zaten hesaplıyor ama yine de)
    uint32_t key_hash = (uint32_t)hash(key);
    
    // Değeri güvenli bir şekilde kopyala
    if (__builtin_expect(found, 1)) {
        // SIMD ile değeri kopyala
        simd_strcpy(table->entries[index]->value, value, MAX_VALUE_SIZE - 1);
        table->entries[index]->value[MAX_VALUE_SIZE - 1] = '\0';
        table->entries[index]->expire_at = 0;
        // Hash değeri zaten mevcut
    } else {
        // Memory pool'dan yeni bir entry al
        Entry* new_entry = pool_alloc();
        if (__builtin_expect(!new_entry, 0)) {
            pthread_mutex_unlock(&table->mutex);
            if (logging_enabled) printf("ERROR: Failed to allocate new entry from pool\n");
            return;
        }
        
        // SIMD ile anahtarı ve değeri kopyala
        simd_strcpy(new_entry->key, key, MAX_KEY_SIZE - 1);
        new_entry->key[MAX_KEY_SIZE - 1] = '\0';
        simd_strcpy(new_entry->value, value, MAX_VALUE_SIZE - 1);
        new_entry->value[MAX_VALUE_SIZE - 1] = '\0';
        new_entry->expire_at = 0;
        new_entry->hash = key_hash; // Hash değerini kaydet
        
        // Entry'yi tabloya ekle
        table->entries[index] = new_entry;
        table->count++;
    }

    pthread_mutex_unlock(&table->mutex);
}

void kv_set_with_ttl(const char* key, const char* value, int ttl_seconds) {
    if (__builtin_expect(!table || !key || !value, 0)) return;

    pthread_mutex_lock(&table->mutex);
    
    bool found;
    size_t index = find_slot(key, &found);
    
    // Tablo doluluk oranını kontrol et
    // Sonsuz döngüden kaçınmak için resize işlemini sınırla
    static int resize_count = 0;
    const int MAX_CONSECUTIVE_RESIZES = 3;
    
    if (__builtin_expect(!found && (double)(table->count + 1) / table->size > 0.60, 0)) {
        resize_count++;
        if (resize_count <= MAX_CONSECUTIVE_RESIZES) {
            pthread_mutex_unlock(&table->mutex);
            kv_resize(table->size * GROWTH_FACTOR);
            pthread_mutex_lock(&table->mutex);
            index = find_slot(key, &found);
        } else {
            if (logging_enabled) printf("WARN: Too many consecutive resizes, skipping resize for key with TTL: %s\n", key);
            resize_count = 0; // Sayacı sıfırla
        }
    } else {
        resize_count = 0; // Başarılı bir işlem olduğunda sayacı sıfırla
    }
    
    // Hash'i hesaplayalım
    uint32_t key_hash = (uint32_t)hash(key);
    
    if (__builtin_expect(found, 1)) {
        // SIMD ile değeri kopyala
        simd_strcpy(table->entries[index]->value, value, MAX_VALUE_SIZE - 1);
        table->entries[index]->value[MAX_VALUE_SIZE - 1] = '\0';
        table->entries[index]->expire_at = ttl_seconds > 0 ? time(NULL) + ttl_seconds : 0;
        // Hash değeri zaten mevcut
    } else {
        // Memory pool'dan yeni bir entry al
        Entry* new_entry = pool_alloc();
        if (__builtin_expect(!new_entry, 0)) {
            pthread_mutex_unlock(&table->mutex);
            if (logging_enabled) printf("ERROR: Failed to allocate new entry from pool\n");
            return;
        }
        
        // SIMD ile anahtarı ve değeri kopyala
        simd_strcpy(new_entry->key, key, MAX_KEY_SIZE - 1);
        new_entry->key[MAX_KEY_SIZE - 1] = '\0';
        simd_strcpy(new_entry->value, value, MAX_VALUE_SIZE - 1);
        new_entry->value[MAX_VALUE_SIZE - 1] = '\0';
        new_entry->expire_at = ttl_seconds > 0 ? time(NULL) + ttl_seconds : 0;
        new_entry->hash = key_hash; // Hash değerini kaydet
        
        // Entry'yi tabloya ekle
        table->entries[index] = new_entry;
        table->count++;
    }

    pthread_mutex_unlock(&table->mutex);
}

const char* kv_get(const char* key) {
    if (__builtin_expect(!table || !key, 0)) return NULL;

    // Özel sorunlu anahtarı kontrol et
    bool is_problem_key = strncmp(key, "resize_key_3205", 15) == 0;
    if (is_problem_key) {
        printf("DEBUG: Trying to get problem key: %s\n", key);
    }
    
    pthread_mutex_lock(&table->mutex);
    
    bool found;
    size_t index = find_slot(key, &found);
    
    if (is_problem_key) {
        printf("DEBUG: Problem key find_slot result: found=%d, index=%zu\n", found, index);
        
        // Tablo içeriğini tarayarak anahtarı doğrudan arayalım
        bool manual_found = false;
        size_t manual_index = 0;
        for (size_t i = 0; i < table->size; i++) {
            if (table->entries[i] != NULL && strcmp(table->entries[i]->key, key) == 0) {
                manual_found = true;
                manual_index = i;
                printf("DEBUG: Manually found problem key at index %zu\n", i);
                break;
            }
        }
        
        if (!manual_found) {
            printf("DEBUG: Problem key not found in entire table (size=%zu)\n", table->size);
        }
    }
    
    if (__builtin_expect(!found, 0)) {
        pthread_mutex_unlock(&table->mutex);
        return NULL;
    }
    
    time_t now = time(NULL);
    if (__builtin_expect(table->entries[index]->expire_at > 0 && now > table->entries[index]->expire_at, 0)) {
        // Süresi dolmuş entry
        Entry* expired_entry = table->entries[index];
        table->entries[index] = NULL;
        pool_free(expired_entry);
        table->count--;
        pthread_mutex_unlock(&table->mutex);
        return NULL;
    }
    
    // Thread-local buffer'a değeri SIMD ile kopyala
    simd_strcpy(value_buffer, table->entries[index]->value, MAX_VALUE_SIZE - 1);
    value_buffer[MAX_VALUE_SIZE - 1] = '\0';
    
    pthread_mutex_unlock(&table->mutex);
    return value_buffer;
}

void kv_del(const char* key) {
    if (__builtin_expect(!table || !key, 0)) return;

    pthread_mutex_lock(&table->mutex);
    
    bool found;
    size_t index = find_slot(key, &found);
    
    if (__builtin_expect(found, 1)) {
        Entry* entry_to_free = table->entries[index];
        table->entries[index] = NULL;
        pool_free(entry_to_free);
        table->count--;
    }
    
    pthread_mutex_unlock(&table->mutex);
}

void kv_cleanup() {
    if (__builtin_expect(!table, 0)) return;
    
    cleanup_running = false;
    pthread_join(cleanup_thread, NULL);
    
    // Tüm entry'leri serbest bırak
    // Not: Aslında entry'leri tek tek serbest bırakmaya gerek yok 
    // çünkü arena_reset/cleanup zaten tüm belleği temizleyecek, 
    // ama pool'a ayrı ayrı free işaretliyoruz
    for (size_t i = 0; i < table->size; i++) {
        if (table->entries[i] != NULL) {
            pool_free(table->entries[i]);
            table->entries[i] = NULL;
        }
    }
    
    pthread_mutex_destroy(&table->mutex);
    table = NULL;
    
    // Memory pool'u ve arena allocator'ı temizle
    pool_cleanup();
    arena_reset(); // Tüm alanı sıfırla ancak belleği serbest bırakma
    
    // Programın sonunda çağrılacak - tüm belleği serbest bırak
    // arena_cleanup();
}

void kv_resize(size_t new_size) {
    if (__builtin_expect(!table, 0)) return;
    if (__builtin_expect(new_size < INITIAL_TABLE_SIZE, 0)) new_size = INITIAL_TABLE_SIZE;
    if (__builtin_expect(new_size > MAX_TABLE_SIZE, 0)) new_size = MAX_TABLE_SIZE;
    
    // Eğer şu anki boyut yeni boyuttan büyük veya eşitse ve yeni boyut minimum boyuttan büyükse,
    // resize işlemini iptal et (sonsuz döngüyü önlemek için)
    if (table->size >= new_size && new_size > INITIAL_TABLE_SIZE) {
        if (logging_enabled) printf("INFO: Resize canceled - current size %zu >= new size %zu\n", table->size, new_size);
        return;
    }
    
    if (logging_enabled) printf("INFO: Resizing table from %zu to %zu\n", table->size, new_size);
    
    pthread_mutex_lock(&table->mutex);
    
    Entry** old_entries = table->entries;
    size_t old_size = table->size;
    size_t old_count = table->count;
    
    // Arena allocator kullanarak yeni entries dizisi oluştur
    Entry** new_entries = arena_alloc(new_size * sizeof(Entry*));
    if (__builtin_expect(!new_entries, 0)) {
        pthread_mutex_unlock(&table->mutex);
        return;
    }
    
    // Yeni diziyi sıfırla
    memset(new_entries, 0, new_size * sizeof(Entry*));
    
    table->entries = new_entries;
    table->size = new_size;
    table->count = 0;
    
    // Kilidi geçici olarak serbest bırak - yeni boyut ayarlandı
    pthread_mutex_unlock(&table->mutex);
    
    // Eski değerleri yeni tabloya yükle
    time_t now = time(NULL);
    
    // Her bir entry için taşıma yap
    size_t successfully_moved = 0;
    bool problem_key_found = false; // resize_key_3205 için kontrol
    
    for (size_t i = 0; i < old_size; i++) {
        if (__builtin_expect(old_entries[i] != NULL, 0)) {
            // Özel anahtarı kontrol et (sorunlu anahtar)
            bool is_problem_key = strncmp(old_entries[i]->key, "resize_key_3205", 15) == 0;
            if (is_problem_key) {
                printf("DEBUG: Found problem key: %s at index %zu\n", old_entries[i]->key, i);
                problem_key_found = true;
            }
            
            // Süresi dolmamış olanları ekle
            if (__builtin_expect(old_entries[i]->expire_at == 0 || old_entries[i]->expire_at > now, 1)) {
                // Doğrudan insert et, kv_set kullanma (recursive resize önlenir)
                pthread_mutex_lock(&table->mutex);
                
                // Mevcut hash değerini kullan
                uint32_t key_hash = old_entries[i]->hash;
                
                // Yeni tablo için indeks hesapla
                size_t index = key_hash % new_size;
                size_t step = 1 + (key_hash % (new_size - 1));
                size_t probe_count = 0;
                size_t original_index = index;
                
                // Eğer sorunlu anahtarsa daha fazla log ekle
                if (is_problem_key) {
                    printf("DEBUG: Problem key hash: %u, initial index: %zu, step: %zu\n", 
                           key_hash, index, step);
                }
                
                // Uygun boş yer bul - linear probing ile
                while (table->entries[index] != NULL && probe_count < new_size) {
                    probe_count++;
                    index = (original_index + probe_count * step) % new_size;
                    
                    // Sorunlu anahtarsa ve probing devam ediyorsa log ekle
                    if (is_problem_key && probe_count % 10 == 0) {
                        printf("DEBUG: Problem key probing - count: %zu, current index: %zu\n", 
                               probe_count, index);
                    }
                }
                
                // Eğer boş yer bulunamazsa (olmaması gereken durum)
                if (__builtin_expect(probe_count >= new_size, 0)) {
                    if (logging_enabled || is_problem_key) {
                        printf("ERROR: Failed to find slot during resize for key: %s\n", old_entries[i]->key);
                    }
                    pthread_mutex_unlock(&table->mutex);
                    pool_free(old_entries[i]);
                    continue;
                }
                
                // Entry'yi doğrudan yeni konuma yerleştir
                table->entries[index] = old_entries[i];
                table->count++;
                successfully_moved++;
                
                // Sorunlu anahtarı nereye yerleştirdiğimizi log ekle
                if (is_problem_key) {
                    printf("DEBUG: Problem key placed at index %zu after %zu probes\n", 
                           index, probe_count);
                }
                
                pthread_mutex_unlock(&table->mutex);
            } else {
                // Süresi dolmuş entry'yi havuza geri ver
                pool_free(old_entries[i]);
            }
        }
    }
    
    // Eğer sorunlu anahtarı bulamadıysak veya yerleştiremedik, özel olarak ekleyelim
    if (!problem_key_found) {
        printf("DEBUG: Problem key (resize_key_3205) not found in original table\n");
    } else {
        // Sorunlu anahtarı aramaya çalışalım
        bool found = false;
        size_t index = find_slot("resize_key_3205", &found);
        printf("DEBUG: After resize - problem key search result: found=%d, index=%zu\n", found, index);
    }
    
    // Count değeri yeni taşınandan farklıysa, kilitle ve düzelt
    pthread_mutex_lock(&table->mutex);
    if (__builtin_expect(table->count != old_count, 0)) {
        if (logging_enabled) {
            printf("INFO: Resize changed count from %zu to %zu (moved %zu entries)\n", 
                   old_count, table->count, successfully_moved);
        }
    }
    pthread_mutex_unlock(&table->mutex);
    
    if (logging_enabled) {
        printf("INFO: Resize completed: %zu -> %zu, moved %zu / %zu entries\n", 
               old_size, new_size, successfully_moved, old_count);
    }
}

// Yardımcı fonksiyonlar
size_t kv_get_size() {
    return table ? table->size : 0;
}

size_t kv_get_count() {
    return table ? table->count : 0;
}

double kv_get_load_factor() {
    return table ? (double)table->count / table->size : 0;
}

HashTable* kv_get_table() {
    return table;
}

static void check_and_resize() {
    if (__builtin_expect(!table, 0)) return;
    
    double load_factor = kv_get_load_factor();
    if (__builtin_expect(load_factor > 0.7, 0)) {
        kv_resize(table->size * GROWTH_FACTOR);
    }
}

// Arena allocator işlemleri
void arena_init() {
    if (global_arena) {
        arena_cleanup();
    }
    
    global_arena = malloc(sizeof(MemoryArena));
    if (__builtin_expect(!global_arena, 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate memory arena\n");
        return;
    }
    
    global_arena->block_count = 0;
    global_arena->current_block = 0;
    global_arena->current_offset = 0;
    
    for (size_t i = 0; i < ARENA_MAX_BLOCKS; i++) {
        global_arena->blocks[i] = NULL;
    }
    
    // İlk bloku ayır
    global_arena->blocks[0] = malloc(ARENA_BLOCK_SIZE);
    if (__builtin_expect(!global_arena->blocks[0], 0)) {
        if (logging_enabled) printf("ERROR: Failed to allocate initial arena block\n");
        free(global_arena);
        global_arena = NULL;
        return;
    }
    
    global_arena->block_count = 1;
    
    // Mutex başlat
    if (__builtin_expect(pthread_mutex_init(&global_arena->mutex, NULL) != 0, 0)) {
        if (logging_enabled) printf("ERROR: Failed to initialize arena mutex\n");
        free(global_arena->blocks[0]);
        free(global_arena);
        global_arena = NULL;
        return;
    }
}

void* arena_alloc(size_t size) {
    if (__builtin_expect(!global_arena, 0)) {
        arena_init();
        if (!global_arena) return NULL;
    }
    
    // Büyük allocationsları doğrudan işleyelim
    if (__builtin_expect(size > ARENA_BLOCK_SIZE / 4, 0)) {
        return malloc(size);
    }
    
    pthread_mutex_lock(&global_arena->mutex);
    
    // Align to 8 bytes
    size = (size + 7) & ~7;
    
    // Eğer blokta yeterli yer yoksa, yeni blok oluştur
    if (global_arena->current_offset + size > ARENA_BLOCK_SIZE) {
        global_arena->current_block++;
        global_arena->current_offset = 0;
        
        // Eğer blok indisi sınırın dışına çıkıyorsa, sıfırla
        if (global_arena->current_block >= ARENA_MAX_BLOCKS) {
            if (logging_enabled) printf("WARN: Arena ran out of blocks, recycling block 0\n");
            global_arena->current_block = 0;
        }
        
        // Eğer bu blok henüz ayrılmamışsa
        if (!global_arena->blocks[global_arena->current_block]) {
            global_arena->blocks[global_arena->current_block] = malloc(ARENA_BLOCK_SIZE);
            if (!global_arena->blocks[global_arena->current_block]) {
                if (logging_enabled) printf("ERROR: Failed to allocate new arena block\n");
                pthread_mutex_unlock(&global_arena->mutex);
                return NULL;
            }
            global_arena->block_count++;
        }
    }
    
    // Mevcut bloktan bellek ayır
    void* ptr = (char*)global_arena->blocks[global_arena->current_block] + global_arena->current_offset;
    global_arena->current_offset += size;
    
    pthread_mutex_unlock(&global_arena->mutex);
    return ptr;
}

void arena_reset() {
    if (__builtin_expect(!global_arena, 0)) return;
    
    pthread_mutex_lock(&global_arena->mutex);
    global_arena->current_block = 0;
    global_arena->current_offset = 0;
    pthread_mutex_unlock(&global_arena->mutex);
}

void arena_cleanup() {
    if (__builtin_expect(!global_arena, 0)) return;
    
    pthread_mutex_lock(&global_arena->mutex);
    
    for (size_t i = 0; i < global_arena->block_count; i++) {
        if (global_arena->blocks[i]) {
            free(global_arena->blocks[i]);
            global_arena->blocks[i] = NULL;
        }
    }
    
    global_arena->block_count = 0;
    global_arena->current_block = 0;
    global_arena->current_offset = 0;
    
    pthread_mutex_unlock(&global_arena->mutex);
    pthread_mutex_destroy(&global_arena->mutex);
    
    free(global_arena);
    global_arena = NULL;
}
