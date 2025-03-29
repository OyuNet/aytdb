#include "storage.h"
#include "kv_store.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>

#define STORAGE_FILE "storage.db"
#define SNAPSHOT_FILE "snapshot.db"
#define TEMP_SNAPSHOT_FILE "snapshot.db.tmp"
#define BUFFER_SIZE 32768  // Buffer boyutunu 32KB'a çıkarıyorum
#define SNAPSHOT_INTERVAL 300 // 5 dakikalık default snapshot aralığı

static FILE* storage_file = NULL;
static char storage_path[256];
static char write_buffer[BUFFER_SIZE];
static size_t buffer_pos = 0;
static pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t snapshot_thread;
static int snapshot_interval = SNAPSHOT_INTERVAL;
static bool snapshot_thread_running = false;
static bool shutdown_requested = false;

// Snapshot thread fonksiyonu - belirli aralıklarla snapshot oluşturur
static void* snapshot_thread_func(void* arg) {
    if (logging_enabled) printf("DEBUG: Snapshot thread started with interval %d seconds\n", snapshot_interval);
    
    while (!shutdown_requested) {
        sleep(snapshot_interval);
        if (shutdown_requested) break;
        
        if (logging_enabled) printf("DEBUG: Automatic snapshot triggered\n");
        storage_save_snapshot();
    }
    
    if (logging_enabled) printf("DEBUG: Snapshot thread exiting\n");
    return NULL;
}

// Buffer yönetimi - artık AOF için değil sadece snapshot yazma için kullanılacak
static void flush_buffer() {
    pthread_mutex_lock(&buffer_mutex);
    
    if (buffer_pos > 0 && storage_file) {
        if (logging_enabled) printf("DEBUG: Flushing buffer with %zu bytes\n", buffer_pos);
        size_t written = fwrite(write_buffer, 1, buffer_pos, storage_file);
        if (written != buffer_pos) {
            if (logging_enabled) printf("DEBUG: Warning - Failed to write all data to storage file\n");
        }
        fflush(storage_file);
        buffer_pos = 0;
        if (logging_enabled) printf("DEBUG: Buffer flushed successfully\n");
    }
    
    pthread_mutex_unlock(&buffer_mutex);
}

static void append_to_buffer(const char* str) {
    if (!str || !storage_file) return;
    
    pthread_mutex_lock(&buffer_mutex);
    
    size_t len = strlen(str);
    if (buffer_pos + len >= BUFFER_SIZE) {
        // Buffer doldu, hemen flush edelim
        size_t written = fwrite(write_buffer, 1, buffer_pos, storage_file);
        if (written != buffer_pos && logging_enabled) {
            printf("DEBUG: Warning - Partial write to storage file\n");
        }
        fflush(storage_file);
        buffer_pos = 0;
    }
    
    if (len < BUFFER_SIZE) {
        memcpy(write_buffer + buffer_pos, str, len);
        buffer_pos += len;
    } else {
        // Çok büyük string, doğrudan dosyaya yazalım
        if (logging_enabled) printf("DEBUG: String too large for buffer, writing directly\n");
        fwrite(str, 1, len, storage_file);
        fflush(storage_file);
    }
    
    // Eğer buffer 3/4 dolduysa, flush yapalım
    if (buffer_pos > (BUFFER_SIZE * 3) / 4) {
        size_t written = fwrite(write_buffer, 1, buffer_pos, storage_file);
        if (written != buffer_pos && logging_enabled) {
            printf("DEBUG: Warning - Partial write to storage file\n");
        }
        fflush(storage_file);
        buffer_pos = 0;
    }
    
    pthread_mutex_unlock(&buffer_mutex);
}

// Storage yönetimi
Storage* storage_init(void) {
    if (logging_enabled) printf("DEBUG: Initializing storage\n");
    
    Storage* storage = (Storage*)malloc(sizeof(Storage));
    if (!storage) {
        if (logging_enabled) printf("DEBUG: Failed to allocate storage structure\n");
        return NULL;
    }
    
    storage->file_path = strdup(SNAPSHOT_FILE);
    if (!storage->file_path) {
        if (logging_enabled) printf("DEBUG: Failed to duplicate file path\n");
        free(storage);
        return NULL;
    }
    
    // Önce KV store'u başlat
    kv_init();
    
    // Snapshot dosyasını okuma için aç
    storage->file = fopen(storage->file_path, "r");
    if (!storage->file) {
        if (logging_enabled) printf("DEBUG: No snapshot file found, starting fresh\n");
    } else {
        // Snapshot dosyasını oku
        if (logging_enabled) printf("DEBUG: Loading data from snapshot file\n");
        storage_load_snapshot();
        fclose(storage->file);
        storage->file = NULL;
    }
    
    // Snapshot dosyasını yazma için aç
    storage->file = fopen(storage->file_path, "w");
    if (!storage->file) {
        if (logging_enabled) printf("ERROR: Failed to open snapshot file for writing\n");
        free(storage->file_path);
        free(storage);
        return NULL;
    }
    
    // Global storage_file'ı güncelle
    storage_file = storage->file;
    
    // Buffer'ı sıfırla
    pthread_mutex_lock(&buffer_mutex);
    buffer_pos = 0;
    memset(write_buffer, 0, BUFFER_SIZE);
    pthread_mutex_unlock(&buffer_mutex);
    
    // Snapshot thread'i başlat
    storage_schedule_snapshot(SNAPSHOT_INTERVAL);
    
    if (logging_enabled) printf("DEBUG: Storage initialization completed\n");
    return storage;
}

void storage_free(Storage* storage) {
    if (!storage) return;

    if (logging_enabled) printf("DEBUG: Starting storage cleanup\n");
    
    printf("Saving snapshot to disk...\n");
    
    // Snapshot thread'i durdur
    shutdown_requested = true;
    if (snapshot_thread_running) {
        if (logging_enabled) printf("DEBUG: Waiting for snapshot thread to exit\n");
        pthread_join(snapshot_thread, NULL);
        snapshot_thread_running = false;
    }

    // Son bir snapshot al
    storage_save_snapshot();

    // Dosyayı kapat
    if (storage->file) {
        if (logging_enabled) printf("DEBUG: Closing storage file\n");
        if (fclose(storage->file) != 0) {
            if (logging_enabled) printf("DEBUG: Warning - Failed to close storage file\n");
        }
        storage->file = NULL;
    }

    // Dosya yolunu temizle
    if (storage->file_path) {
        if (logging_enabled) printf("DEBUG: Freeing file path\n");
        free(storage->file_path);
        storage->file_path = NULL;
    }

    // Storage yapısını temizle
    if (logging_enabled) printf("DEBUG: Freeing storage structure\n");
    free(storage);

    if (logging_enabled) printf("DEBUG: Storage cleanup completed\n");
}

// Temel operasyonlar
bool storage_set(Storage* storage, const char* key, const char* value) {
    if (!storage || !key || !value) return false;
    
    // Key-value çiftini hafızaya kaydet
    kv_set(key, value);
    
    return true;
}

bool storage_set_with_ttl(Storage* storage, const char* key, const char* value, int ttl) {
    if (!storage || !key || !value) return false;
    
    // Key-value çiftini hafızaya kaydet
    kv_set_with_ttl(key, value, ttl);
    
    return true;
}

char* storage_get(Storage* storage, const char* key) {
    if (!storage || !key) return NULL;
    
    // Key'i tabloda ara
    const char* value = kv_get(key);
    if (!value) return NULL;
    
    // Değeri güvenli bir şekilde kopyala
    size_t len = strlen(value);
    char* result = malloc(len + 1);
    if (!result) return NULL;
    
    memcpy(result, value, len);
    result[len] = '\0';
    return result;
}

bool storage_delete(Storage* storage, const char* key) {
    if (!storage || !key) return false;
    
    // Key'i hafızadan sil
    kv_del(key);
    
    return true;
}

// AOF fonksiyonları yerine boş fonksiyonlar (geriye dönük uyumluluk için)
void storage_append_set(const char* key, const char* value, const int ttl) {
    // Artık AOF kullanmıyoruz, snapshot'a geçtik
    return;
}

void storage_append_del(const char* key) {
    // Artık AOF kullanmıyoruz, snapshot'a geçtik
    return;
}

void storage_load() {
    // Artık AOF kullanmıyoruz, snapshot'a geçtik
    return;
}

// Snapshot işlemleri
bool storage_save_snapshot() {
    if (logging_enabled) printf("DEBUG: Saving snapshot\n");
    
    // Temporary dosya oluştur
    FILE* f = fopen(TEMP_SNAPSHOT_FILE, "w");
    if (!f) {
        if (logging_enabled) printf("DEBUG: Failed to create temporary file for snapshot\n");
        return false;
    }
    
    // Daha verimli yazma için tampon boyutunu ayarla
    setvbuf(f, NULL, _IOFBF, BUFFER_SIZE);

    HashTable* table = kv_get_table();
    if (!table) {
        fclose(f);
        if (logging_enabled) printf("DEBUG: Failed to get hash table for snapshot\n");
        return false;
    }

    time_t now = time(NULL);
    size_t total_entries = 0;
    size_t live_entries = 0;

    // Verileri kilitle
    pthread_mutex_lock(&table->mutex);
    
    // Başlık bilgisi yaz (format: AYTDB_SNAPSHOT_V1)
    fprintf(f, "AYTDB_SNAPSHOT_V1\n");
    fprintf(f, "TIME:%ld\n", now);
    
    // Toplam girdi sayısını hesapla
    for (int i = 0; i < table->size; i++) {
        if (table->entries[i] != NULL) {
            total_entries++;
            // Sadece yaşayan girişleri say
            if (table->entries[i]->expire_at == 0 || table->entries[i]->expire_at > now) {
                live_entries++;
            }
        }
    }
    
    // Toplam giriş sayısını yaz
    fprintf(f, "ENTRIES:%zu\n", live_entries);
    fprintf(f, "---\n"); // Başlık sonu ayracı
    
    // Girişleri yaz - metin formatında, daha okunaklı
    for (int i = 0; i < table->size; i++) {
        if (table->entries[i] != NULL) {
            // Sadece yaşayan girişleri yaz
            if (table->entries[i]->expire_at == 0 || table->entries[i]->expire_at > now) {
                time_t ttl = table->entries[i]->expire_at == 0 ? 0 : table->entries[i]->expire_at - now;
                
                // Anahtar değer çiftini ve TTL'i yaz
                fprintf(f, "KEY:%s\n", table->entries[i]->key);
                fprintf(f, "VALUE:%s\n", table->entries[i]->value);
                fprintf(f, "TTL:%ld\n", ttl);
                fprintf(f, "---\n"); // Ayraç
            }
        }
    }
    
    pthread_mutex_unlock(&table->mutex);
    
    fclose(f);
    
    // Dosya değişimi
    if (remove(SNAPSHOT_FILE) != 0 && errno != ENOENT) {
        if (logging_enabled) printf("DEBUG: Failed to remove old snapshot file: %s\n", strerror(errno));
    }
    
    if (rename(TEMP_SNAPSHOT_FILE, SNAPSHOT_FILE) != 0) {
        if (logging_enabled) printf("DEBUG: Failed to rename temporary file: %s\n", strerror(errno));
        return false;
    }
    
    if (logging_enabled) printf("DEBUG: Snapshot saved. Total entries: %zu, Live entries: %zu\n", 
           total_entries, live_entries);
    
    return true;
}

bool storage_load_snapshot() {
    if (logging_enabled) printf("DEBUG: Loading snapshot\n");
    
    FILE* f = fopen(SNAPSHOT_FILE, "r");
    if (!f) {
        if (logging_enabled) printf("DEBUG: No snapshot file found\n");
        return false;
    }
    
    // Snapshot başlığını oku ve doğrula
    char header[50];
    if (!fgets(header, sizeof(header), f)) {
        if (logging_enabled) printf("DEBUG: Failed to read snapshot header\n");
        fclose(f);
        return false;
    }
    
    // Başlık satırındaki yeni satır karakterini kaldır
    header[strcspn(header, "\n")] = 0;
    
    if (strcmp(header, "AYTDB_SNAPSHOT_V1") != 0) {
        if (logging_enabled) printf("DEBUG: Invalid snapshot header: %s\n", header);
        fclose(f);
        return false;
    }
    
    // Zaman bilgisini oku
    char time_str[50];
    if (!fgets(time_str, sizeof(time_str), f)) {
        if (logging_enabled) printf("DEBUG: Failed to read snapshot time\n");
        fclose(f);
        return false;
    }
    
    time_str[strcspn(time_str, "\n")] = 0;
    
    time_t snapshot_time = 0;
    if (sscanf(time_str, "TIME:%ld", &snapshot_time) != 1) {
        if (logging_enabled) printf("DEBUG: Invalid time format: %s\n", time_str);
        fclose(f);
        return false;
    }
    
    // Girdi sayısını oku
    char entries_str[50];
    if (!fgets(entries_str, sizeof(entries_str), f)) {
        if (logging_enabled) printf("DEBUG: Failed to read entry count\n");
        fclose(f);
        return false;
    }
    
    entries_str[strcspn(entries_str, "\n")] = 0;
    
    size_t entry_count = 0;
    if (sscanf(entries_str, "ENTRIES:%zu", &entry_count) != 1) {
        if (logging_enabled) printf("DEBUG: Invalid entries format: %s\n", entries_str);
        fclose(f);
        return false;
    }
    
    // Ayraç satırını oku
    char separator[50];
    if (!fgets(separator, sizeof(separator), f)) {
        if (logging_enabled) printf("DEBUG: Failed to read separator\n");
        fclose(f);
        return false;
    }
    
    separator[strcspn(separator, "\n")] = 0;
    
    if (strcmp(separator, "---") != 0) {
        if (logging_enabled) printf("DEBUG: Invalid separator: %s\n", separator);
        fclose(f);
        return false;
    }
    
    // Girişleri oku
    time_t now = time(NULL);
    size_t entries_loaded = 0;
    char line[MAX_LINE_SIZE];
    char key[MAX_KEY_SIZE] = "";
    char value[MAX_VALUE_SIZE] = "";
    time_t ttl = 0;
    bool has_key = false;
    bool has_value = false;
    bool has_ttl = false;
    
    if (logging_enabled) printf("DEBUG: Loading %zu entries from snapshot created at %s", 
                               entry_count, ctime(&snapshot_time));
    
    // Satır satır dosyayı oku ve key-value çiftlerini işle
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = 0; // Yeni satır karakterini kaldır
        
        if (strcmp(line, "---") == 0) {
            // Bir kayıt bitti, tamamlanmışsa kaydet
            if (has_key && has_value && has_ttl) {
                // TTL'i kontrol et ve girişi ekle
                if (ttl == 0 || now + ttl > now) { // overflow kontrolü
                    if (logging_enabled && entries_loaded < 5) {
                        printf("DEBUG: Loading key '%s' with value '%s' and TTL %ld\n", key, value, ttl);
                    }
                    kv_set_with_ttl(key, value, ttl);
                    entries_loaded++;
                } else if (logging_enabled && entries_loaded < 5) {
                    printf("DEBUG: Skipping expired key '%s' with TTL %ld\n", key, ttl);
                }
            }
            
            // Yeni kayıt için bayrakları sıfırla
            has_key = false;
            has_value = false;
            has_ttl = false;
            continue;
        }
        
        // Anahtar satırı
        if (strncmp(line, "KEY:", 4) == 0) {
            strncpy(key, line + 4, MAX_KEY_SIZE - 1);
            key[MAX_KEY_SIZE - 1] = '\0';
            has_key = true;
            continue;
        }
        
        // Değer satırı
        if (strncmp(line, "VALUE:", 6) == 0) {
            strncpy(value, line + 6, MAX_VALUE_SIZE - 1);
            value[MAX_VALUE_SIZE - 1] = '\0';
            has_value = true;
            continue;
        }
        
        // TTL satırı
        if (strncmp(line, "TTL:", 4) == 0) {
            ttl = atol(line + 4);
            has_ttl = true;
            continue;
        }
    }
    
    // Son kayıt için kontrol
    if (has_key && has_value && has_ttl) {
        if (ttl == 0 || now + ttl > now) { // overflow kontrolü
            if (logging_enabled && entries_loaded < 5) {
                printf("DEBUG: Loading key '%s' with value '%s' and TTL %ld\n", key, value, ttl);
            }
            kv_set_with_ttl(key, value, ttl);
            entries_loaded++;
        }
    }
    
    fclose(f);
    
    if (logging_enabled) printf("DEBUG: Snapshot load completed, loaded %zu/%zu entries\n", 
                               entries_loaded, entry_count);
    
    return entries_loaded > 0;
}

void storage_schedule_snapshot(int interval_seconds) {
    // Eğer zaten çalışan bir thread varsa, onu durdur
    if (snapshot_thread_running) {
        shutdown_requested = true;
        pthread_join(snapshot_thread, NULL);
        snapshot_thread_running = false;
        shutdown_requested = false;
    }
    
    // Yeni aralık değerini ayarla
    snapshot_interval = interval_seconds > 0 ? interval_seconds : SNAPSHOT_INTERVAL;
    
    // Yeni snapshot thread'i başlat
    if (pthread_create(&snapshot_thread, NULL, snapshot_thread_func, NULL) == 0) {
        snapshot_thread_running = true;
        if (logging_enabled) printf("DEBUG: Snapshot thread scheduled with interval %d seconds\n", snapshot_interval);
    } else {
        if (logging_enabled) printf("ERROR: Failed to create snapshot thread\n");
    }
}

void storage_compact() {
    // Artık compact yapmak yerine snapshot alıyoruz
    if (logging_enabled) printf("DEBUG: Compaction requested, taking snapshot instead\n");
    storage_save_snapshot();
    printf("Snapshot saved successfully.\n");
}

long storage_file_size() {
    struct stat st;
    if (stat(SNAPSHOT_FILE, &st) == 0) {
        return st.st_size;
    }
    return 0;
}
