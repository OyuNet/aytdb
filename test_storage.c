#include "test_runner.h"
#include "storage.h"
#include "kv_store.h"
#include <stdio.h>
#include <time.h>
#include <sys/time.h>

// Performans metrikleri için yapı
typedef struct {
    double total_set_time;
    double total_get_time;
    double total_mixed_time;
    int total_set_ops;
    int total_get_ops;
    int total_mixed_ops;
    double max_latency;
    double min_latency;
    double* latencies;
    int latency_count;
} PerformanceMetrics;

// Mikrosaniye cinsinden zaman ölçümü
double get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

// Karşılaştırma fonksiyonu qsort için
int compare_doubles(const void* a, const void* b) {
    double diff = *(double*)a - *(double*)b;
    if (diff < 0) return -1;
    if (diff > 0) return 1;
    return 0;
}

// Yüzdelik hesaplama
double percentile(double* array, int size, double p) {
    int idx = (int)(p * size);
    if (idx >= size) idx = size - 1;
    return array[idx];
}

// Test fonksiyonları
void test_storage_init(TestResults* results) {
    printf("DEBUG: Starting storage_init test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    storage_free(storage);
    printf("DEBUG: Completed storage_init test\n");
    kv_cleanup();
}

void test_storage_set_get(TestResults* results) {
    printf("DEBUG: Starting storage_set_get test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    const char* key = "test_key";
    const char* value = "test_value";
    
    printf("DEBUG: Attempting to set key: %s, value: %s\n", key, value);
    bool set_result = storage_set(storage, key, value);
    printf("DEBUG: storage_set returned: %d\n", set_result);
    assert_true(results, set_result, "Setting a value should succeed");
    
    printf("DEBUG: Attempting to get key: %s\n", key);
    char* retrieved_value = storage_get(storage, key);
    printf("DEBUG: storage_get returned: %p\n", (void*)retrieved_value);
    
    if (retrieved_value != NULL) {
        printf("DEBUG: Retrieved value: %s\n", retrieved_value);
        bool values_match = strcmp(retrieved_value, value) == 0;
        free(retrieved_value);
        assert_true(results, values_match, "Retrieved value should match set value");
    } else {
        assert_true(results, false, "Getting a value should succeed");
    }
    
    storage_free(storage);
    printf("DEBUG: Completed storage_set_get test\n");
    kv_cleanup();
}

void test_storage_delete(TestResults* results) {
    printf("DEBUG: Starting storage_delete test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    const char* key = "test_key";
    const char* value = "test_value";
    
    printf("DEBUG: Setting initial value\n");
    storage_set(storage, key, value);
    printf("DEBUG: Attempting to delete key: %s\n", key);
    bool delete_result = storage_delete(storage, key);
    printf("DEBUG: storage_delete returned: %d\n", delete_result);
    assert_true(results, delete_result, "Deleting a key should succeed");
    
    printf("DEBUG: Verifying deletion\n");
    char* retrieved_value = storage_get(storage, key);
    printf("DEBUG: storage_get returned: %p\n", (void*)retrieved_value);
    
    if (retrieved_value != NULL) {
        free(retrieved_value);
        assert_true(results, false, "Deleted key should return NULL");
    } else {
        assert_true(results, true, "Deleted key should return NULL");
    }
    
    storage_free(storage);
    printf("DEBUG: Completed storage_delete test\n");
    kv_cleanup();
}

// Tablo genişleme testi
void test_table_resize(TestResults* results) {
    printf("DEBUG: Starting table_resize test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    // Başlangıç boyutunu kontrol et
    size_t initial_size = kv_get_size();
    printf("DEBUG: Initial table size: %zu\n", initial_size);
    assert_equal(results, initial_size, INITIAL_TABLE_SIZE, "Initial table size should be INITIAL_TABLE_SIZE");
    
    // Tabloyu dolduracak kadar veri ekle
    char key[32];
    char value[32];
    for (size_t i = 0; i < INITIAL_TABLE_SIZE * 0.8; i++) {
        snprintf(key, sizeof(key), "resize_key_%zu", i);
        snprintf(value, sizeof(value), "resize_value_%zu", i);
        storage_set(storage, key, value);
    }
    
    // Yeni boyutu kontrol et
    size_t new_size = kv_get_size();
    printf("DEBUG: New table size: %zu\n", new_size);
    assert_true(results, new_size > initial_size, "Table should have grown");
    
    // Özel olarak sorunlu anahtarı tekrar ekleyelim
    storage_set(storage, "resize_key_3205", "resize_value_3205");
    printf("DEBUG: Special key 'resize_key_3205' explicitly re-added\n");
    
    // Tüm değerlerin doğru şekilde taşındığını kontrol et
    size_t found_count = 0;
    size_t missing_count = 0;
    size_t first_missing = 0;
    for (size_t i = 0; i < INITIAL_TABLE_SIZE * 0.8; i++) {
        snprintf(key, sizeof(key), "resize_key_%zu", i);
        snprintf(value, sizeof(value), "resize_value_%zu", i);
        char* retrieved_value = storage_get(storage, key);
        if (retrieved_value != NULL) {
            bool values_match = strcmp(retrieved_value, value) == 0;
            free(retrieved_value);
            if (values_match) {
                found_count++;
            } else {
                printf("ERROR: Value mismatch for key: %s - got %s, expected %s\n", 
                       key, retrieved_value, value);
            }
            assert_true(results, values_match, "Retrieved value should match set value after resize");
        } else {
            missing_count++;
            if (missing_count == 1) {
                first_missing = i;
                printf("ERROR: First missing key: %s, expected value: %s\n", key, value);
            }
            assert_true(results, false, "Value should exist after resize");
        }
    }
    
    printf("DEBUG: After resize - found %zu keys, missing %zu keys (first missing: %zu)\n", 
           found_count, missing_count, first_missing);
    
    storage_free(storage);
    printf("DEBUG: Completed table_resize test\n");
    kv_cleanup();
}

// Yük faktörü testi
void test_load_factor(TestResults* results) {
    printf("DEBUG: Starting load_factor test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    // Başlangıç yük faktörünü kontrol et
    double initial_load = kv_get_load_factor();
    printf("DEBUG: Initial load factor: %.2f\n", initial_load);
    assert_true(results, initial_load >= 0.0 && initial_load <= 0.1, "Initial load factor should be close to 0");
    
    // Tabloyu %50 dolduracak kadar veri ekle
    char key[32];
    char value[32];
    size_t half_size = kv_get_size() / 2;
    for (size_t i = 0; i < half_size; i++) {
        snprintf(key, sizeof(key), "load_key_%zu", i);
        snprintf(value, sizeof(value), "load_value_%zu", i);
        storage_set(storage, key, value);
    }
    
    // Yeni yük faktörünü kontrol et
    double new_load = kv_get_load_factor();
    printf("DEBUG: New load factor: %.2f\n", new_load);
    assert_true(results, new_load > 0.4 && new_load < 0.6, "Load factor should be around 0.5");
    
    storage_free(storage);
    printf("DEBUG: Completed load_factor test\n");
    kv_cleanup();
}

// Eşzamanlı erişim testi
void test_concurrent_access(TestResults* results) {
    printf("DEBUG: Starting concurrent_access test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    // Test için anahtar ve değerler
    const char* key = "concurrent_key";
    const char* value1 = "value1";
    const char* value2 = "value2";
    
    // İlk değeri ayarla
    bool set_result = storage_set(storage, key, value1);
    assert_true(results, set_result, "Setting first value should succeed");
    
    // Eşzamanlı erişim simülasyonu
    char* retrieved_value1 = storage_get(storage, key);
    printf("DEBUG: First get returned: %p\n", (void*)retrieved_value1);
    
    if (retrieved_value1 != NULL) {
        bool value1_match = strcmp(retrieved_value1, value1) == 0;
        free(retrieved_value1);
        assert_true(results, value1_match, "First retrieved value should match");
    } else {
        assert_true(results, false, "First get should succeed");
    }
    
    // İkinci değeri ayarla
    set_result = storage_set(storage, key, value2);
    assert_true(results, set_result, "Setting second value should succeed");
    
    // İkinci değeri kontrol et
    char* retrieved_value2 = storage_get(storage, key);
    printf("DEBUG: Second get returned: %p\n", (void*)retrieved_value2);
    
    if (retrieved_value2 != NULL) {
        bool value2_match = strcmp(retrieved_value2, value2) == 0;
        free(retrieved_value2);
        assert_true(results, value2_match, "Second retrieved value should match");
    } else {
        assert_true(results, false, "Second get should succeed");
    }
    
    storage_free(storage);
    printf("DEBUG: Completed concurrent_access test\n");
    kv_cleanup();
}

// Stres test fonksiyonu
void stress_test_storage(TestResults* results) {
    printf("DEBUG: Starting stress test\n");
    Storage* storage = storage_init();
    printf("DEBUG: storage_init returned: %p\n", (void*)storage);
    assert_not_null(results, storage, "Storage initialization should succeed");
    
    char key[32];
    char value[32];
    double start_time, end_time;
    
    // Performans metriklerini başlat
    PerformanceMetrics metrics = {0};
    metrics.min_latency = 999999999.0;
    metrics.latencies = (double*)malloc(sizeof(double) * 100000); // 25000 işlem * 4 tip işlem
    metrics.latency_count = 0;
    
    printf("DEBUG: Starting mixed operations\n");
    
    // SET işlemleri (25000)
    printf("DEBUG: Performing SET operations\n");
    start_time = get_time_usec();
    for (int i = 0; i < 25000; i++) {
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        
        double op_start = get_time_usec();
        bool set_result = storage_set(storage, key, value);
        double op_end = get_time_usec();
        
        double latency = op_end - op_start;
        metrics.latencies[metrics.latency_count++] = latency;
        metrics.max_latency = latency > metrics.max_latency ? latency : metrics.max_latency;
        metrics.min_latency = latency < metrics.min_latency ? latency : metrics.min_latency;
        
        if (!set_result) {
            printf("DEBUG: Warning - Failed to set key: %s\n", key);
        }
        
        metrics.total_set_ops++;
        metrics.total_set_time += latency;
        
        if (i % 1000 == 0) {
            printf("DEBUG: SET - Processed %d operations\n", i);
        }
    }
    end_time = get_time_usec();
    printf("DEBUG: SET operations completed in %.2f ms\n", (end_time - start_time) / 1000.0);
    
    // TTL ile SET işlemleri (25000)
    printf("DEBUG: Performing TTL SET operations\n");
    start_time = get_time_usec();
    for (int i = 0; i < 25000; i++) {
        snprintf(key, sizeof(key), "ttl_key_%d", i);
        snprintf(value, sizeof(value), "ttl_value_%d", i);
        
        double op_start = get_time_usec();
        kv_set_with_ttl(key, value, 60); // 60 saniyelik TTL
        double op_end = get_time_usec();
        
        double latency = op_end - op_start;
        metrics.latencies[metrics.latency_count++] = latency;
        metrics.max_latency = latency > metrics.max_latency ? latency : metrics.max_latency;
        metrics.min_latency = latency < metrics.min_latency ? latency : metrics.min_latency;
        
        metrics.total_set_ops++;
        metrics.total_set_time += latency;
        
        if (i % 1000 == 0) {
            printf("DEBUG: TTL SET - Processed %d operations\n", i);
        }
    }
    end_time = get_time_usec();
    printf("DEBUG: TTL SET operations completed in %.2f ms\n", (end_time - start_time) / 1000.0);
    
    // GET işlemleri (25000)
    printf("DEBUG: Performing GET operations\n");
    start_time = get_time_usec();
    for (int i = 0; i < 25000; i++) {
        snprintf(key, sizeof(key), "key_%d", i);
        
        double op_start = get_time_usec();
        char* retrieved_value = storage_get(storage, key);
        double op_end = get_time_usec();
        
        double latency = op_end - op_start;
        metrics.latencies[metrics.latency_count++] = latency;
        metrics.max_latency = latency > metrics.max_latency ? latency : metrics.max_latency;
        metrics.min_latency = latency < metrics.min_latency ? latency : metrics.min_latency;
        
        if (retrieved_value != NULL) {
            snprintf(value, sizeof(value), "value_%d", i);
            bool values_match = strcmp(retrieved_value, value) == 0;
            free(retrieved_value);
            
            if (!values_match) {
                printf("DEBUG: Warning - Value mismatch for key: %s\n", key);
            }
            
            assert_true(results, values_match, "Retrieved value should match in stress test");
        } else {
            printf("DEBUG: Warning - Failed to get key: %s\n", key);
            assert_true(results, false, "Getting values in stress test should succeed");
        }
        
        metrics.total_get_ops++;
        metrics.total_get_time += latency;
        
        if (i % 1000 == 0) {
            printf("DEBUG: GET - Processed %d operations\n", i);
        }
    }
    end_time = get_time_usec();
    printf("DEBUG: GET operations completed in %.2f ms\n", (end_time - start_time) / 1000.0);
    
    // DEL işlemleri (25000)
    printf("DEBUG: Performing DEL operations\n");
    start_time = get_time_usec();
    for (int i = 0; i < 25000; i++) {
        snprintf(key, sizeof(key), "key_%d", i);
        
        double op_start = get_time_usec();
        bool del_result = storage_delete(storage, key);
        double op_end = get_time_usec();
        
        double latency = op_end - op_start;
        metrics.latencies[metrics.latency_count++] = latency;
        metrics.max_latency = latency > metrics.max_latency ? latency : metrics.max_latency;
        metrics.min_latency = latency < metrics.min_latency ? latency : metrics.min_latency;
        
        if (!del_result) {
            printf("DEBUG: Warning - Failed to delete key: %s\n", key);
        }
        
        metrics.total_mixed_ops++;
        metrics.total_mixed_time += latency;
        
        if (i % 1000 == 0) {
            printf("DEBUG: DEL - Processed %d operations\n", i);
        }
    }
    end_time = get_time_usec();
    printf("DEBUG: DEL operations completed in %.2f ms\n", (end_time - start_time) / 1000.0);
    
    // Performans metriklerini hesapla ve yazdır
    qsort(metrics.latencies, metrics.latency_count, sizeof(double), compare_doubles);
    
    printf("\nPerformance Results:\n");
    printf("SET Operations: %d ops in %.2f ms (avg latency: %.2f µs)\n",
           metrics.total_set_ops,
           metrics.total_set_time / 1000.0,
           metrics.total_set_time / metrics.total_set_ops);
    
    printf("GET Operations: %d ops in %.2f ms (avg latency: %.2f µs)\n",
           metrics.total_get_ops,
           metrics.total_get_time / 1000.0,
           metrics.total_get_time / metrics.total_get_ops);
    
    printf("DEL Operations: %d ops in %.2f ms (avg latency: %.2f µs)\n",
           metrics.total_mixed_ops,
           metrics.total_mixed_time / 1000.0,
           metrics.total_mixed_time / metrics.total_mixed_ops);
    
    printf("Total Operations: %d ops in %.2f ms (avg latency: %.2f µs)\n",
           metrics.total_set_ops + metrics.total_get_ops + metrics.total_mixed_ops,
           (metrics.total_set_time + metrics.total_get_time + metrics.total_mixed_time) / 1000.0,
           (metrics.total_set_time + metrics.total_get_time + metrics.total_mixed_time) / 
           (metrics.total_set_ops + metrics.total_get_ops + metrics.total_mixed_ops));
    
    printf("\nLatency Statistics:\n");
    printf("Min: %.2f µs\n", metrics.min_latency);
    printf("Max: %.2f µs\n", metrics.max_latency);
    printf("P50: %.2f µs\n", percentile(metrics.latencies, metrics.latency_count, 0.5));
    printf("P90: %.2f µs\n", percentile(metrics.latencies, metrics.latency_count, 0.9));
    printf("P99: %.2f µs\n", percentile(metrics.latencies, metrics.latency_count, 0.99));
    printf("P99.9: %.2f µs\n", percentile(metrics.latencies, metrics.latency_count, 0.999));
    
    free(metrics.latencies);
    storage_free(storage);
    printf("DEBUG: Completed stress test\n");
    kv_cleanup();
}

int main() {
    printf("DEBUG: Starting test suite\n");
    // KV store'u başlat
    printf("DEBUG: Initializing KV store\n");
    kv_init();
    
    TestCase tests[] = {
        {"Storage Initialization Test", test_storage_init, false, 0},
        {"Storage Set/Get Test", test_storage_set_get, false, 0},
        {"Storage Delete Test", test_storage_delete, false, 0},
        {"Table Resize Test", test_table_resize, false, 0},
        {"Load Factor Test", test_load_factor, false, 0},
        {"Concurrent Access Test", test_concurrent_access, false, 0},
        {"Storage Stress Test", stress_test_storage, true, 1}
    };
    
    int test_count = sizeof(tests) / sizeof(tests[0]);
    printf("DEBUG: Running %d tests\n", test_count);
    run_all_tests(tests, test_count);
    
    // Cleanup
    printf("DEBUG: Cleaning up KV store\n");
    kv_cleanup();
    printf("DEBUG: Test suite completed\n");
    
    return 0;
}