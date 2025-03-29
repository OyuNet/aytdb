#include <string.h>
#include <stdint.h>
#include "hash_util.h"

// FNV-1a hash algoritması implementasyonu - daha hızlı
size_t hash(const char* key) {
    const uint64_t FNV_PRIME = 1099511628211ULL;
    const uint64_t FNV_OFFSET = 14695981039346656037ULL;
    
    uint64_t hash = FNV_OFFSET;
    const unsigned char* data = (const unsigned char*)key;
    
    while (*data) {
        hash ^= (uint64_t)*data++;
        hash *= FNV_PRIME;
    }
    
    return (size_t)hash;
}