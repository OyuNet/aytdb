//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#ifndef ENTRY_H
#define ENTRY_H

#include <time.h>
#include <stdint.h>

typedef struct __attribute__((aligned(64))) {
    char key[256];
    char value[1024];
    time_t expire_at;
    uint32_t hash;  // Hash değerini saklayarak tekrar hesaplamaya gerek kalmaz
    uint8_t flags;  // İleride kullanılabilecek flag'ler
    uint8_t in_use; // Entry'nin kullanımda olup olmadığı
    uint8_t reserved[2]; // Memory alignment için padding
    
    // Memory pool işlemleri için gereken alanlar
    struct Entry* next; // Bağlı liste için sonraki entry
} Entry;

#endif //ENTRY_H
