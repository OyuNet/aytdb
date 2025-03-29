//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#ifndef ENTRY_H
#define ENTRY_H

#include "kv_store.h"
#include "time.h"

typedef struct {
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    time_t expire_at;
    int in_use;
} Entry;

#endif //ENTRY_H
