//
// Created by Arda Yiğit Tok on 28.03.2025.
//
#ifndef HASH_UTIL_H
#define HASH_UTIL_H

#include "hash_util.h"

unsigned int hash_key(const char* key) {
    unsigned int hash = 5381;
    int c;
    while ((c = *key++)) hash = ((hash << 5) + hash) + c;
    return hash;
}

#endif // HASH_UTIL_H
