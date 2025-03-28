//
// Created by Arda Yiğit Tok on 28.03.2025.
//

#ifndef STORAGE_H
#define STORAGE_H

#define MAX_LINE_SIZE (4 + MAX_KEY_SIZE + MAX_VALUE_SIZE + 4)

void storage_append_set(const char* key, const char* value);
void storage_append_del(const char* key);
void storage_load();

#endif //STORAGE_H
