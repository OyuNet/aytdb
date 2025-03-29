#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kv_store.h"
#include "storage.h"

#define MAX_TOKENS 10

// Hata ayıklama için
extern bool logging_enabled;

static char* parse_quoted_string(char* str, char* result) {
    if (*str != '"') return NULL;
    str++;
    
    while (*str && *str != '"') {
        *result++ = *str++;
    }
    if (*str == '"') str++;
    *result = '\0';
    
    return str;
}

static int parse_command(char* line, char* tokens[], int max_tokens) {
    char* current = line;
    int count = 0;
    char temp[MAX_VALUE_SIZE];
    
    while (*current && count < max_tokens) {
        while (*current == ' ' || *current == '\t') current++;
        if (!*current) break;
        
        if (*current == '"') {
            char* end = parse_quoted_string(current, temp);
            if (end) {
                tokens[count++] = strdup(temp);
                current = end;
                continue;
            }
        }
        
        char* start = current;
        while (*current && *current != ' ' && *current != '\t') current++;
        if (current > start) {
            int len = current - start;
            if (len >= MAX_VALUE_SIZE) len = MAX_VALUE_SIZE - 1;
            strncpy(temp, start, len);
            temp[len] = '\0';
            tokens[count++] = strdup(temp);
        }
    }
    
    return count;
}

int main() {
    // Hata ayıklama loglarını etkinleştir
    logging_enabled = true;
    
    Storage* storage = storage_init();
    if (!storage) {
        printf("Error: Failed to initialize storage\n");
        return 1;
    }
    printf("Storage initialized. Ready to process commands.\n");

    char line[MAX_LINE_SIZE];
    char* tokens[MAX_TOKENS];
    int token_count;

    printf("Welcome to AytDB!\n");
    printf("Commands:\n");
    printf("  set <key> <value>       : Store a key-value pair\n");
    printf("  setex <key> <value> <ttl>: Store a key-value pair with expiration time in seconds\n");
    printf("  get <key>               : Retrieve a value by key\n");
    printf("  del <key>               : Delete a key-value pair\n");
    printf("  save                    : Save a snapshot immediately\n");
    printf("  interval <seconds>      : Set automatic snapshot interval (default: 300 seconds)\n");
    printf("  compact                 : Remove expired keys and save snapshot\n");
    printf("  exit                    : Exit the program\n");

    while (1) {
        printf("> ");
        if (!fgets(line, sizeof(line), stdin)) {
            break;
        }

        line[strcspn(line, "\n")] = 0;
        
        if (strlen(line) == 0) continue;

        token_count = parse_command(line, tokens, MAX_TOKENS);
        
        if (token_count == 0) continue;

        if (strcmp(tokens[0], "set") == 0) {
            if (token_count >= 3) {
                if (storage_set(storage, tokens[1], tokens[2])) {
                    printf("%s key added successfully.\n", tokens[1]);
                } else {
                    printf("Error: Failed to set key %s\n", tokens[1]);
                }
            } else {
                printf("Error: set command requires key and value\n");
            }
        } else if (strcmp(tokens[0], "setex") == 0) {
            if (token_count >= 4) {
                int ttl = atoi(tokens[3]);
                if (storage_set_with_ttl(storage, tokens[1], tokens[2], ttl)) {
                    printf("%s key added successfully with TTL %d seconds.\n", tokens[1], ttl);
                } else {
                    printf("Error: Failed to set key %s with TTL\n", tokens[1]);
                }
            } else {
                printf("Error: setex command requires key, value, and ttl\n");
            }
        } else if (strcmp(tokens[0], "get") == 0) {
            if (token_count >= 2) {
                char* val = storage_get(storage, tokens[1]);
                if (val) {
                    printf("%s\n", val);
                    free(val);
                } else {
                    printf("NULL\n");
                }
            } else {
                printf("Error: get command requires key\n");
            }
        } else if (strcmp(tokens[0], "del") == 0) {
            if (token_count >= 2) {
                if (storage_delete(storage, tokens[1])) {
                    printf("%s key removed successfully.\n", tokens[1]);
                } else {
                    printf("Error: Failed to delete key %s\n", tokens[1]);
                }
            } else {
                printf("Error: del command requires key\n");
            }
        } else if (strcmp(tokens[0], "compact") == 0) {
            storage_compact();
            printf("Compaction process complete.\n");
        } else if (strcmp(tokens[0], "save") == 0) {
            if (storage_save_snapshot()) {
                printf("Snapshot saved successfully.\n");
            } else {
                printf("Error: Failed to save snapshot\n");
            }
        } else if (strcmp(tokens[0], "interval") == 0) {
            if (token_count >= 2) {
                int interval = atoi(tokens[1]);
                if (interval > 0) {
                    storage_schedule_snapshot(interval);
                    printf("Snapshot interval set to %d seconds.\n", interval);
                } else {
                    printf("Error: Invalid interval value\n");
                }
            } else {
                printf("Error: interval command requires seconds value\n");
            }
        } else if (strcmp(tokens[0], "exit") == 0) {
            storage_free(storage);
            break;
        } else {
            printf("Invalid command, please try again\n");
        }

        for (int i = 0; i < token_count; i++) {
            free(tokens[i]);
        }
    }
    
    arena_cleanup();
    
    return 0;
}