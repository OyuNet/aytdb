#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kv_store.h"
#include "storage.h"

#define MAX_LINE_SIZE 2048
#define MAX_TOKENS 10

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
    kv_init();
    char line[MAX_LINE_SIZE];
    char* tokens[MAX_TOKENS];
    int token_count;

    printf("Welcome to AytDB!\nCommands: set <key> <value>, setex <key> <value> <ttl>, get <key>, del <key>, compact, exit\n");

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
                kv_set(tokens[1], tokens[2]);
                printf("%s key added successfully.\n", tokens[1]);
            } else {
                printf("Error: set command requires key and value\n");
            }
        } else if (strcmp(tokens[0], "setex") == 0) {
            if (token_count >= 4) {
                int ttl = atoi(tokens[3]);
                kv_set_with_ttl(tokens[1], tokens[2], ttl);
                printf("%s key added successfully.\n", tokens[1]);
            } else {
                printf("Error: setex command requires key, value, and ttl\n");
            }
        } else if (strcmp(tokens[0], "get") == 0) {
            if (token_count >= 2) {
                const char* val = kv_get(tokens[1]);
                printf(val ? "%s\n" : "NULL\n", val);
            } else {
                printf("Error: get command requires key\n");
            }
        } else if (strcmp(tokens[0], "del") == 0) {
            if (token_count >= 2) {
                kv_del(tokens[1]);
                printf("%s key removed successfully.\n", tokens[1]);
            } else {
                printf("Error: del command requires key\n");
            }
        } else if (strcmp(tokens[0], "compact") == 0) {
            storage_compact();
            printf("Compaction process complete.\n");
        } else if (strcmp(tokens[0], "exit") == 0) {
            kv_cleanup();
            break;
        } else {
            printf("Invalid command, please try again\n");
        }

        for (int i = 0; i < token_count; i++) {
            free(tokens[i]);
        }
    }
    return 0;
}