#include <stdio.h>
#include <string.h>
#include "kv_store.h"
#include "storage.h"

static void execute_set();
static void execute_setex();
static void execute_get();
static void execute_del();

int main() {
    kv_init();

    char command[64];

    printf("Welcome to AytDB!\nCommands: set <key> <value>, setex <key> <value> <ttl>, get <key>, del <key>, compact, exit\n");

    while (1) {
        printf("> ");
        scanf("%63s", command);

        if (strcmp(command, "set") == 0) {
            execute_set();
        } else if (strcmp(command, "setex") == 0) {
            execute_setex();
        } else if (strcmp(command, "get") == 0) {
            execute_get();
        } else if (strcmp(command, "del") == 0) {
            execute_del();
        } else if (strcmp(command, "compact") == 0) {
            storage_compact();
            printf("Compaction process complete.\n");
        } else if (strcmp(command, "exit") == 0) {
            break;
        } else {
            printf("Invalid command, please try again\n");
        }
    }
}

static void execute_set() {
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    scanf("%255s %1023s", key, value);
    kv_set(key, value);
    printf("%s key added successfully.\n", key);
}

static void execute_setex() {
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    int ttl;
    scanf("%255s %1023s %d", key, value, &ttl);
    kv_set_with_ttl(key, value, ttl);
}

static void execute_get() {
    char key[MAX_KEY_SIZE];
    scanf("%255s", key);
    const char* val = kv_get(key);
    printf(val ? "%s\n": "NULL\n", val);
}

static void execute_del() {
    char key[MAX_KEY_SIZE];
    scanf("%255s", key);
    kv_del(key);
    printf("%s key removed successfully.\n", key);
}