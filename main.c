#include <stdio.h>
#include <string.h>
#include "kv_store.h"

static void execute_set();
static void execute_get();
static void execute_del();

int main() {
    kv_init();

    char command[16];

    printf("Welcome to AytDB!\nCommands: set <key> <value>, get <key>, del <key>, exit\n");

    while (1) {
        printf("> ");
        scanf("%15s", command);

        if (strcmp(command, "set") == 0) {
            execute_set();
        } else if (strcmp(command, "get") == 0) {
            execute_get();
        } else if (strcmp(command, "del") == 0) {
            execute_del();
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