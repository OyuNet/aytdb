#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "server.h"

// Show usage for command line parameters
void show_usage(const char* program_name) {
    printf("Usage: %s [port]\n", program_name);
    printf("  port: The port number on which the server will listen (default: 6379)\n");
}

int main(int argc, char* argv[]) {
    int port = 6379; // Default Redis port
    
    // Process command line parameters
    if (argc > 1) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            show_usage(argv[0]);
            return 0;
        }
        
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Error: Invalid port number. Port must be between 1-65535.\n");
            return 1;
        }
    }
    
    printf("Starting AytDB telnet server...\n");
    
    // Start the server on the specified port
    return server_init(port);
} 