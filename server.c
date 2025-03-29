#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include "storage.h"
#include "kv_store.h"

#define SERVER_PORT 6379 // Redis default port
#define MAX_CLIENTS 64
#define BUFFER_SIZE MAX_LINE_SIZE
#define MAX_TOKENS 10
#define DEFAULT_PASSWORD "password" // Varsayılan şifre

static int server_socket = -1;
static int client_sockets[MAX_CLIENTS];
static int client_auth_status[MAX_CLIENTS]; // Kimlik doğrulama durumlarını saklar
static Storage* storage = NULL;
static volatile int running = 1;
static char server_password[128] = DEFAULT_PASSWORD; // Sunucu şifresi

// Bu fonksiyon main.c'deki ile aynı
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

// Bu fonksiyon main.c'deki ile aynı
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

// Sinyali yakala ve sunucuyu durdur
void signal_handler(int signum) {
    printf("\nSignal %d received, shutting down...\n", signum);
    running = 0;
    
    // Server socket'i kapat
    if (server_socket != -1) {
        close(server_socket);
        server_socket = -1;
    }
}

// İstemci soketi kapat
void close_client_socket(int client_socket_index) {
    close(client_sockets[client_socket_index]);
    client_sockets[client_socket_index] = -1;
}

// Komutu işle ve yanıtı oluştur
char* process_command(char* command, int client_socket) {
    char* result = malloc(BUFFER_SIZE);
    if (!result) return NULL;
    result[0] = '\0';

    char* tokens[MAX_TOKENS];
    int token_count = 0;
    char line[BUFFER_SIZE];
    
    strncpy(line, command, BUFFER_SIZE);
    line[BUFFER_SIZE - 1] = '\0';
    
    token_count = parse_command(line, tokens, MAX_TOKENS);
    
    if (token_count == 0) {
        strcpy(result, "ERROR: Command not found\r\n");
        return result;
    }
    
    // AUTH komutu her zaman çalışabilir, kimlik doğrulama kontrolü yapılmaz
    if (strcmp(tokens[0], "auth") == 0) {
        if (token_count >= 2) {
            if (strcmp(tokens[1], server_password) == 0) {
                client_auth_status[client_socket] = 1; // Kimlik doğrulama başarılı
                strcpy(result, "OK: Authentication successful\r\n");
            } else {
                strcpy(result, "ERROR: Invalid password\r\n");
            }
        } else {
            strcpy(result, "ERROR: auth command requires password\r\n");
        }
    }
    // PING ve HELP komutları da kimlik doğrulama kontrolü olmadan çalışabilir
    else if (strcmp(tokens[0], "ping") == 0) {
        strcpy(result, "PONG\r\n");
    } else if (strcmp(tokens[0], "help") == 0) {
        strcpy(result, "Available commands:\r\n");
        strcat(result, "  auth <password>         : Authenticate with server\r\n");
        strcat(result, "  set <key> <value>       : Store a key-value pair\r\n");
        strcat(result, "  setex <key> <value> <ttl>: Store a key-value pair with expiration time in seconds\r\n");
        strcat(result, "  get <key>               : Retrieve a value by key\r\n");
        strcat(result, "  del <key>               : Delete a key-value pair\r\n");
        strcat(result, "  save                    : Save a snapshot immediately\r\n");
        strcat(result, "  interval <seconds>      : Set automatic snapshot interval (default: 300 seconds)\r\n");
        strcat(result, "  compact                 : Remove expired keys and save snapshot\r\n");
        strcat(result, "  config password <value> : Change server password\r\n");
        strcat(result, "  ping                    : Test connection\r\n");
        strcat(result, "  quit                    : Close connection\r\n");
        strcat(result, "  shutdown                : Shutdown server\r\n");
        strcat(result, "  help                    : Show this help message\r\n");
    } 
    // Diğer komutlar için kimlik doğrulama kontrolü yap
    else if (client_auth_status[client_socket] != 1) {
        strcpy(result, "ERROR: Authentication required. Use 'auth <password>' command\r\n");
    }
    // Kimlik doğrulaması yapılmışsa diğer komutları işle
    else if (strcmp(tokens[0], "set") == 0) {
        if (token_count >= 3) {
            if (storage_set(storage, tokens[1], tokens[2])) {
                strcpy(result, "OK\r\n");
            } else {
                sprintf(result, "ERROR: Failed to set key %s\r\n", tokens[1]);
            }
        } else {
            strcpy(result, "ERROR: set command requires key and value\r\n");
        }
    } else if (strcmp(tokens[0], "setex") == 0) {
        if (token_count >= 4) {
            int ttl = atoi(tokens[3]);
            if (storage_set_with_ttl(storage, tokens[1], tokens[2], ttl)) {
                sprintf(result, "OK\r\n");
            } else {
                sprintf(result, "ERROR: Failed to set key %s with TTL\r\n", tokens[1]);
            }
        } else {
            strcpy(result, "ERROR: setex command requires key, value, and ttl\r\n");
        }
    } else if (strcmp(tokens[0], "get") == 0) {
        if (token_count >= 2) {
            char* val = storage_get(storage, tokens[1]);
            if (val) {
                sprintf(result, "%s\r\n", val);
                free(val);
            } else {
                strcpy(result, "NULL\r\n");
            }
        } else {
            strcpy(result, "ERROR: get command requires key\r\n");
        }
    } else if (strcmp(tokens[0], "del") == 0) {
        if (token_count >= 2) {
            if (storage_delete(storage, tokens[1])) {
                strcpy(result, "OK\r\n");
            } else {
                sprintf(result, "ERROR: Failed to delete key %s\r\n", tokens[1]);
            }
        } else {
            strcpy(result, "ERROR: del command requires key\r\n");
        }
    } else if (strcmp(tokens[0], "compact") == 0) {
        storage_compact();
        strcpy(result, "OK: Compaction process complete\r\n");
    } else if (strcmp(tokens[0], "save") == 0) {
        if (storage_save_snapshot()) {
            strcpy(result, "OK: Snapshot saved successfully\r\n");
        } else {
            strcpy(result, "ERROR: Failed to save snapshot\r\n");
        }
    } else if (strcmp(tokens[0], "interval") == 0) {
        if (token_count >= 2) {
            int interval = atoi(tokens[1]);
            if (interval > 0) {
                storage_schedule_snapshot(interval);
                sprintf(result, "OK: Snapshot interval set to %d seconds\r\n", interval);
            } else {
                strcpy(result, "ERROR: Invalid interval value\r\n");
            }
        } else {
            strcpy(result, "ERROR: interval command requires seconds value\r\n");
        }
    } else if (strcmp(tokens[0], "exit") == 0 || strcmp(tokens[0], "quit") == 0) {
        strcpy(result, "OK: Closing connection\r\n");
        close_client_socket(client_socket);
    } else if (strcmp(tokens[0], "shutdown") == 0) {
        strcpy(result, "OK: Server shutting down\r\n");
        running = 0;
    } else if (strcmp(tokens[0], "config") == 0) {
        if (token_count >= 3) {
            if (strcmp(tokens[1], "password") == 0) {
                if (strlen(tokens[2]) > 0) {
                    strncpy(server_password, tokens[2], sizeof(server_password) - 1);
                    server_password[sizeof(server_password) - 1] = '\0';
                    strcpy(result, "OK: Password changed successfully\r\n");
                } else {
                    strcpy(result, "ERROR: Password cannot be empty\r\n");
                }
            } else {
                sprintf(result, "ERROR: Unknown config option: %s\r\n", tokens[1]);
            }
        } else {
            strcpy(result, "ERROR: config command requires option and value\r\n");
            strcat(result, "       Available options: password\r\n");
        }
    } else {
        sprintf(result, "ERROR: Unknown command: %s\r\n", tokens[0]);
    }
    
    // Belleği temizle
    for (int i = 0; i < token_count; i++) {
        free(tokens[i]);
    }
    
    return result;
}

// AytDB telnet sunucusunu başlat
int start_server(int port) {
    struct sockaddr_in server_addr;
    
    // Tüm istemci socketleri ve kimlik doğrulama durumlarını -1 ile başlat
    for (int i = 0; i < MAX_CLIENTS; i++) {
        client_sockets[i] = -1;
        client_auth_status[i] = 0;
    }
    
    // TCP soketi oluştur
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("Could not create socket");
        return 1;
    }
    
    // SO_REUSEADDR ayarla (port yeniden kullanımı için)
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        close(server_socket);
        return 1;
    }
    
    // Sunucu adresini hazırla
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    
    // Soketi bağla
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_socket);
        return 1;
    }
    
    // Dinlemeye başla
    if (listen(server_socket, 5) < 0) {
        perror("Listen failed");
        close(server_socket);
        return 1;
    }
    
    printf("AytDB server started on port %d...\n", port);
    printf("To connect: telnet localhost %d\n", port);
    
    fd_set readfds;
    int max_sd, activity, new_socket, sd;
    int addrlen = sizeof(server_addr);
    char buffer[BUFFER_SIZE];
    
    while (running) {
        // fd_set'i temizle
        FD_ZERO(&readfds);
        
        // Sunucu soketini ekle
        FD_SET(server_socket, &readfds);
        max_sd = server_socket;
        
        // İstemci soketlerini ekle
        for (int i = 0; i < MAX_CLIENTS; i++) {
            sd = client_sockets[i];
            
            // Geçerli bir soket ise fd_set'e ekle
            if (sd > 0)
                FD_SET(sd, &readfds);
            
            // En yüksek soket tanımlayıcısını güncelle
            if (sd > max_sd)
                max_sd = sd;
        }
        
        // Soketleri bekle
        activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);
        
        if ((activity < 0) && (errno != EINTR)) {
            if (!running) break; // Sinyal nedeniyle çıkıyorsa sessizce çık
            perror("select error");
            continue;
        }
        
        // Yeni bağlantı var mı kontrol et
        if (FD_ISSET(server_socket, &readfds)) {
            if ((new_socket = accept(server_socket, (struct sockaddr *)&server_addr, (socklen_t*)&addrlen)) < 0) {
                perror("accept");
                continue;
            }
            
            printf("New connection, socket fd: %d, ip: %s, port: %d\n", 
                new_socket, inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));
            
            // Hoş geldin mesajı gönder
            char *welcome_message = "Welcome to AytDB!\r\nAuthentication required. Use 'auth <password>' command.\r\nType 'help' for available commands\r\n> ";
            send(new_socket, welcome_message, strlen(welcome_message), 0);
            
            // Soketi istemci soketi listesine ekle
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (client_sockets[i] == -1) {
                    client_sockets[i] = new_socket;
                    client_auth_status[i] = 0; // Yeni bağlantıyı kimlik doğrulaması yapılmamış olarak işaretle
                    printf("Client added to list, index: %d\n", i);
                    break;
                }
            }
        }
        
        // İstemcilerden gelen verileri kontrol et
        for (int i = 0; i < MAX_CLIENTS; i++) {
            sd = client_sockets[i];
            
            if (FD_ISSET(sd, &readfds)) {
                // İstemciden veri oku
                int valread = read(sd, buffer, BUFFER_SIZE);
                
                if (valread == 0) {
                    // İstemci bağlantıyı kapattı
                    printf("Client disconnected, socket fd: %d\n", sd);
                    close_client_socket(i);
                    client_auth_status[i] = 0; // Bağlantı kapandığında kimlik doğrulama durumunu sıfırla
                } else {
                    // Veriyi terminat et (null-sonlandırma)
                    buffer[valread] = '\0';
                    
                    // Yeni satır karakterlerini kaldır
                    buffer[strcspn(buffer, "\r\n")] = 0;
                    
                    // Komutu işle
                    if (strlen(buffer) > 0) {
                        printf("Command received from client: %s\n", buffer);
                        char* response = process_command(buffer, i);
                        if (response) {
                            send(sd, response, strlen(response), 0);
                            // Komut işlendikten sonra yeni prompt gönder
                            char *prompt = "> ";
                            send(sd, prompt, strlen(prompt), 0);
                            free(response);
                        }
                    }
                }
            }
        }
    }
    
    // Tüm soketleri kapat
    printf("Server shutting down...\n");
    if (server_socket != -1) {
        close(server_socket);
    }
    
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (client_sockets[i] != -1) {
            close(client_sockets[i]);
        }
    }
    
    return 0;
}

// Telnet sunucusunu başlat
int server_init(int port) {
    // Storage initialization
    storage = storage_init();
    if (!storage) {
        printf("Error: Failed to initialize storage\n");
        return 1;
    }
    
    printf("Storage initialized. Ready to process commands.\n");
    
    // Sinyal işleyiciyi ayarla
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Sunucuyu başlat
    return start_server(port);
} 