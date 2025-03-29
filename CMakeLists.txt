cmake_minimum_required(VERSION 3.10)
project(aytdb C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Ana proje kaynak dosyaları
add_executable(aytdb
    main.c
    storage.c
    kv_store.c
    hash_util.c
)

# Telnet sunucusu modülü
add_executable(aytdb_server
    server_main.c
    server.c
    storage.c
    kv_store.c
    hash_util.c
)

# Test kaynak dosyaları
add_executable(aytdb_test
    test_runner.c
    test_storage.c
    storage.c
    kv_store.c
    hash_util.c
)

# Test çalıştırma hedefi
add_custom_target(run_tests
    COMMAND aytdb_test
    DEPENDS aytdb_test
)

# Telnet sunucusu çalıştırma hedefi
add_custom_target(run_server
    COMMAND aytdb_server
    DEPENDS aytdb_server
)
