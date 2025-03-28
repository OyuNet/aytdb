cmake_minimum_required(VERSION 3.30)
project(aytdb C)

set(CMAKE_C_STANDARD 23)

add_executable(aytdb main.c
        kv_store.h
        hash_util.h
        entry.h
        kv_store.c
        storage.h
        storage.c)
