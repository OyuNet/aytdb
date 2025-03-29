#ifndef AYTDB_TEST_RUNNER_H
#define AYTDB_TEST_RUNNER_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_FAILED_MESSAGES 1000
#define MAX_MESSAGE_LENGTH 256

// Test sonuçlarını tutan yapı
typedef struct {
    int total_tests;
    int passed_tests;
    int failed_tests;
    char failed_messages[MAX_FAILED_MESSAGES][MAX_MESSAGE_LENGTH];
    int failed_count;
} TestResults;

// Test fonksiyonu tipi
typedef void (*TestFunction)(TestResults* results);

// Test case yapısı
typedef struct {
    const char* name;
    TestFunction func;
    bool is_stress_test;
    int stress_iterations;
} TestCase;

// Test runner fonksiyonları
void init_test_results(TestResults* results);
void run_test(TestCase* test, TestResults* results);
void run_all_tests(TestCase* tests, int test_count);
void print_test_results(TestResults* results);

// Test yardımcı fonksiyonları
void assert_true(TestResults* results, bool condition, const char* message);
void assert_false(TestResults* results, bool condition, const char* message);
void assert_equal(TestResults* results, int expected, int actual, const char* message);
void assert_not_null(TestResults* results, void* ptr, const char* message);
void assert_null(TestResults* results, void* ptr, const char* message);

#endif // AYTDB_TEST_RUNNER_H