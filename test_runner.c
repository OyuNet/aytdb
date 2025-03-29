#include "test_runner.h"

void init_test_results(TestResults* results) {
    results->total_tests = 0;
    results->passed_tests = 0;
    results->failed_tests = 0;
    results->failed_count = 0;
}

void assert_true(TestResults* results, bool condition, const char* message) {
    if (!results || !message) return;
    
    results->total_tests++;
    if (!condition) {
        if (results->failed_count < 1000) {
            strncpy(results->failed_messages[results->failed_count], 
                    message, 255);
            results->failed_messages[results->failed_count][255] = '\0';
            results->failed_count++;
        }
        results->failed_tests++;
    } else {
        results->passed_tests++;
    }
}

void assert_false(TestResults* results, bool condition, const char* message) {
    assert_true(results, !condition, message);
}

void assert_equal(TestResults* results, int expected, int actual, const char* message) {
    char detailed_message[256];
    snprintf(detailed_message, 256, "%s (Expected: %d, Actual: %d)", 
             message, expected, actual);
    assert_true(results, expected == actual, detailed_message);
}

void assert_not_null(TestResults* results, void* ptr, const char* message) {
    char detailed_message[256];
    snprintf(detailed_message, 256, "%s (Pointer is NULL)", message);
    assert_true(results, ptr != NULL, detailed_message);
}

void assert_null(TestResults* results, void* ptr, const char* message) {
    char detailed_message[256];
    snprintf(detailed_message, 256, "%s (Pointer is not NULL)", message);
    assert_true(results, ptr == NULL, detailed_message);
}

void run_test(TestCase* test, TestResults* results) {
    printf("\nRunning test: %s\n", test->name);
    
    if (test->is_stress_test) {
        printf("Stress test mode - %d iterations\n", test->stress_iterations);
        clock_t start = clock();
        
        for (int i = 0; i < test->stress_iterations; i++) {
            test->func(results);
        }
        
        clock_t end = clock();
        double time_spent = (double)(end - start) / CLOCKS_PER_SEC;
        printf("Stress test completed in %.2f seconds\n", time_spent);
    } else {
        test->func(results);
    }
}

void print_test_results(TestResults* results) {
    printf("\n=== Test Results ===\n");
    printf("Total Tests: %d\n", results->total_tests);
    printf("Passed: %d\n", results->passed_tests);
    printf("Failed: %d\n", results->failed_tests);
    
    if (results->failed_count > 0) {
        printf("\nFailed Test Messages:\n");
        for (int i = 0; i < results->failed_count; i++) {
            printf("%d. %s\n", i + 1, results->failed_messages[i]);
        }
    }
    
    float success_rate = (float)results->passed_tests / results->total_tests * 100;
    printf("\nSuccess Rate: %.2f%%\n", success_rate);
}

void run_all_tests(TestCase* tests, int test_count) {
    TestResults results;
    init_test_results(&results);
    
    printf("Starting test suite with %d tests...\n", test_count);
    
    for (int i = 0; i < test_count; i++) {
        run_test(&tests[i], &results);
    }
    
    print_test_results(&results);
}