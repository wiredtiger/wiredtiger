/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <fstream>
#include <iostream>
#include <string>

#include "test_harness/util/debug_utils.h"
#include "test_harness/test.h"

#include "example_test.cxx"
#include "poc_test.cxx"

std::string
parse_configuration_from_file(const std::string &filename)
{
    std::string cfg, line, error;
    std::ifstream cFile(filename);

    if (cFile.is_open()) {
        while (getline(cFile, line)) {
            /* Whitespaces are only for readability, they can be removed safely. */
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (line[0] == '#' || line.empty())
                continue;
            cfg += line;
        }

    } else {
        error = "Couldn't open " + filename + " file for reading.";
        testutil_die(EINVAL, error.c_str());
    }

    return (cfg);
}

void
print_help()
{
    std::cout << "NAME" << std::endl;
    std::cout << "\trun" << std::endl;
    std::cout << std::endl;
    std::cout << "SYNOPSIS" << std::endl;
    std::cout << "\trun [OPTIONS]" << std::endl;
    std::cout << "\trun -C [CONFIGURATION]" << std::endl;
    std::cout << "\trun -f [FILE]" << std::endl;
    std::cout << "\trun -l [TRACEL_LEVEL]" << std::endl;
    std::cout << "\trun -t [TEST_NAME]" << std::endl;
    std::cout << std::endl;
    std::cout << "DESCRIPTION" << std::endl;
    std::cout << "\trun  executes the test framework." << std::endl;
    std::cout << "\tIf no test is indicated, all tests are executed." << std::endl;
    std::cout
      << "\tIf no configuration is indicated, the default configuration for each test will be used."
      << std::endl;
    std::cout
      << "\tIf a configuration is indicated, the given configuration will be used either for "
         "all tests or the test indicated."
      << std::endl;
    std::cout << std::endl;
    std::cout << "OPTIONS" << std::endl;
    std::cout << "\t-h Output a usage message and exit." << std::endl;
    std::cout << "\t-C Configuration. Cannot be used with -f." << std::endl;
    std::cout << "\t-f File that contains the configuration. Cannot be used with -C." << std::endl;
    std::cout << "\t-l Trace level from 0 (default) to 2." << std::endl;
    std::cout << "\t-t Test name to be executed." << std::endl;
}

void
value_missing_error(const std::string &str)
{
    test_harness::debug_print(
      "Value missing for option " + str + ".\nTry './run -h' for more information.", DEBUG_ERROR);
}

/*
 * Run a specific test.
 * test_name: specifies which test to run.
 * config: defines the configuration used for the test.
 */
int64_t
run_test(const std::string &test_name, const std::string &config)
{
    int error_code = 0;

    test_harness::debug_print("Configuration\t:" + config, DEBUG_INFO);

    if (test_name == "poc_test")
        poc_test(config, test_name).run();
    else if (test_name == "example_test")
        example_test(config, test_name).run();
    else {
        test_harness::debug_print("Test not found: " + test_name, DEBUG_ERROR);
        error_code = -1;
    }

    if (error_code == 0)
        test_harness::debug_print("Test " + test_name + " done.", DEBUG_INFO);

    return (error_code);
}

int
main(int argc, char *argv[])
{
    std::string cfg, config_filename, test_name, current_test_name;
    int64_t error_code = 0;
    const std::vector<std::string> all_tests = {"example_test", "poc_test"};

    /* Parse args
     * -C   : Configuration. Cannot be used with -f. If no specific test is specified to be run, the
     * same coniguration will be used for all existing tests.
     * -f   : Filename that contains the configuration. Cannot be used with -C. If no specific test
     * is specified to be run, the same coniguration will be used for all existing tests.
     * -l   : Trace level.
     * -t   : Test to run. All tests are run if not specified.
     */
    for (size_t i = 1; (i < argc) && (error_code == 0); ++i) {
        if (std::string(argv[i]) == "-h") {
            print_help();
            return 0;
        } else if (std::string(argv[i]) == "-C") {
            if (!config_filename.empty()) {
                test_harness::debug_print("Option -C cannot be used with -f", DEBUG_ERROR);
                error_code = -1;
            } else if ((i + 1) < argc)
                cfg = argv[++i];
            else {
                value_missing_error(argv[i]);
                error_code = -1;
            }
        } else if (std::string(argv[i]) == "-f") {
            if (!cfg.empty()) {
                test_harness::debug_print("Option -f cannot be used with -C", DEBUG_ERROR);
                error_code = -1;
            } else if ((i + 1) < argc)
                config_filename = argv[++i];
            else {
                value_missing_error(argv[i]);
                error_code = -1;
            }
        } else if (std::string(argv[i]) == "-t") {
            if ((i + 1) < argc)
                test_name = argv[++i];
            else {
                value_missing_error(argv[i]);
                error_code = -1;
            }
        } else if (std::string(argv[i]) == "-l") {
            if ((i + 1) < argc)
                test_harness::_trace_level = std::stoi(argv[++i]);
            else {
                value_missing_error(argv[i]);
                error_code = -1;
            }
        }
    }

    if (error_code == 0) {
        test_harness::debug_print(
          "Trace level\t:" + std::to_string(test_harness::_trace_level), DEBUG_INFO);
        if (test_name.empty()) {
            /* Run all tests. */
            test_harness::debug_print("Running all tests.", DEBUG_INFO);
            for (auto const &it : all_tests) {
                current_test_name = it;
                /* Configuration parsing. */
                if (!config_filename.empty())
                    cfg = parse_configuration_from_file(config_filename);
                else if (cfg.empty()) {
                    config_filename = "configs/config_" + current_test_name + "_default.txt";
                    cfg = parse_configuration_from_file(config_filename);
                }

                error_code = run_test(current_test_name, cfg);
                if (error_code != 0)
                    break;
            }
        } else {
            current_test_name = test_name;
            /* Configuration parsing. */
            if (!config_filename.empty())
                cfg = parse_configuration_from_file(config_filename);
            else if (cfg.empty()) {
                config_filename = "configs/config_" + test_name + "_default.txt";
                cfg = parse_configuration_from_file(config_filename);
            }
            error_code = run_test(current_test_name, cfg);
        }

        if (error_code != 0)
            test_harness::debug_print("Test " + current_test_name + " failed.", DEBUG_ERROR);
    }

    return (error_code);
}
