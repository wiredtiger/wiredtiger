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

#include <algorithm>
#include <iostream>
#include <string>

#include "src/common/logger.h"
#include "src/main/test.h"

#include "bounded_cursor_perf.cpp"
#include "burst_inserts.cpp"
#include "cache_resize.cpp"
#include "cursor_bound_01.cpp"
#include "hs_cleanup.cpp"
#include "operations_test.cpp"
#include "search_near_01.cpp"
#include "search_near_02.cpp"
#include "search_near_03.cpp"
#include "test_template.cpp"

extern "C" {
#include "test_util.h"
}

/* Declarations to avoid the error raised by -Werror=missing-prototypes. */
const std::string ParseConfigurationFromFile(const std::string &filename);
void PrintHelp();
int64_t RunTest(
  const std::string &testName, const std::string &config, const std::string &wtOpenConfig);

const std::string
ParseConfigurationFromFile(const std::string &filename)
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
PrintHelp()
{
    std::cout << "NAME" << std::endl;
    std::cout << "\trun" << std::endl;
    std::cout << std::endl;
    std::cout << "SYNOPSIS" << std::endl;
    std::cout << "\trun [OPTIONS]" << std::endl;
    std::cout << "\trun -C [WIREDTIGER_OPEN_CONFIGURATION]" << std::endl;
    std::cout << "\trun -c [TEST_FRAMEWORK_CONFIGURATION]" << std::endl;
    std::cout << "\trun -f [FILE]" << std::endl;
    std::cout << "\trun -l [TRACE_LEVEL]" << std::endl;
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
    std::cout << "\t-C Additional wiredtiger open configuration." << std::endl;
    std::cout << "\t-c Test framework configuration. Cannot be used with -f." << std::endl;
    std::cout << "\t-f File that contains the configuration. Cannot be used with -C." << std::endl;
    std::cout << "\t-l Trace level from 0 to 3. "
                 "1 is the default level, all warnings and errors are logged."
              << std::endl;
    std::cout << "\t-t Test name to be executed." << std::endl;
}

/*
 * Run a specific test.
 * - testName: specifies which test to run.
 * - config: defines the configuration used for the test.
 */
int64_t
RunTest(const std::string &testName, const std::string &config, const std::string &wtOpenConfig)
{
    int error = 0;

    test_harness::Logger::LogMessage(LOG_TRACE, "Configuration\t:" + config);
    test_harness::test_args args = {
      .testConfig = config, .testName = testName, .wtOpenConfig = wtOpenConfig};

    if (testName == "bounded_cursor_perf")
        BoundedCursorPerf(args).Run();
    else if (testName == "burst_inserts")
        BurstInserts(args).Run();
    else if (testName == "cache_resize")
        CacheResize(args).Run();
    else if (testName == "cursor_bound_01")
        cursor_bound_01(args).Run();
    else if (testName == "hs_cleanup")
        HsCleanup(args).Run();
    else if (testName == "operations_test")
        OperationsTest(args).Run();
    else if (testName == "search_near_01")
        SearchNear01(args).Run();
    else if (testName == "search_near_02")
        SearchNear02(args).Run();
    else if (testName == "search_near_03")
        SearchNear03(args).Run();
    else if (testName == "test_template")
        TestTemplate(args).Run();
    else {
        test_harness::Logger::LogMessage(LOG_ERROR, "Test not found: " + testName);
        error = -1;
    }

    if (error == 0)
        test_harness::Logger::LogMessage(LOG_INFO, "Test " + testName + " done.");

    return (error);
}

static std::string
GetDefaultConfigPath(const std::string &testName)
{
    return ("configs/" + testName + "_default.txt");
}

int
main(int argc, char *argv[])
{
    std::string cfg, configFilename, currentConfig, testName, wtOpenConfig;
    int64_t error = 0;
    const std::vector<std::string> all_tests = {"bounded_cursor_perf", "burst_inserts",
      "cache_resize", "cursor_bound_01", "hs_cleanup", "operations_test", "search_near_01",
      "search_near_02", "search_near_03", "test_template"};

    /* Set the program name for error messages. */
    (void)testutil_set_progname(argv);

    /* Parse args
     * -C   : Additional wiredtiger_open configuration.
     * -c   : Test framework configuration. Cannot be used with -f. If no specific test is specified
     * to be run, the same configuration will be used for all existing tests.
     * -f   : Filename that contains the configuration. Cannot be used with -C. If no specific test
     * is specified to be run, the same configuration will be used for all existing tests.
     * -l   : Trace level.
     * -t   : Test to run. All tests are run if not specified.
     */
    for (size_t i = 1; (i < argc) && (error == 0); ++i) {
        if (std::string(argv[i]) == "-h") {
            PrintHelp();
            return 0;
        } else if (std::string(argv[i]) == "-C") {
            if ((i + 1) < argc) {
                wtOpenConfig = argv[++i];
                /* Add a comma to the front if the user didn't supply one. */
                if (wtOpenConfig[0] != ',')
                    wtOpenConfig.insert(0, 1, ',');
            } else
                error = -1;
        } else if (std::string(argv[i]) == "-c") {
            if (!configFilename.empty()) {
                test_harness::Logger::LogMessage(LOG_ERROR, "Option -C cannot be used with -f");
                error = -1;
            } else if ((i + 1) < argc)
                cfg = argv[++i];
            else
                error = -1;
        } else if (std::string(argv[i]) == "-f") {
            if (!cfg.empty()) {
                test_harness::Logger::LogMessage(LOG_ERROR, "Option -f cannot be used with -C");
                error = -1;
            } else if ((i + 1) < argc)
                configFilename = argv[++i];
            else
                error = -1;
        } else if (std::string(argv[i]) == "-t") {
            if ((i + 1) < argc)
                testName = argv[++i];
            else
                error = -1;
        } else if (std::string(argv[i]) == "-l") {
            if ((i + 1) < argc)
                test_harness::Logger::traceLevel = std::stoi(argv[++i]);
            else
                error = -1;
        } else
            error = -1;
    }

    if (error == 0) {
        test_harness::Logger::LogMessage(
          LOG_INFO, "Trace level: " + std::to_string(test_harness::Logger::traceLevel));
        std::string currentTestName;
        if (testName.empty()) {
            /* Run all tests. */
            test_harness::Logger::LogMessage(LOG_INFO, "Running all tests.");
            for (auto const &it : all_tests) {
                currentTestName = it;
                /* Configuration parsing. */
                if (!configFilename.empty())
                    currentConfig = ParseConfigurationFromFile(configFilename);
                else if (cfg.empty())
                    currentConfig =
                      ParseConfigurationFromFile(GetDefaultConfigPath(currentTestName));
                else
                    currentConfig = cfg;

                error = RunTest(currentTestName, currentConfig, wtOpenConfig);
                /*
                 * The connection is usually closed using the destructor of the connection manager.
                 * Because it is a singleton and we are executing all tests, we are not going
                 * through its destructor between each test, we need to close the connection
                 * manually before starting the next test.
                 */
                ConnectionManager::GetInstance().Close();
                if (error != 0)
                    break;
            }
        } else {
            currentTestName = testName;
            /* Check the test exists. */
            if (std::find(all_tests.begin(), all_tests.end(), currentTestName) == all_tests.end()) {
                test_harness::Logger::LogMessage(
                  LOG_ERROR, "The test " + currentTestName + " was not found.");
                error = -1;
            } else {
                /* Configuration parsing. */
                if (!configFilename.empty())
                    cfg = ParseConfigurationFromFile(configFilename);
                else if (cfg.empty())
                    cfg = ParseConfigurationFromFile(GetDefaultConfigPath(currentTestName));
                error = RunTest(currentTestName, cfg, wtOpenConfig);
            }
        }

        if (error != 0)
            test_harness::Logger::LogMessage(LOG_ERROR, "Test " + currentTestName + " failed.");
    } else
        test_harness::Logger::LogMessage(LOG_ERROR,
          "Invalid command line arguments supplied. Try "
          "'./run -h' for help.");

    return (error);
}
