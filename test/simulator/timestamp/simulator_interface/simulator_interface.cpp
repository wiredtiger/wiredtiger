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

#include "simulator_interface.h"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

void
print_border_msg(const std::string &msg, const std::string &color)
{
    const int count = msg.length() + 2;
    const std::string dash(count, '-');

    std::cout << color << "+" + dash + "+" << RESET << std::endl;
    std::cout << color << "| " << msg << " |" << RESET << std::endl;
    std::cout << color << "+" + dash + "+" << RESET << std::endl;
}

void
print_options(const std::vector<std::string> &options)
{
    for (int i = 0; i < options.size(); i++)
        std::cout << i + 1 << ": " << options[i] << std::endl;
}

int
choose_num(int min, int max, const std::string &cli_str)
{
    std::string text_line;
    int choice;

    do {
        std::cout << "\n" << cli_str << " ";
        std::getline(std::cin, text_line);

        /* Extract number from the text. */
        std::istringstream text_stream(text_line);
        text_stream >> choice;

        /* Validate the number of choice. */
        if (choice < min || choice > max)
            print_border_msg(
              "Choose a number between " + std::to_string(min) + " and " + std::to_string(max),
              RED);

    } while (choice < min || choice > max);

    return (choice);
}

void
interface_session_management()
{
}

void
interface_set_timestamp()
{
}

void
interface_conn_query_timestamp()
{
}

void
interface_begin_transaction()
{
}

void
interface_commit_transaction()
{
}

void
interface_prepare_transaction()
{
}

void
interface_rollback_transaction()
{
}

void
interface_timestamp_transaction()
{
}

void
interface_session_query_timestamp()
{
}

void
print_rules()
{
    bool exit = false;
    std::vector<std::string> options;
    options.push_back("oldest and stable timestamp");
    options.push_back("commit timestamp");
    options.push_back("prepare timestamp");
    options.push_back("durable timestamp");
    options.push_back("read timestamp");
    options.push_back("<- go back");

    do {
        std::cout << std::endl;

        print_options(options);

        int choice = choose_num(1, options.size(), "Choose timestamp >>");

        switch (choice) {
        case 1:
            print_border_msg("Timestamp value should be greater than 0.", WHITE);
            print_border_msg(
              "It is a no-op to set the oldest or stable timestamps behind the global values.",
              WHITE);
            print_border_msg("Oldest must not be greater than the stable timestamp", WHITE);
            break;
        case 2:
            print_border_msg(
              "The commit_ts cannot be less than the first_commit_timestamp.", WHITE);
            print_border_msg("The commit_ts cannot be less than the oldest timestamp.", WHITE);
            print_border_msg("The commit timestamp must be after the stable timestamp.", WHITE);
            print_border_msg("The commit_ts cannot be less than the prepared_ts", WHITE);
            break;
        case 3:
            print_border_msg(
              "Cannot set the prepared timestamp if the transaction is already prepared.", WHITE);
            print_border_msg("Cannot set prepared timestamp more than once.", WHITE);
            print_border_msg(
              "Commit timestamp should not have been set before the prepare timestamp.", WHITE);
            print_border_msg(
              "Prepare timestamp must be greater than the latest active read timestamp.", WHITE);
            print_border_msg("Prepare timestamp cannot be less than the stable timestamp", WHITE);
            break;
        case 4:
            print_border_msg(
              "Durable timestamp should not be specified for non-prepared transaction.", WHITE);
            print_border_msg(
              "Commit timestamp is required before setting a durable timestamp.", WHITE);
            print_border_msg(
              "The durable timestamp should not be less than the oldest timestamp.", WHITE);
            print_border_msg("The durable timestamp must be after the stable timestamp.", WHITE);
            print_border_msg(
              "The durable timestamp should not be less than the commit timestamp.", WHITE);
            break;
        case 5:
            print_border_msg(
              "The read timestamp can only be set before a transaction is prepared.", WHITE);
            print_border_msg("Read timestamps can only be set once.", WHITE);
            print_border_msg(
              "The read timestamp must be greater than or equal to the oldest timestamp.", WHITE);
            break;
        case 6:
            exit = true;
        }
    } while (!exit);
}

int
main(int argc, char *argv[])
{
    bool exit = false;
    std::vector<std::string> options;
    options.push_back("Session Management");
    options.push_back("[Conn] set_timestamp()");
    options.push_back("[Conn] query_timestamp()");
    options.push_back("[Session] begin_transaction()");
    options.push_back("[Session] commit_transaction()");
    options.push_back("[Session] prepare_transaction()");
    options.push_back("[Session] rollback_transaction()");
    options.push_back("[Session] timestamp_transaction()");
    options.push_back("[Session] query_timestamp()");
    options.push_back("Print rules for timestamps");
    options.push_back("Exit");

    do {
        std::cout << std::endl;

        print_options(options);

        int choice = choose_num(1, options.size(), "timestamp_simulator >>");

        try {
            switch (choice) {
            case 1:
                interface_session_management();
                break;
            case 2:
                interface_set_timestamp();
                break;
            case 3:
                interface_conn_query_timestamp();
                break;
            case 4:
                interface_begin_transaction();
                break;
            case 5:
                interface_commit_transaction();
                break;
            case 6:
                interface_prepare_transaction();
                break;
            case 7:
                interface_rollback_transaction();
                break;
            case 8:
                interface_timestamp_transaction();
                break;
            case 9:
                interface_session_query_timestamp();
                break;
            case 10:
                print_rules();
                break;
            case 11:
                exit = true;
            }
        } catch (const std::string &exception_str) {
            print_border_msg("exception: " + exception_str, RED);
        }

    } while (!exit);

    return (0);
}
