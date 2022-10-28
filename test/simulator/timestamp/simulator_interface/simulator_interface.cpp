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

#include <iostream>
#include <sstream>
#include <string>

#include "connection_simulator.h"

#define RESET "\033[0m"
#define RED "\033[31m"

int choose_num(int, int, const std::string &);
void print_border_msg(const std::string &, const std::string &);

void interface_begin_transaction();
void interface_commit_transaction();
void interface_prepare_transaction();
void interface_rollback_transaction();
void interface_set_timestamp();
void interface_timestamp_transaction();
void interface_timestamp_transaction_uint();
void print_rules();

int
main(int argc, char *argv[])
{
    bool exit = false;

    do {
        std::cout << std::endl;

        std::cout << "1: set_timestamp()" << std::endl;
        std::cout << "2: begin_transaction()" << std::endl;
        std::cout << "3: prepare_transaction()" << std::endl;
        std::cout << "4: timestamp_transaction()" << std::endl;
        std::cout << "5: commit_transaction()" << std::endl;
        std::cout << "6: rollback_transaction()" << std::endl;
        std::cout << "7: print rules for timestamps" << std::endl;
        std::cout << "8: exit" << std::endl;

        int choice = choose_num(1, 8, "timestamp_simulator >>");

        try {
            switch (choice) {
            case 1:
                interface_set_timestamp();
                break;
            case 2:
                interface_begin_transaction();
                break;
            case 3:
                interface_prepare_transaction();
                break;
            case 4:
                interface_timestamp_transaction();
                break;
            case 5:
                interface_commit_transaction();
                break;
            case 6:
                interface_rollback_transaction();
                break;
            case 7:
                print_rules();
                break;
            case 8:
                exit = true;
            }
        } catch (std::string &exception_str) {
            print_border_msg("exception: " + exception_str, RED);
        }

    } while (!exit);

    return (0);
}

void
print_border_msg(const std::string &msg, const std::string &color)
{
    const int count = msg.length() + 2;
    const std::string dash(count, '-');

    std::cout << color << "+" + dash + "+" << RESET << std::endl;
    std::cout << color << "| " << msg << " |" << RESET << std::endl;
    std::cout << color << "+" + dash + "+" << RESET << std::endl;
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
interface_set_timestamp()
{
}

void
interface_timestamp_transaction()
{
}

void
interface_timestamp_transaction_uint()
{
}

void
print_rules()
{
}
