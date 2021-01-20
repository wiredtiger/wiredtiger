/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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
 *
 * ex_col_store.c
 *	This is an example application that demonstrates
 *	how to perform operations with column store.
 */

#include <test_util.h>
#include <assert.h>

#define NUM_ENTRIES 100
#define TABLE_NAME "table:weathertable"

static const char *home;

typedef struct {
    char day[5];
    uint16_t hour;
    uint8_t temp;
    uint8_t humidity;
    uint16_t pressure;
    uint8_t wind;
    uint8_t feels_like_temp;
    uint16_t loc_lat;
    uint16_t loc_long;
    char country[5];
} WEATHER;

static void update_celsius_to_fahrenheit(WT_SESSION *session);
static void print_temp_column(WT_SESSION *session);
static void print_all_columns(WT_SESSION *session);
static void chance_of_rain(WT_SESSION *session);
static void generate_data(WEATHER *w_array);
static void remove_country(WT_SESSION *session);
static void average_data(WT_SESSION *session);
static bool find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time, int *result);
static bool find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time, int *result);

static void
print_all_columns(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    const char *country;
    int ret;
    uint64_t recno;
    const char *day;
    uint16_t hour, pressure, loc_lat, loc_long;
    uint8_t humidity, temp, wind, feels_like_temp;

    error_check(session->open_cursor(session, TABLE_NAME, NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &day, &hour, &temp, &humidity, &pressure, &wind,
          &feels_like_temp, &loc_lat, &loc_long, &country));

        printf(
          "{\n"
          "    ID: %" PRIu64
          "\n"
          "    day: %s\n"
          "    hour: %" PRIu16
          "\n"
          "    temp: %" PRIu8
          "\n"
          "    humidity: %" PRIu8
          "\n"
          "    pressure: %" PRIu16
          "\n"
          "    wind: %" PRIu8
          "\n"
          "    feels like: %" PRIu8
          "\n"
          "    lat: %" PRIu16
          "\n"
          "    long: %" PRIu16
          "\n"
          "    country: %s\n"
          "}\n\n",
          recno, day, hour, temp, humidity, pressure, wind, feels_like_temp, loc_lat, loc_long,
          country);
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

static void
print_temp_column(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int ret;
    uint8_t temp;

    error_check(
      session->open_cursor(session, "colgroup:weathertable:temperature", NULL, NULL, &cursor));
    printf("Temperature: \n");
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_value(cursor, &temp));
        printf("%d\n", temp);
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

static void
update_celsius_to_fahrenheit(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int ret;
    uint8_t temp;
    uint8_t temp_in_fahrenheit;

    error_check(
      session->open_cursor(session, "colgroup:weathertable:temperature", NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_value(cursor, &temp));

        /*
         * Update the value from celsius to fahrenheit. Discarding decimals and keeping data simple
         * by type casting to uint8_t.
         */
        temp_in_fahrenheit = (uint8_t)((1.8 * temp) + 32.0);

        cursor->set_value(cursor, temp_in_fahrenheit);
        error_check(cursor->update(cursor));
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

static void
chance_of_rain(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;
    uint8_t humidity;
    uint16_t pressure;

    error_check(session->open_cursor(
      session, "colgroup:weathertable:humidity_pressure", NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &humidity, &pressure));
        if (humidity > 70 && pressure < 1000) {
            printf("Rain likely\n");
        } else {
            printf("Rain unlikely\n");
        }
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

static void
remove_country(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;
    uint16_t loc_lat, loc_long;
    const char *country;

    error_check(
      session->open_cursor(session, "colgroup:weathertable:location", NULL, NULL, &cursor));
    /*
     * All Australian data is being removed, to test if deletion works.
     */
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &loc_lat, &loc_long, &country));
        if (strcmp("AUS", country) == 0) {
            printf("Removing %s\n", country);
            error_check(cursor->remove(cursor));
        }
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

static void
generate_data(WEATHER *w_array)
{
    int day;
    int country;

    srand((unsigned int)getpid());

    for (int i = 0; i < NUM_ENTRIES; i++) {
        WEATHER w;
        day = rand() % 7;

        switch (day) {
        case 0:
            strcpy(w.day, "MON");
            break;
        case 1:
            strcpy(w.day, "TUE");
            break;
        case 2:
            strcpy(w.day, "WED");
            break;
        case 3:
            strcpy(w.day, "THU");
            break;
        case 4:
            strcpy(w.day, "FRI");
            break;
        case 5:
            strcpy(w.day, "SAT");
            break;
        case 6:
            strcpy(w.day, "SUN");
            break;
        default:
            assert(false);
        }
        w.hour = rand() % 2401;
        w.temp = rand() % 51;
        w.humidity = rand() % 101;
        w.pressure = rand() % (1100 + 1 - 900) + 900;
        w.wind = rand() % 201;
        w.loc_lat = rand() % 181;
        w.loc_long = rand() % 91;
        country = rand() % 7;
        switch (country) {
        case 0:
            strcpy(w.country, "AUS");
            break;
        case 1:
            strcpy(w.country, "UK");
            break;
        case 2:
            strcpy(w.country, "US");
            break;
        case 3:
            strcpy(w.country, "NZ");
            break;
        case 4:
            strcpy(w.country, "IND");
            break;
        case 5:
            strcpy(w.country, "CHI");
            break;
        case 6:
            strcpy(w.country, "RUS");
            break;
        default:
            assert(false);
        }

        w_array[i] = w;
    }
}

static bool
find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time, int *result)
{
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int exact;
    int ret = 0;
    uint64_t recno;
    uint16_t hour;
    uint8_t temp;

    /* Open cursors needed by the join. */
    error_check(session->open_cursor(
      session, "join:table:weathertable(hour,temp)", NULL, NULL, &join_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &start_time_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &end_time_cursor));

    /*
     * Select values WHERE (hour >= start AND hour <= end). Find the starting record closest to
     * desired start time.
     */
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search_near(start_time_cursor, &exact));
    switch (exact) {
    case -1:
        ret = start_time_cursor->next(start_time_cursor);
        break;
    default:
        break;
    }

    if (ret == 0) {
        error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge"));
    } else {
        return false;
    }

    /* Find the ending record closest to desired end time. */
    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search_near(end_time_cursor, &exact));
    switch (exact) {
    case 1:
        ret = end_time_cursor->prev(end_time_cursor);
        break;
    default:
        break;
    }

    if (ret == 0) {
        error_check(session->join(session, join_cursor, end_time_cursor, "compare=le"));
    } else {
        return false;
    }

    /* Initialize minimum temperature to temperature of the first record. */
    ret = join_cursor->next(join_cursor);
    if (ret != 0) {
        return false;
    }

    error_check(join_cursor->get_key(join_cursor, &recno));
    error_check(join_cursor->get_value(join_cursor, &hour, &temp));
    *result = temp;

    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp < *result) {
            *result = temp;
        }

        /* For debugging purposes */
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
    }

    return true;
}

static bool
find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time, int *result)
{
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int exact;
    int ret = 0;
    uint64_t recno;
    uint16_t hour;
    uint8_t temp;

    /* Open cursors needed by the join. */
    error_check(session->open_cursor(
      session, "join:table:weathertable(hour,temp)", NULL, NULL, &join_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &start_time_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &end_time_cursor));

    /* select values WHERE (hour >= start AND hour <= end) */
    /* Find the starting record closest to desired start time. */
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search_near(start_time_cursor, &exact));
    switch (exact) {
    case -1:
        ret = start_time_cursor->next(start_time_cursor);
        break;
    default:
        break;
    }

    if (ret == 0) {
        error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge"));
    } else {
        return false;
    }

    /* Find the ending record closest to desired end time. */
    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search_near(end_time_cursor, &exact));
    switch (exact) {
    case 1:
        ret = end_time_cursor->prev(end_time_cursor);
        break;
    default:
        break;
    }
    if (ret == 0) {
        error_check(session->join(session, join_cursor, end_time_cursor, "compare=le"));
    } else {
        return false;
    }

    /* Initialize maximum temperature to temperature of the first record. */
    join_cursor->next(join_cursor);
    if (ret != 0) {
        return false;
    }

    error_check(join_cursor->get_key(join_cursor, &recno));
    error_check(join_cursor->get_value(join_cursor, &hour, &temp));
    *result = temp;

    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp > *result) {
            *result = temp;
        }

        /* For debugging purposes */
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
    }

    return true;
}
/*! [Obtains the average data across all fields given a specific location] */
void
average_data(WT_SESSION *session)
{
    WT_CURSOR *loc_cursor;
    const char *day;
    const char *country;
    uint64_t recno;
    uint16_t hour, pressure, loc_long, loc_lat;
    uint8_t temp, humidity, wind, feels_like_temp;
    /* num_rec is the total number of records we're obtaining averages for. */
    int ret, num_rec = 5, exact;
    unsigned int count = 0;
    /* rec_arr holds the sum of the records in order to obtain the averages. */
    unsigned int rec_arr[5] = {0, 0, 0, 0, 0};
    /* Open a cursor to search for the location, currently RUS. */
    error_check(
      session->open_cursor(session, "index:weathertable:country", NULL, NULL, &loc_cursor));
    loc_cursor->set_key(loc_cursor, "RUS\0\0");
    error_check(loc_cursor->search_near(loc_cursor, &exact));
    /* Error handling in the case RUS is not found. In this case as it's a hardcoded location,
     *  if there aren't any matching locations, no average data is obtained.
     * */
    if (exact != 0) {
        return;
    }
    /* Populate the array with the totals of each of the columns. */
    while ((ret = loc_cursor->next(loc_cursor)) == 0) {
        count++;
        error_check(loc_cursor->get_key(loc_cursor, &recno));
        error_check(loc_cursor->get_value(loc_cursor, &day, &hour, &temp, &humidity, &pressure,
          &wind, &feels_like_temp, &loc_lat, &loc_long, &country));
        /* Increment the values of the rec_arr with the temp_arr values. */
        rec_arr[0] += temp;
        rec_arr[1] += humidity;
        rec_arr[2] += pressure;
        rec_arr[3] += wind;
        rec_arr[4] += feels_like_temp;
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(loc_cursor->close(loc_cursor));
    /* For debugging purposes. */
    printf("Number of matching entries: %u \n", count);
    /* Get the average values by dividing with the total number of records. */
    for (int i = 0; i < num_rec; i++) {
        rec_arr[i] = rec_arr[i] / count;
    }
    /* List the average records */
    printf("Average records for location RUS : \nTemp: %" PRIu8 ", Humidity: %" PRIu8
           ", Pressure: %" PRIu16 ", Wind: %" PRIu8 ", Feels like: %" PRIu8 "\n",
      rec_arr[0], rec_arr[1], rec_arr[2], rec_arr[3], rec_arr[4]);
}

int
main(int argc, char *argv[])
{
    WEATHER *w;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    WEATHER weather_data[NUM_ENTRIES];
    uint16_t start, end;
    int min_temp_result, max_temp_result;

    start = 1000;
    end = 2000;
    min_temp_result = 0;

    /* Generating random data to populate the weather table */
    generate_data(weather_data);

    home = example_setup(argc, argv);

    /* Establishing a connection */
    error_check(wiredtiger_open(home, NULL, "create,statistics=(fast)", &conn));

    /* Establishing a session */
    error_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create a table with columns and colgroup */
    error_check(session->create(session, TABLE_NAME,
      "key_format=r,value_format=" WT_UNCHECKED_STRING(
        5sHBBHBBHH5s) ",columns=(id,day,hour,temp,"
                      "humidity,pressure,wind,"
                      "feels_like_temp,loc_lat,loc_long,country),colgroups=(day_time,temperature,"
                      "humidity_pressure,"
                      "wind,feels_like_temp,location)"));

    /* Create the colgroups */
    error_check(session->create(session, "colgroup:weathertable:day_time", "columns=(day,hour)"));
    error_check(session->create(session, "colgroup:weathertable:temperature", "columns=(temp)"));
    error_check(session->create(
      session, "colgroup:weathertable:humidity_pressure", "columns=(humidity,pressure)"));
    error_check(session->create(session, "colgroup:weathertable:wind", "columns=(wind)"));
    error_check(session->create(
      session, "colgroup:weathertable:feels_like_temp", "columns=(feels_like_temp)"));
    error_check(session->create(
      session, "colgroup:weathertable:location", "columns=(loc_lat,loc_long,country)"));

    /* Open a cursor on the table to insert the data ---[[[[ INSERT ]]]]--- */
    error_check(session->open_cursor(session, TABLE_NAME, NULL, "append", &cursor));
    w = weather_data;
    for (int i = 0; i < NUM_ENTRIES; i++) {
        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure, w->wind,
          w->feels_like_temp, w->loc_lat, w->loc_long, w->country);
        error_check(cursor->insert(cursor));
        w++;
    }
    /* Close cursor */
    error_check(cursor->close(cursor));

    /* Prints all the data in the database */
    print_all_columns(session);

    /* Update the temperature from Celsius to Fahrenheit */
    print_temp_column(session);
    update_celsius_to_fahrenheit(session);
    print_temp_column(session);

    /* Create indexes for searching */
    error_check(session->create(session, "index:weathertable:hour", "columns=(hour)"));
    error_check(session->create(session, "index:weathertable:country", "columns=(country)"));

    /* Calling the example operations */
    if (find_min_temp(session, start, end, &min_temp_result)) {
        printf("The minimum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", start, end,
          min_temp_result);
    } else {
        printf("Invalid start and end time range, please try again.");
    }

    max_temp_result = 0;
    if (find_max_temp(session, start, end, &max_temp_result)) {
        printf("The maximum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", start, end,
          max_temp_result);
    } else {
        printf("Invalid start and end time range, please try again.");
    }

    chance_of_rain(session);
    remove_country(session);
    print_all_columns(session);
    average_data(session);

    /* Close the connection */
    error_check(conn->close(conn, NULL));
    return (EXIT_SUCCESS);
}
