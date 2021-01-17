#include <test_util.h>

#define N_DATA 100

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

uint8_t celcius_to_farenheit(uint8_t temp_in_celcius);
void update_celcius_to_farenheit(WT_SESSION *session);
void print_temp_column(WT_SESSION *session);
void print_all_columns(WT_SESSION *session);
void chance_of_rain(WT_SESSION *session);
void generate_data(WEATHER *w_array);
void remove_country(WT_SESSION *session);
void search_temperature(WT_SESSION *session);
int average_data(WT_SESSION *session);
int find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time);
int find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time);

void
print_all_columns(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    const char *country;
    int ret;
    uint64_t recno;
    const char *day;
    uint16_t hour, pressure, loc_lat, loc_long;
    uint8_t humidity, temp, wind, feels_like_temp;

    error_check(session->open_cursor(session, "table:weathertable", NULL, NULL, &cursor));
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

void
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

uint8_t
celcius_to_farenheit(uint8_t temp_in_celcius)
{
    return (uint8_t)(1.8 * temp_in_celcius) + 32.0;
}

void
update_celcius_to_farenheit(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int ret;
    uint8_t temp;
    error_check(
      session->open_cursor(session, "colgroup:weathertable:temperature", NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_value(cursor, &temp));
        cursor->set_value(cursor, celcius_to_farenheit(temp));
        error_check(cursor->update(cursor));
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));
}

void
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
    return;
}

void
remove_country(WT_SESSION *session){
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;
    uint16_t loc_lat;
    uint16_t loc_long;
    const char *country;

    error_check(session->open_cursor(
      session, "colgroup:weathertable:location", NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &loc_lat, &loc_long, &country));
        if (!strcmp("AUS",country)){
            printf("Removing %s\n", country);
            error_check(cursor->remove(cursor));
        }
    } 

    return;
}

void
generate_data(WEATHER *w_array)
{
    // rand() % (max_number + 1 - minimum_number) + minimum_number
    int day;
    int country;
    for (int i = 0; i < N_DATA; i++) {
        WEATHER w;
        day = rand() % (6 + 1 - 0) + 0;
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
        }
        w.hour = rand() % (2400 + 1 - 0) + 0;
        w.temp = rand() % (50 + 1 - 0) + 0;
        w.humidity = rand() % (100 + 1 - 0) + 0;
        w.pressure = rand() % (1100 + 1 - 900) + 900;
        w.wind = rand() % (200 + 1 - 0) + 0;
        w.loc_lat = rand() % (180 + 1 - 0) + 0;
        w.loc_long = rand() % (90 + 1 - 0) + 0;
        country = rand() % (6 + 1 - 0) + 0;
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
        }

        w_array[i] = w;
    }
    return;
}

int
find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time)
{
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int min_so_far;
    int ret;
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
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search(start_time_cursor));
    error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge,count=10"));

    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search(end_time_cursor));
    error_check(session->join(session, join_cursor, end_time_cursor, "compare=le,count=10"));

    /* 
    Initialize minimum temperature to temperature of the first record.
    Assumes at least one record is found in weather records. 
    */
    join_cursor->next(join_cursor);
    error_check(join_cursor->get_key(join_cursor, &recno));
    error_check(join_cursor->get_value(join_cursor, &hour, &temp));
    min_so_far = temp;

    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp < min_so_far) {
            min_so_far = temp;
        }

        /* For debugging purposes */
        /* 
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
        */
    }

    return min_so_far;
}

int
find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time)
{
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int ret;
    uint64_t recno;
    uint16_t hour;
    uint8_t temp;
    uint8_t max_so_far = 0;

    /* Open cursors needed by the join. */
    error_check(session->open_cursor(
      session, "join:table:weathertable(hour,temp)", NULL, NULL, &join_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &start_time_cursor));
    error_check(
      session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &end_time_cursor));

    /* select values WHERE (hour >= start AND hour <= end) */
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search(start_time_cursor));
    error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge,count=10"));

    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search(end_time_cursor));
    error_check(session->join(session, join_cursor, end_time_cursor, "compare=le,count=10"));

    /* 
    Initialize maximum temperature to temperature of the first record. 
    Assumes at least one record is found in weather records.
    */
    join_cursor->next(join_cursor);
    error_check(join_cursor->get_key(join_cursor, &recno));
    error_check(join_cursor->get_value(join_cursor, &hour, &temp));
    max_so_far = temp;

    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp > max_so_far) {
            max_so_far = temp;
        }

        /* For debugging purposes */
        /*
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
        */
    }

    return max_so_far;
}

int
average_data(WT_SESSION *session)
{
    WT_CURSOR *loc_cursor;
    unsigned int count = 0;
    uint64_t recno;
    const char *country;
    unsigned int ret_arr[5] = {0, 0, 0, 0, 0};
    int ret;
    uint16_t hour, pressure;
    uint8_t temp, humidity, wind, feels_like_temp, loc_lat;
    uint16_t start, end, loc_long;

    /* Create an index to search for country*/
    error_check(session->create(session, "index:weathertable:country", "columns=(country)"));

    /* Open a cursor to search for the location */
    error_check(
      session->open_cursor(session, "index:weathertable:country", NULL, NULL, &loc_cursor));
    loc_cursor->set_key(loc_cursor, "AU\0\0\0");
    error_check(loc_cursor->search(loc_cursor));

    /* Populate the array with the totals of each of the columns*/
    while ((ret = loc_cursor->next(loc_cursor)) == 0) {
        error_check(loc_cursor->get_key(loc_cursor, &recno));
        error_check(loc_cursor->get_value(loc_cursor, &temp, &humidity, &pressure, &wind,
          &feels_like_temp, &loc_lat, &loc_long, &country));
        count++;
        ret_arr[0] += temp;
        ret_arr[1] += humidity;
        ret_arr[2] += pressure;
        ret_arr[3] += wind;
        ret_arr[4] += feels_like_temp;
        printf("%d", ret_arr[0]);
    }

    /* Get the average values by dividing with the total number of records*/
    for (int i = 0; i < 5; i++) {
        ret_arr[i] = ret_arr[i] / count;
    }

    // printf("Average records for location: \n Longitude: %" PRIu16 ", Latitude: %" PRIu16 ",
    // Country: %s", search_loc.longitude, search_loc.latitude, search_loc.country);
    /* List the average records */
    for (int i = 0; i < 5; i++) {
        printf("Average data : temp: %" PRIu8 ", humidity: %" PRIu8 ", pressure: %" PRIu16
               ", wind: %" PRIu8 ", feels like: %" PRIu8,
          ret_arr[0], ret_arr[1], ret_arr[2], ret_arr[3], ret_arr[4]);
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(loc_cursor->close(loc_cursor));

    return 0;
}

int
main(int argc, char *argv[])
{
    WEATHER *w;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    WEATHER weather_data[N_DATA];
    uint16_t start, end;

    generate_data(weather_data);

    home = example_setup(argc, argv);

    /* Establising a connection */
    error_check(wiredtiger_open(home, NULL, "create,statistics=(fast)", &conn));

    /* Establising a session */
    error_check(conn->open_session(conn, NULL, NULL, &session));

    /* create a table, with coloumns and colgroup */
    error_check(session->create(session, "table:weathertable",
      "key_format=r,value_format=5sHBBHBBHH5s,columns=(id,day,hour,temp,humidity,pressure,wind,"
      "feels_like_temp,loc_lat,loc_long,country),colgroups=(day_time,temperature,humidity_pressure,"
      "wind,feels_like_temp,location)"));

    /* create the colgroups */
    error_check(session->create(session, "colgroup:weathertable:day_time", "columns=(day,hour)"));
    error_check(session->create(session, "colgroup:weathertable:temperature", "columns=(temp)"));
    error_check(session->create(
      session, "colgroup:weathertable:humidity_pressure", "columns=(humidity,pressure)"));
    error_check(session->create(session, "colgroup:weathertable:wind", "columns=(wind)"));
    error_check(session->create(
      session, "colgroup:weathertable:feels_like_temp", "columns=(feels_like_temp)"));
    error_check(session->create(
      session, "colgroup:weathertable:location", "columns=(loc_lat,loc_long,country)"));

    /* open a cursor on the table to insert the data ---[[[[ INSERT ]]]]--- */
    error_check(session->open_cursor(session, "table:weathertable", NULL, "append", &cursor));
    w = weather_data;
    for (int i = 0; i < N_DATA; i++) {
        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure, w->wind,
          w->feels_like_temp, w->loc_lat, w->loc_long, w->country);
        error_check(cursor->insert(cursor));
        w++;
    }

    /* close cursor */
    error_check(cursor->close(cursor));

    /* prints all the data in the database. */
    print_all_columns(session);

    /* Update the temperature from celcius to fahrenheit */
    print_temp_column(session);
    update_celcius_to_farenheit(session);
    print_temp_column(session);

    start = 1000;
    end = 2000;

    error_check(session->create(session, "index:weathertable:hour", "columns=(hour)"));

    printf("The minimum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", start, end,
      find_min_temp(session, start, end));
    printf("The maximum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", start, end,
      find_max_temp(session, start, end));

    chance_of_rain(session);
    remove_country(session);
    print_all_columns(session);
    average_data(session);

    // close the connection
    error_check(conn->close(conn, NULL));
    return (EXIT_SUCCESS);
}