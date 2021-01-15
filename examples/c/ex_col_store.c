#include <test_util.h>

static const char *home;

typedef struct {
    uint16_t longitude; 
    uint16_t latitude; 
    char country[5];
} LOCATION;

typedef struct {
    char day[5]; 
    uint16_t hour;
    uint8_t temp; 
    uint8_t humidity;
    uint16_t pressure; 
    uint8_t wind;
    uint8_t feels_like_temp; 
    LOCATION location;
} WEATHER;

static WEATHER weather_data[] = {                               
                                { "WED", 600, 20, 60, 1000, 20, 21, { 36, 174, "NZ" }},
                                { "WED", 1200, 25, 65, 1001, 23, 21, { 36, 174, "NZ" }},
                                { "WED", 1800, 18, 53, 1002, 18, 21, { 36, 174, "NZ" }},
                                { "WED", 2400, 15, 50, 1002, 15, 21, { 36, 174, "NZ" }},
                                { "FRI", 1700, 18, 15, 1050, 10, 16, {34, 151, "AU"}},
                                { "SAT", 1000, 5, 30, 1040, 45, 3, {36, 140, "JPN"}},
                                { "MON", 1300, 10, 50, 1050, 65, 9, {37, 96, "US"}},
                                { "TUE", 2000, 10, 20, 1030, 15, 12, {38, 127, "KOR"}},
                                { "THU", 1100, 5, 28, 1050, 40, 7, {36, 140, "JPN"}},
                                { "TUE", 1540, 25, 80, 800, 15, 21, {3, 31, "AU"} }, 
                                { "FRI", 2110, 16, 40, 600, 30, 14, {36, 311, "CAN"}}, 
                                { "MON", 625, 30, 100, 1000, 5, 32, {42,3, "USA"}},
                                { "THU", 2231, 19, 100, 1100, 12, 20, {3,31, "AU"}},  
                                { "WED", 2215, 22, 120, 950, 21, 34,{50,150, "NZ"}}, 
                                { "", 0, 0, 0, 0, 0, 0, {0,0,""}}                                
                                };  


// int celcius_to_farenheit(int temp_in_celcius) {
//     return (1.8 * temp_in_celcius) + 32.0;
// }

// void update_celcius_to_farenheit(WT_SESSION* session) {
//     WT_CURSOR *cursor;
//     int ret;
//     uint8_t temp;

//     error_check(session->open_cursor(session, "colgroup:weathertable:temprature", NULL, NULL, &cursor));

//     while((ret = cursor->next(cursor) == 0)) {
//         error_check(cursor->get_value(cursor, &temp));
//         cursor->set_value(cursor, celcius_to_farenheit(temp));
//         error_check(cursor->update(cursor));
//     }

//     scan_end_check(ret == WT_NOTFOUND);
//     error_check(cursor->close(cursor)); 
// } 

int
find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time);

int
find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time);

int
find_min_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time) {
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int ret;
    uint64_t recno;
    uint16_t hour;
    uint8_t temp;

    // Assumes temperatures do not exceed 100.
    int min_so_far = 1000;

    /* Open cursors needed by the join. */
    error_check(
        session->open_cursor(session, "join:table:weathertable(hour,temp)", NULL, NULL, &join_cursor));
    error_check(
        session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &start_time_cursor));
    error_check(
        session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &end_time_cursor));

    /* select values WHERE (hour >= start AND hour <= end) AND country=NZ */
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search(start_time_cursor));
    error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge,count=10"));

    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search(end_time_cursor));
    error_check(session->join(session, join_cursor, end_time_cursor, "compare=le,count=10"));

    /* List records that match criteria */
    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp < min_so_far) {
            min_so_far = temp;
        }

        /* For debugging purposes */
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
    }

    return min_so_far;
}

int
find_max_temp(WT_SESSION *session, uint16_t start_time, uint16_t end_time) {
    WT_CURSOR *join_cursor, *start_time_cursor, *end_time_cursor;
    int ret;
    uint64_t recno;
    uint16_t hour;
    uint8_t temp;
    uint8_t max_so_far = 0;

    /* Open cursors needed by the join. */
    error_check(
        session->open_cursor(session, "join:table:weathertable(hour,temp)", NULL, NULL, &join_cursor));
    error_check(
        session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &start_time_cursor));
    error_check(
        session->open_cursor(session, "index:weathertable:hour", NULL, NULL, &end_time_cursor));

    /* select values WHERE (hour >= start AND hour <= end) AND country=NZ */
    start_time_cursor->set_key(start_time_cursor, start_time);
    error_check(start_time_cursor->search(start_time_cursor));
    error_check(session->join(session, join_cursor, start_time_cursor, "compare=ge,count=10"));

    end_time_cursor->set_key(end_time_cursor, end_time);
    error_check(end_time_cursor->search(end_time_cursor));
    error_check(session->join(session, join_cursor, end_time_cursor, "compare=le,count=10"));
    
    /* Add location filter LATER */
    /*country_cursor->set_key(country_cursor, "NZ\0\0\0");
    error_check(country_cursor->search(country_cursor));
    error_check(
        session->join(session, join_cursor, country_cursor, "compare=eq,count=10,strategy=bloom"));
    */

    /* List records that match criteria */
    while ((ret = join_cursor->next(join_cursor)) == 0) {
        error_check(join_cursor->get_key(join_cursor, &recno));
        error_check(join_cursor->get_value(join_cursor, &hour, &temp));

        if (temp > max_so_far) {
            max_so_far = temp;
        }

        /* For debugging purposes */
        printf("ID %" PRIu64, recno);
        printf(": hour %" PRIu16 " temp: %" PRIu8 "\n", hour, temp);
    }

    return max_so_far;
}

int
main(int argc, char* argv[]) 
{
    WEATHER* w;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;

    const char* day; 
    uint16_t hour, pressure;
    uint8_t temp, humidity, wind, feels_like_temp; 
    uint16_t start, end;
    LOCATION *location;

    home = example_setup(argc, argv);

    /* Establising a connection */
    error_check(wiredtiger_open(home, NULL, "create,statistics=(fast)", &conn));

    /* Establising a session */
    error_check(conn->open_session(conn, NULL,NULL,&session));

    /* create a table, with coloumns and colgroup */ 
    /*===Problem: placeholders if we use double, and not sure with the Location as a struct to use 'u'===*/
    error_check(session->create(session, "table:weathertable",
    "key_format=r,value_format=5sHBBHBBu,columns=(id,day,hour,temp,humidity,pressure,wind,feels_like_temp,location),colgroups=(day_time,temperature,humidity_pressure,"
    "wind,feels_like_temp,location)"));
    

    /* create the colgroups */
    error_check(session->create(session, "colgroup:weathertable:day_time", "columns=(day,hour)"));
    error_check(session->create(session, "colgroup:weathertable:temperature", "columns=(temp)"));
    error_check(session->create(session, "colgroup:weathertable:humidity_pressure", "columns=(humidity,pressure)"));
    error_check(session->create(session, "colgroup:weathertable:wind", "columns=(wind)"));
    error_check(session->create(session, "colgroup:weathertable:feels_like_temp", "columns=(feels_like_temp)"));
    error_check(session->create(session, "colgroup:weathertable:location", "columns=(location)"));


    /* open a cursor on the table to insert the data ---[[[[ INSERT ]]]]--- */  
    error_check(session->open_cursor(session, "table:weathertable", NULL, "append", &cursor));
    for (w = weather_data; w->pressure != 0; w++) {
        WT_ITEM loc;
        loc.data = &w->location;
        loc.size = sizeof(w->location);

        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure,w->wind,w->feels_like_temp, &loc);
        error_check(cursor->insert(cursor));
    }
    /* close cursor */
    error_check(cursor->close(cursor));    
    
    error_check(session->open_cursor(session, "table:weathertable", NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &day, &hour, &temp, &humidity, &pressure, &wind, &feels_like_temp, &location));
        printf("{\n"
               "    ID: %" PRIu64 "\n"
               "    day: %s\n"
               "    hour: %" PRIu16 "\n"
               "    temp: %" PRIu8 "\n"
               "    humidity: %" PRIu8 "\n"
               "    pressure: %" PRIu16 "\n"
               "    wind: %" PRIu8  "\n"
               "    feels like: %" PRIu8 "\n" 
               "    location:  {\n"
               "                  lat: %" PRIu16 "\n"
               "                  long: %" PRIu16 "\n"
               "                  country: %s\n"
               "                }\n"
               "}\n\n",recno, day, hour,temp, humidity, pressure, wind, feels_like_temp, 
               location->latitude, location-> longitude, location->country);
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));

    /* averageLocationData() ASSIGNED TO:  CLARISSE */

    start = 1000;
    end = 2000;

    error_check(session->create(session, "index:weathertable:hour", "columns=(hour)"));

    printf("The minimum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", 
            start, end, find_min_temp(session, start, end));
    printf("The maximum temperature between %" PRIu16 " and %" PRIu16 " is %d.\n", 
            start, end, find_max_temp(session, start, end));

    /* C to F ASSIGNED TO:  SID */
    /* difficult one (Probability of rain) ASSIGNED TO:  Sean*/

    return (EXIT_SUCCESS);
}