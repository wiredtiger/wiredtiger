#include <test_util.h>

static const char *home;

typedef struct {
    uint16_t longitude; /* change to double */
    uint16_t latitude; /* change to double */
    char country[5];
} LOCATION;

typedef struct {
    char day[5]; 
    uint16_t hour;
    uint8_t temp; /* change to double */
    uint8_t humidity;
    uint16_t pressure; /* change to double */
    uint8_t wind;
    uint8_t feels_like_temp; /* change to double */
    LOCATION location;
} WEATHER;

static WEATHER weather_data[] = {                               
                                { "WED", 123, 20, 60, 1000, 20, 21, { 36, 174, "NZ" }},
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
                                { "MON", 0625, 30, 100, 1000, 5, 32, {42,3, "USA"}},
                                { "THU", 2231, 19, 100, 1100, 12, 20, {3,31, "AU"}},  
                                { "WED", 2215, 22, 120, 950, 21, 34,{50,150, "NZ"}}, 
                                { "", 0, 0, 0, 0, 0, 0, {0,0,""}}                                
                                };  

int
main(int argc, char* argv[]) 
{
    WEATHER* w;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;

    char day[5]; 
    uint16_t hour;
    uint8_t temp; /* change to double */
    uint8_t humidity;
    uint16_t pressure; /* change to double */
    uint8_t wind;
    uint8_t feels_like_temp; /* change to double */
    LOCATION *location;

    home = example_setup(argc, argv);

    /* Establising a connection */
    error_check(wiredtiger_open(home, NULL, "create,statistics=(fast)", &conn));

    /* Establising a session */
    error_check(conn->open_session(conn, NULL,NULL,&session));

    /* create a table, with coloumns and colgroup */ 
    /*===Problem: placeholders if we use double, and not sure with the Location as a struct to use 'u'===*/
    error_check(session->create(session, "table:weathertable",
    "key_format=r,value_format=3sBBBBBBu,columns=(id,day,hour,temp,humidity,pressure,wind,feels_like_temp,location),colgroups=(day_time,temperature,humidity_pressure,"
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
        // printf("lat: %" PRIu16 "\nlong: %" PRIu32 "\n", w->location.latitude, w->location.longitude);
        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure,w->wind,w->feels_like_temp, &loc);
        error_check(cursor->insert(cursor));

        printf("Interting...\n");
        printf(
            "day: %s : hour: %" PRIu16 ", temp: %" PRIu8 ", humidity: %" PRIu8 ", pressure: %" PRIu16 ", wind: %" PRIu8 ", feels like: %" PRIu8 "\n"
            , w->day, w->hour, w->temp, w->humidity, w->pressure, w->wind, w->feels_like_temp
        );
    }
    /* close cursor */
    error_check(cursor->close(cursor));    
    
    error_check(session->open_cursor(session, "table:weathertable", NULL, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &day, &hour, &temp, &humidity, &pressure, &wind, &feels_like_temp, &location));
        printf("Reading\n");
        printf("ID %" PRIu64, recno);
        printf(
            "day: %s : hour: %" PRIu16 ", temp: %" PRIu8 ", humidity: %" PRIu8 ", pressure: %" PRIu16 ", wind: %" PRIu8 ", feels like: %" PRIu8 
            ", location: lat: %" PRIu16 " long: %" PRIu16 " %s\n", day, hour,
            temp, humidity, pressure, wind, feels_like_temp, location->latitude, location-> longitude, location->country
        );
    }
    scan_end_check(ret == WT_NOTFOUND);
    error_check(cursor->close(cursor));

    /* averageLocationData() ASSIGNED TO:  CLARISSE */
    /* MaxMin()  ASSIGNED TO:  MONICA */
    /* C to F ASSIGNED TO:  SID */
    /* difficult one (Probability of rain) ASSIGNED TO:  Sean*/

    return (EXIT_SUCCESS);
}