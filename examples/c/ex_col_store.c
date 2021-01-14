#include <test_util.h>

static const char *home;
/* Jeera ticket */

typedef struct {
    uint8_t longitude; /* change to double */
    uint8_t latitude; /* change to double */
    char country[5];
} LOCATION;

typedef struct {
    char day[3]; 
    uint8_t hour;
    uint8_t temp; /* change to double */
    uint8_t humidity;
    uint8_t pressure; /* change to double */
    uint8_t wind;
    uint8_t feels_like_temp; /* change to double */
    LOCATION location;
} WEATHER;

static WEATHER weather_data[] = {                               
                                { "WED", 0600, 20, 60, 1000, 20, 21, { 36, 174, "NZ" }},
                                { "WED", 1200, 25, 65, 1001, 23, 21, { 36, 174, "NZ" }},
                                { "WED", 1800, 18, 53, 1002, 18, 21, { 36, 174, "NZ" }},
                                { "WED", 2400, 15, 50, 1002, 15, 21, { 36, 174, "NZ" }},
                                { "FRI", 1700, 18, 15, 1050, 10, 16, {34, 151, "AU"}},
                                { "SAT", 1000, 5, 30, 1040, 45, 3, {36, 140, "JPN"}},
                                { "MON", 1300, 10, 50, 1050, 65, 9, {37, 96, "US"}},
                                { "TUE", 2000, 10, 20, 1030, 15, 12, {38, 127, "KOR"}},
                                { "THU", 1100, 5, 28, 1050, 40, 7, {36, 140, "JPN"}}, 
                                { "", 0, 0, 0, 0, 0, 0, {0,0,""}}                                
                                };  

int
main(int argc, char* argv[]) 
{
    WEATHER* w;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;

    home = example_setup(argc, argv);

    /* Open a connection */
    error_check(wiredtiger_open(home, NULL, "create,statistics=(fast)", &conn));

    /* Open a session */
    error_check(conn->open_session(conn, NULL,NULL,&session));

    /* create a table, with coloumns and colgroup */ /*Problem: placeholders if we use double!!!!*/
    error_check(session->create(session, "table:weathertable",
    "key_format=r,value_format=3sBBBBBBu,columns=(id,day,hour,temp,humidity,pressure,wind,feels_like_temp,location),colgroups=(day_time,temprature,humidity_pressure,"
    "wind,feels_like_temp,location)"));
    

    /* create the colgroups */
    error_check(session->create(session, "colgroup:weathertable:day_time", "columns=(day,hour)"));
    error_check(session->create(session, "colgroup:weathertable:temprature", "columns=(temp)"));
    error_check(session->create(session, "colgroup:weathertable:humidity_pressure", "columns=(humidity,pressure)"));
    error_check(session->create(session, "colgroup:weathertable:wind", "columns=(wind)"));
    error_check(session->create(session, "colgroup:weathertable:feels_like_temp", "columns=(feels_like_temp)"));
    error_check(session->create(session, "colgroup:weathertable:location", "columns=(location)"));


    /* open a cursor on the table to insert the data ---[[[[ INSERT ]]]]--- */  
    error_check(session->open_cursor(session, "table:poptable", NULL, "append", &cursor));
    for (w = weather_data; w->pressure != 0; w++) {
        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure,w->wind,w->feels_like_temp, w->location);
        error_check(cursor->insert(cursor));
    }
    /* close cursor */
    error_check(cursor->close(cursor));    

    


    /* open a cursor on the table to update the data ---[[[[ UPDATE ]]]]--- ASSIGNED TO:  MONICA */ 

    /* close cursor */

    /* open the cursor to print the data ---[[[[ LIST ]]]]--- * ASSIGNED TO:  CLARISSE/

    /* operations */

    /* averageLocationData() ASSIGNED TO:  CLARISSE */
    /* MaxMin()  ASSIGNED TO:  MONICA */
    /* C to F ASSIGNED TO:  SID */
    /* difficult one (Probability of rain) ASSIGNED TO:  SHAWN*/

    return (EXIT_SUCCESS);
}