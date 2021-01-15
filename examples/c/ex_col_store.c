#include <test_util.h>

#define N_DATA 100

static const char *home;

typedef struct {
    uint16_t longitude; 
    uint16_t latitude; 
    char country[5];
} LOCATION;

typedef struct weather_data {
    char day[5]; 
    uint16_t hour;
    uint8_t temp; 
    uint8_t humidity;
    uint16_t pressure; 
    uint8_t wind;
    uint8_t feels_like_temp; 
    LOCATION location;
} WEATHER;

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

void chance_of_rain(WT_SESSION* session);
void generate_data(WEATHER *w_array);
void search_temperature(WT_SESSION *session);


void chance_of_rain(WT_SESSION* session){
    WT_CURSOR *cursor;
    int ret;
    uint64_t recno;
    uint8_t humidity;
    uint16_t pressure;

    error_check(session->open_cursor(session, "colgroup:weathertable:humidity_pressure", NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &recno));
        error_check(cursor->get_value(cursor, &humidity, &pressure));
        if (humidity > 70 && pressure < 1000){
            printf("Rain likely\n");
        } else {
            printf("Rain unlikely\n");
        }
    }    
    return;
}

void search_temperature(WT_SESSION *session){
    WT_CURSOR *day_cursor, *join_cursor;
    WT_CURSOR *temp_cursor;
    uint16_t temp;
    char *day;
    // uint64_t recno;

    error_check(session->open_cursor(session, "join:table:weathertable", NULL, NULL, &join_cursor));
    error_check(session->open_cursor(session, "colgroup:weathertable:day_time", NULL, NULL, &day_cursor));
    error_check(session->open_cursor(session, "colgroup:weathertable:temperature", NULL, NULL, &temp_cursor));

    temp_cursor->set_key(temp_cursor, 20);
    error_check(temp_cursor->search(temp_cursor));
    error_check(session->join(session, join_cursor, temp_cursor, "compare=lt"));
    error_check(temp_cursor->get_value(temp_cursor,&temp));
    printf("found day: %s",day);

    // day_cursor->set_key(day_cursor, "MON\0\0");
    // error_check(day_cursor->search(day_cursor));
    // error_check(session->join(session, join_cursor, day_cursor, "compare=eq"));
    // error_check(day_cursor->get_value(day_cursor,&day));
    // printf("found day: %s",day);
    return;
}

void generate_data(WEATHER *w_array){
    // rand() % (max_number + 1 - minimum_number) + minimum_number
    int day;
    int country;
    for (int i = 0; i < N_DATA; i++){
        WEATHER w;
        day = rand() % (6 + 1 - 0) + 0;
        switch(day){
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
        w.hour = 1200;
        w.temp = rand() % (50 + 1 - 0) + 0;
        w.humidity = rand() % (100 + 1 - 0) + 0;
        w.pressure = rand() % (1100 + 1 - 900) + 900;
        w.wind = rand() % (200 + 1 - 0) + 0;
        w.location.latitude = rand() % (180 + 1 - 0) + 0;
        w.location.longitude = rand() % (90 + 1 - 0) + 0;
        country = rand() % (6 + 1 - 0) + 0;
        switch(country){
            case 0:
                strcpy(w.location.country, "AUS");
                break;
            case 1:
                strcpy(w.location.country, "UK");
                break;
            case 2:
                strcpy(w.location.country, "US");
                break;
            case 3:
                strcpy(w.location.country, "NZ");
                break;
            case 4:
                strcpy(w.location.country, "IND");
                break;
            case 5:
                strcpy(w.location.country, "CHI");
                break;
            case 6:
                strcpy(w.location.country, "RUS");
                break;
        }

        w_array[i] = w;
    }
    return;
}

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
    WEATHER weather_data[N_DATA];

    const char* day; 
    uint16_t hour, pressure;
    uint8_t temp, humidity, wind, feels_like_temp; 
    uint16_t start, end;
    LOCATION *location;

    generate_data(weather_data);

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
    w = weather_data;
    for (int i = 0; i < N_DATA; i++) {
        WT_ITEM loc;
        loc.data = &w->location;
        loc.size = sizeof(w->location);

        cursor->set_value(cursor, w->day, w->hour, w->temp, w->humidity, w->pressure,w->wind,w->feels_like_temp, &loc);
        error_check(cursor->insert(cursor));
        w++;
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
    chance_of_rain(session);
    // search_temperature(session);

    return (EXIT_SUCCESS);
}