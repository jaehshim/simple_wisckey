#include "wisc.h"

WK_HEAD *wk_head;

void run_gc() {
    int i;

    while (1) {
        i++;
        if (i%2)
            gc_check(wk_head->wk1, 1);
        else
            gc_check(wk_head->wk2, 1);
    }
}

int main(int argc, char **argv)
{
    struct timeval start_point, end_point;
    double operating_time;
    DB * db;

    int wisckey = atoi(argv[1]);

    if (wisckey)
    {
        destroy_leveldb("wisckeyDB");
        remove("logfile1");
        remove("logfile2");
        wk_head = open_wisckey("wisckeyDB");
        if (wk_head == NULL)
        {
            cerr << "open failed!" << endl;
            exit(1);
        }
    }
    else {
        destroy_leveldb("testdb");
        db = open_leveldb("testdb");
    }

    thread t1(run_gc);
    t1.detach();

    cout << "run count : " << TOTAL_SIZE / (KEY_SIZE + VALUE_SIZE) << endl;
    startTimer();
    for (unsigned int i = 0; i < TOTAL_SIZE / (KEY_SIZE+VALUE_SIZE); ++i)
    {
        ostringstream keyStream;
        char ch;
        
        for (int j = 0; j<KEY_SIZE-3; j++) 
            keyStream << 'A';

        for (int j = 0; j<3; j++) {
            ch = rand()%26 + 'A';
            keyStream << ch;
        }

        ostringstream valueStream;
        for (int j = 0; j<VALUE_SIZE; j++) {
            ch = rand()%26 + 'A';
            valueStream << ch;
        }

        string keystr = keyStream.str();
        string valuestr = valueStream.str();

        if (wisckey)
            wisc_put(wk_head, keystr, valuestr);
        else
            lsmt_put(db, keystr, valuestr);
    }
    stopTimer("WiscKey load time");

    operating_time = end_point.tv_sec - start_point.tv_sec;

    // for (unsigned int i = 0; i < 256; ++i)
    // {
    //     ostringstream keyStream;

    //     ostringstream valueStream;
    //     valueStream << "Test data value: " << i;

    //     string read_value;
    //     string keystr = keyStream.str();

    //     wisc_get(wk, keystr, read_value);

    //     cout << valueStream.str() << read_value << endl;
    //     if (valueStream.str() != read_value)
    //     {
    //         cout << "ERROR##########################" << endl;
    //     }
    // }
    
    return 0;
}
