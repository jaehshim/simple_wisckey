#include "wisc.h"

int main(int argc, char **argv)
{
    struct timeval start_point, end_point;
    double operating_time;
    DB * db;
    WK * wk;
    int run_count = TOTAL_SIZE / (KEY_SIZE + VALUE_SIZE);
    int wisckey = atoi(argv[1]);
    float percent = atoi(argv[2]);

    if (wisckey)
    {
        destroy_leveldb("wisckeyDB");
        remove("logfile");
        wk = open_wisckey("wisckeyDB");
        if (wk == NULL)
        {
            cerr << "open failed!" << endl;
            exit(1);
        }
    }
    else {
        destroy_leveldb("testdb");
        db = open_leveldb("testdb");
    }

    cout << "run count : " << run_count << endl;
    for (unsigned int i = 0; i < run_count; ++i)
    {
        ostringstream keyStream;
        char ch;
        string str = to_string(i);

        keyStream << str;

        for (int j = 0; j < KEY_SIZE - str.length(); j++)
            keyStream << 'A';

        ostringstream valueStream;
        valueStream << str;
        for (int j = 0; j < VALUE_SIZE - str.length(); j++)
        {
            valueStream << 'B';
        }

        string keystr = keyStream.str();
        string valuestr = valueStream.str();

        if (wisckey)
            wisc_put(wk, keystr, valuestr);
        else
            lsmt_put(db, keystr, valuestr);
    }

    /* update phase */
    for (int j = 0; j < 100/percent; j++)
    {
        for (unsigned int i = 0; i < run_count * (percent/100); ++i)
        {
            ostringstream keyStream;
            char ch;
            string str = to_string(i);

            keyStream << str;

            for (int j = 0; j < KEY_SIZE - str.length(); j++)
                keyStream << 'A';

            ostringstream valueStream;
            valueStream << str;
            for (int j = 0; j < VALUE_SIZE - str.length(); j++)
            {
                valueStream << 'B';
            }

            string keystr = keyStream.str();
            string valuestr = valueStream.str();

            if (wisckey)
                wisc_put(wk, keystr, valuestr);
            else
                lsmt_put(db, keystr, valuestr);
        }
    }

    if (wisckey)
        close_wisckey(wk);

    
    return 0;
}
