#include "wisc.h"

int main(int argc, char **argv)
{
    struct timeval start_point, end_point;
    double operating_time;

    WK *wk = open_wisckey("wisckeyDB");
    if (wk == NULL)
    {
        cerr << "open failed!" << endl;
        exit(1);
    }

    gettimeofday(&start_point, NULL);
    for (unsigned int i = 0; i < 1024*1024; ++i)
    {
        ostringstream keyStream;
        char ch;
        for (int j = 0; j<KEY_SIZE; j++) {
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
        wisc_put(wk, keystr, valuestr);
    }
    gettimeofday(&end_point, NULL);

    operating_time = end_point.tv_sec - start_point.tv_sec;
    printf("WiscKey load time %lf s\n", operating_time);


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

    close_wisckey(wk);
//    destroy_leveldb("wisckey_test_dir");
//    remove("logfile");
    
    return 0;
}
