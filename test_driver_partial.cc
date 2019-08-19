#include "wisc.h"

int main(int argc, char **argv)
{
    destroy_leveldb("wisckey_test_dir");
    remove("logfile");
    remove("coldfile");
    WK *wk = open_wisckey("wisckey_test_dir");
    if (wk == NULL)
    {
        cerr << "open failed!" << endl;
        exit(1);
    }

    for (unsigned int i = 0; i < 1000; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;

        ostringstream valueStream;
        valueStream << "Test data value: " << i;
        string keystr = keyStream.str();
        string valuestr = valueStream.str();
        wisc_put(wk, keystr, valuestr);
  
    }

    for(int j=0; j<100; j++)
    {
        for (unsigned int i = 0; i < 100; ++i)
        {
            ostringstream keyStream;
            keyStream << "Key" << i;

            ostringstream valueStream;
            valueStream << "Test data value: " << i;
            string keystr = keyStream.str();
            string valuestr = valueStream.str();
            wisc_put(wk, keystr, valuestr);
    
            ostringstream keyStream1;
            keyStream1 << "Key" << i;

            ostringstream valueStream1;
            valueStream1 << "Test data value: " << i;

            string read_value1;
            string keystr1 = keyStream1.str();

            wisc_get(wk, keystr1, read_value1);

            cout << valueStream1.str() << read_value1 << endl;
            if (valueStream1.str() != read_value1)
            {
                cout << "ERROR##########################" << endl;
                exit(1);
            }
        }
    }

    cout << "---------------------------------------------last iter start" << endl;

    for (unsigned int i = 0; i < 1000; ++i)
    {
        //cout << "driver" << endl;
        ostringstream keyStream1;
        keyStream1 << "Key" << i;

        ostringstream valueStream1;
        valueStream1 << "Test data value: " << i;

        string read_value1;
        string keystr1 = keyStream1.str();

        wisc_get(wk, keystr1, read_value1);
    
        cout << valueStream1.str() << read_value1 << endl;
   
        if (valueStream1.str() != read_value1)
        {
            cout << "ERROR##########################" << endl;
            exit(1);
        }
    }


    close_wisckey(wk);

    cout << "Finish Driver" << endl;
    
    return 0;
}