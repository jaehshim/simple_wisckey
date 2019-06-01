#include "wisc.h"

int main(int argc, char **argv)
{
    WK *wk = open_wisckey("wisckey_test_dir");
    if (wk == NULL)
    {
        cerr << "open failed!" << endl;
        exit(1);
    }

    for (unsigned int i = 0; i < 5; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;

        ostringstream valueStream;
        valueStream << "Test data value: " << i;
        string keystr = keyStream.str();
        string valuestr = valueStream.str();
        wisc_put(wk, keystr, valuestr);
    }

    for (unsigned int i = 0; i < 5; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;

        ostringstream valueStream;
        valueStream << "Test data value: " << i;

        string read_value;
        string keystr = keyStream.str();

        wisc_get(wk, keystr, read_value);

        cout << valueStream.str() << read_value << endl;
        if (valueStream.str() != read_value)
        {
            cout << "ERROR##########################" << endl;
        }
    }

    close_wisckey(wk);
    destroy_leveldb("wisckey_test_dir");
    //remove("logfile");
    exit(0);
}
