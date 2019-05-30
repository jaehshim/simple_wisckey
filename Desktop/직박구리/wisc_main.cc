#include "wisc.h"
#include <fstream>
#include <algorithm>
#include <vector>
#include <ctime>
#include <cstdlib>

using namespace std;

int main(int argc, char **argv)
{
    WK *wk = open_wisckey("wisckey_test_dir");
    if (wk == NULL)
    {
        cerr << "open failed!" << endl;
        exit(1);
    }

    for (unsigned int i = 0; i < 256; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;

        ostringstream valueStream;
        valueStream << "Test data value: " << i;

        wisc_put(wk, keyStream.str(), valueStream.str());
    }

    for (unsigned int i = 0; i < 256; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;

        ostringstream valueStream;
        valueStream << "Test data value: " << i;

        string read_value;

        wisc_get(wk, keyStream.str(), read_value);

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
