#include <iostream>
#include <sstream>
#include <string>

#include "./wisc.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
using namespace std;

int main(int argc, char** argv)
{
    // Set up database connection information and open database
    leveldb::DB* db;
    //leveldb::Options options;
    //options.create_if_missing = true;
    
    db = open_leveldb("./testdb");
    //leveldb::Status status = leveldb::DB::Open(options, "./sampledb", &db);

    // if (false == status.ok())
    // {
    //     cerr << "Unable to open/create test database './sampledb'" << endl;
    //     cerr << status.ToString() << endl;
    //     return -1;
    // }
    
    // Add 256 values to the database
    //leveldb::WriteOptions writeOptions;
    for (unsigned int i = 0; i < 256; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;
        
        ostringstream valueStream;
        valueStream << "Test data value: " << i;
        
        lsmt_put(db, keyStream.str(), valueStream.str());
    }
    
    for (unsigned int i = 0; i < 256; ++i)
    {
        ostringstream keyStream;
        keyStream << "Key" << i;
        
        ostringstream valueStream;
        valueStream << "Test data value: " << i;

        string read_value;
        
        lsmt_get(db, keyStream.str(), read_value);

        cout << valueStream.str() << read_value << endl;
        if (valueStream.str() != read_value)
        {
            cout << "ERROR##########################" << endl;
        }

    }

    // Iterate over each item in the database and print them
    // leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    // for (it->SeekToFirst(); it->Valid(); it->Next())
    // {
    //     cout << it->key().ToString() << " : " << it->value().ToString() << endl;
    // }
    
    // if (false == it->status().ok())
    // {
    //     cerr << "An error was found during the scan" << endl;
    //     cerr << it->status().ToString() << endl; 
    // }
    
    // delete it;

    // // Close the database
    // delete db;
    destroy_leveldb("./testdb");
}

