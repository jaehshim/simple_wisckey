#pragma once
#include <assert.h>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
#include <ctime>
#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <chrono>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"

#define DELIMITER "$$"
#define GC_DELIMITER "##"
#define COLD_DELIMITER "??"
#define DELI_LENGTH 2

#define TOTAL_SIZE 1048576 * 1024
#define KEY_SIZE 16
#define VALUE_SIZE 1024 - KEY_SIZE

//#define FILE_SIZE 50500
//#define FILE_SIZE 104857600*6 // 500MB
//#define FILE_SIZE 1610612736 // 1.5GB
#define FILE_SIZE 1401316966

#define SELECTIVE_THRESHOLD 100
#define SELECRIVE "@"

#define GC_DEMAND 0
#define GC_DEFAULT_READ_SIZE KEY_SIZE+VALUE_SIZE
#define GC_INCR KEY_SIZE+VALUE_SIZE
#define GC_CHUNK_SIZE FILE_SIZE/10

using namespace std;

using leveldb::DB;
using leveldb::Iterator;
using leveldb::Options;
using leveldb::ReadOptions;
using leveldb::Status;
using leveldb::WriteBatch;
using leveldb::WriteOptions;
using std::cerr;
using std::cin;
using std::cout;
using std::endl;
using std::fstream;
using std::string;
using std::stringstream;
using std::vector;


typedef struct WiscKey
{
    string dir;
    DB *leveldb;
    string logfile;
    string coldfile;
    long long head;
    long long tail;
    long long chead;
    fstream logStream;
    fstream coldStream;

} WK;

static bool lsmt_get(DB *db, string &key, string &value)
{
    ReadOptions ropt;
    Status s = db->Get(ropt, key, &value);
    return s.ok();
}

static void lsmt_put(DB *db, string &key, string &value)
{
    WriteOptions wopt;
    Status s = db->Put(wopt, key, value);
    assert(s.ok());
}

static void leveldb_del(DB *db, string &key)
{
    WriteOptions wopt;
    Status s;
    s = db->Delete(wopt, key);
    assert(s.ok());
}

static void destroy_leveldb(const string &dirname)
{
    Options options;
    leveldb::DestroyDB(dirname, options);
}

static DB *open_leveldb(const string &dirname)
{
    Options options;
    options.create_if_missing = true;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.write_buffer_size = 1u << 21;
    destroy_leveldb(dirname);
    DB *db = NULL;
    Status s = DB::Open(options, dirname, &db);
    return db;
}

static void wisckey_del(WK *wk, string &key)
{
    leveldb_del(wk->leveldb, key);
}

static WK *open_wisckey(const string &dirname)
{
    WK *wk = new WK;
    wk->leveldb = open_leveldb(dirname);
    wk->dir = dirname;
    wk->logfile = "logfile";
    wk->coldfile = "coldfile";

    ofstream createFile;
    createFile.open(wk->logfile, fstream::trunc);
    if (createFile.fail())
        cout << "truncate file failed"<< endl;
    createFile.close();
    
    ofstream createFile2;
    createFile2.open(wk->coldfile, fstream::trunc);
    if (createFile2.fail())
        cout << "truncate file failed"<< endl;
    createFile2.close();

    wk->head = 0;
    wk->tail = 0;
    wk->chead = 0;

    wk->logStream.open(wk->logfile, fstream::out | fstream::in);
    wk->coldStream.open(wk->coldfile, fstream::out | fstream::in);

    return wk;
}

static void close_wisckey(WK *wk)
{
    delete wk->leveldb;
    delete wk;
}

void wisc_put(WK *wk, string &key, string &value);
bool wisc_get(WK *wk, string &key, string &value);

void vlog_read(WK *wk, long long offset, long long value_size, string &data, int gc_mode);
void vlog_write(WK *wk, long long size, char *ch);
void clog_read(WK *wk, long long offset, long long value_size, string &data);
void clog_write(WK *wk, long long size, char *ch);


int gc_check(WK *wk, long long valuesize);
int gc_proc(WK *wk);
int vlog_parser(WK *wk, long long &bias, long long &length, char *key_buff, char *temp_buff);
int valid_check(WK *wk, string &key, long long &offset);

void startTimer();
void stopTimer(const char *label);
