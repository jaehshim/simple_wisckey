#pragma once
#include <assert.h>
#include <vector>
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
#include <unordered_map>
#include <thread>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"

#define DELIMITER "$$"
#define GC_DELIMITER "##"
#define DELI_LENGTH 2

#define TOTAL_SIZE 1048576 * 1024
#define KEY_SIZE 16
#define VALUE_SIZE 1024 - KEY_SIZE

#define FILE_SIZE 734003200
//#define FILE_SIZE 104857600*5 // 500MB
//#define FILE_SIZE 1610612736 // 1.5GB

#define SELECTIVE_THRESHOLD 18
#define SELECRIVE "@"

#define GC_DEMAND 0
#define GC_DEFAULT_READ_SIZE 1024
#define GC_INCR 128
#define GC_CHUNK_SIZE 1024*3

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
using std::thread;

typedef struct WiscKey
{
    string dir;
    DB *leveldb;
    string logfile;
    long long head, tail;
    fstream logStream;

} WK;

typedef struct WiscKeyHead {
    WK * wk1;
    WK * wk2;
} WK_HEAD;

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

static WK_HEAD *open_wisckey(const string &dirname)
{
    string dir;
    DB *leveldb;

    WK_HEAD * wk_head = new WK_HEAD;
    wk_head->wk1 = new WK;
    wk_head->wk2 = new WK;

    leveldb = open_leveldb(dirname);
    dir = dirname;

    wk_head->wk1->dir = dir;
    wk_head->wk1->leveldb = leveldb;
    wk_head->wk1->logfile = "logfile1";
    ofstream createFile;
    createFile.open(wk_head->wk1->logfile, fstream::trunc);
    if (createFile.fail())
        cout << "truncate file 1 failed"<< endl;
    createFile.close();

    wk_head->wk1->head = 0;
    wk_head->wk1->tail = 0;

    wk_head->wk1->logStream.open(wk_head->wk1->logfile, fstream::out | fstream::in);

    wk_head->wk2->dir = dir;
    wk_head->wk2->leveldb = leveldb;
    wk_head->wk2->logfile = "logfile2";
    createFile.open(wk_head->wk2->logfile, fstream::trunc);
    if (createFile.fail())
        cout << "truncate file 2 failed"<< endl;
    createFile.close();

    wk_head->wk2->head = 0;
    wk_head->wk2->tail = 0;

    wk_head->wk2->logStream.open(wk_head->wk2->logfile, fstream::out | fstream::in);

    return wk_head;
}

static void close_wisckey(WK *wk)
{
    delete wk->leveldb;
    delete wk;
}

void wisc_put(WK_HEAD *wk, string &key, string &value);
bool wisc_get(WK_HEAD *wk, string &key, string &value);

void vlog_read(WK *wk, long long offset, long long value_size, string &data);
void vlog_write(WK *wk, long long size, char *ch);

int gc_check(WK *wk, int );
int gc_proc(WK *wk);
int vlog_parser(WK *wk, int &bias, int &length, char *key_buff, char *temp_buff);
int valid_check(WK *wk, string &key, long long &offset);

void startTimer();
void stopTimer(const char *label);
