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
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"

#define delimiter "$$"
#define deli_length 2

using namespace std;

using std::string;
using std::vector;
using std::cin;
using std::cout;
using std::cerr;
using std::endl;
using std::stringstream;
using std::fstream;
using leveldb::ReadOptions;
using leveldb::Options;
using leveldb::Status;
using leveldb::WriteBatch;
using leveldb::WriteOptions;
using leveldb::DB;
using leveldb::Iterator;

typedef struct WiscKey {
  string dir;
  DB * leveldb;
  string logfile;
} WK;

static bool lsmt_get(DB * db, string &key, string &value)
{
  ReadOptions ropt;
  Status s = db->Get(ropt, key, &value);
  return s.ok();
}

static void lsmt_put(DB * db, string &key, string &value)
{
  WriteOptions wopt;
  Status s = db->Put(wopt, key, value);
  assert(s.ok());
}

static void leveldb_del(DB * db, string &key)
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

static DB * open_leveldb(const string &dirname)
{
  Options options;
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.write_buffer_size = 1u << 21;
  destroy_leveldb(dirname);
  DB * db = NULL;
  Status s = DB::Open(options, dirname, &db);
  return db;
}

static void wisckey_del(WK * wk, string &key)
{
	leveldb_del(wk->leveldb,key);

}

static WK * open_wisckey(const string& dirname)
{
	WK * wk = new WK;
	wk->leveldb = open_leveldb(dirname);
    wk->dir = dirname;
	wk->logfile = "logfile";
  return wk;
}

static void close_wisckey(WK * wk)
{
  	delete wk->leveldb;
  	delete wk;
}

void wisc_put(WK *wk, string &key, string &value);
bool wisc_get(WK *wk, string &key, string &value);
