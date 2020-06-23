#include "wisckey_test.h"
#include <fstream>
#include <algorithm> 
#include <vector>      
#include <ctime>       
#include <cstdlib>    

typedef struct WiscKey {
  string dir;
  DB * leveldb;
  FILE * logfile;
} WK;

static bool wisckey_get(WK * wk, string &key, string &value)
{
	string offsetinfo;
  const bool found = leveldb_get(wk->leveldb, key, offsetinfo);
  if (!found)
    return false;
	std::string value_offset;
	std::string value_length;
	std::string s = offsetinfo;
	std::string delimiter = "&&";
	size_t pos = 0;
	std::string token;
	while ((pos = s.find(delimiter)) != std::string::npos) {
    		token = s.substr(0, pos);
		value_offset = token;
    		s.erase(0, pos + delimiter.length());
	}
	value_length = s;
  std::string::size_type sz;
  long offset = std::stol (value_offset,&sz);
	long length = std::stol (value_length,&sz);

  // cout << ftell(wk->logfile) << endl;
	fseek(wk->logfile,offset,SEEK_SET);
  // cout << ftell(wk->logfile) << endl;
  char buffer[length+1];
  fread(buffer,length+1,1,wk->logfile);
  // cout << buffer << endl;
  value = buffer;
	rewind(wk->logfile);
	return true;
}	

static void wisckey_set(WK * wk, string &key, string &value)
{
	long offset = ftell (wk->logfile);
	long size = value.length();
	std::string vlog_offset = std::to_string(offset);
	std::string vlog_size = std::to_string(size);
	std::stringstream vlog_value;
	vlog_value << vlog_offset << "&&" << vlog_size;
	std::string s = vlog_value.str();
  // cout << value.length() << endl;
  // cout << value << endl;
	fwrite(&value, value.length()+1, 1, wk->logfile);
  // fwrite(&key, key.length()+1, 1, wk->logfile);
	leveldb_set(wk->leveldb,key,s);
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
	wk->logfile = fopen("logfile","wb+");
  return wk;
}

static void close_wisckey(WK * wk)
{
	fclose(wk->logfile);
  	delete wk->leveldb;
  	delete wk;
}

static void test_functions(WK * wk)
{
  //test wrtie function
  string key1 = string("123");
  string value1 = string("abcde");
  string key2 = string("54321");
  string value2 = string("abcde");
  wisckey_set(wk, key1, value1);
  wisckey_set(wk, key2, value2);

  // test read function
  string return_value;
  const bool found = wisckey_get(wk, key1, return_value);
  // cout << return_value;
  if (found)
    cout << "Read Function is OK" << endl;
  else
    cout << "Not Found" << endl;

  // test modify function
  string key3 = string("12345");
  string value_modify = string("ABCDE");
  wisckey_set(wk, key3, value_modify);
  const bool found_modify = wisckey_get(wk, key3, return_value);
  cout << return_value << endl;
  // if (return_value == value_modify)
  //   cout << "Modify function is OK" << endl;

  // test delete function
  wisckey_del(wk, key1);
  const bool found_delete = wisckey_get(wk, key1, return_value);
  if (!found_delete)
    cout << "Delete function is OK" <<endl;

  close_wisckey(wk);
  destroy_leveldb("wisckey_test_dir");
  // remove("logfile");
}

int main(int argc, char ** argv)
{
  WK * wk = open_wisckey("wisckey_test_dir");
  if (wk == NULL) 
  {
    cerr << "Open WiscKey failed!" << endl;
    exit(1);
  }

  // test_functions(wk);

  if (argc < 2) {
        cout << "Usage: " << argv[0] << " <value-size>" << endl;
        exit(0);
    }
    const size_t value_size = std::stoull(argv[1], NULL, 10);
    if (value_size < 1 || value_size > 100000) {
        cout << "  <value-size> must be positive and less then 100000" << endl;
      exit(0);
    }

  char * vbuf = new char[value_size];
  for (size_t i = 0; i < value_size; i++) {
    vbuf[i] = rand();
  }

	string value = string(vbuf, value_size);
  // cout << vbuf << endl;
  // cout << value << endl;
  // cout << value_size << endl;
  // cout << value.length() << endl;
	size_t nfill = 536870912 / (value_size + 16); //16 means key-size
  size_t nread = nfill;
  // size_t nfill = 3000;

  int entries_per_batch_ = 1;
  char seq_tmp1 = argv[2][0]; //sequential or random wrtie
  int seq1 = (seq_tmp1 == '0') ? 0 : 1;
  clock_t t0 = clock();
	// size_t p1 = nfill / 40;
	WriteBatch batch;
  Status s;
  WriteOptions wopt;
  size_t j;
  size_t i;

  for (i = 0; i < nfill; i += entries_per_batch_) {
    batch.Clear();
    for (j = 0; j< entries_per_batch_; j++){
      string key = seq1 ? std::to_string(i+j) :std::to_string(((size_t)rand())*((size_t)rand()) % nread);
      long offset = ftell (wk->logfile);
      long size = value.length();
      std::string vlog_offset = std::to_string(offset);
      std::string vlog_size = std::to_string(size);
      std::stringstream vlog_value;
      vlog_value << vlog_offset << "&&" << vlog_size;
      std::string s = vlog_value.str();
      // cout << value.length() << endl;
      fwrite(&value, value.length()+1, 1, wk->logfile);
      fwrite(&key, key.length()+1, 1, wk->logfile);
      
      batch.Put(key, s);
    }
    wopt.sync = false;
    s = wk->leveldb->Write(wopt, &batch);
    assert(s.ok());

    // if (i >= p1) {
    //   clock_t dt = clock() - t0;
    //   cout << "progress: " << i+1 << "/" << nfill << " time elapsed: " << dt * 1.0e-6 << endl << std::flush;
    //   p1 += (nfill / 40);
    // }
  }
  clock_t dt = clock() - t0;
  cout << "Write time elapsed: " << dt * 1.0e-6 << " seconds" << endl;

// if(seq1==1){
//     clock_t t1 = clock();
//     char * return_vbuf = new char[value_size];
//     // cout << value_size << endl;
//     string return_value = string(return_vbuf, value_size);

//     size_t found = 0;
//     for(i = 0; i < nread; i++){
//       string key = std::to_string(i);
//       if(wisckey_get(wk, key, return_value))
//         found++;
//     }
//     clock_t dt2 = clock() - t1;
//     // cout << "Found number:" << found << endl;
//     cout << "Read Sequence time elapsed: " << dt2 * 1.0e-6 << " seconds" << endl;

//     clock_t t2 = clock();

//     found = 0;
//     for(i = 0; i < nread; i++){
//       string key = std::to_string(((size_t)rand())*((size_t)rand()) % nread);
//       if(wisckey_get(wk, key, return_value))
//         found++;
//     }
//     clock_t dt3 = clock() - t2;
//     // cout << "Found number:" << found << endl;
//     cout << "Read Random time elapsed: " << dt3 * 1.0e-6 << " seconds" << endl;

//     clock_t t3 = clock();
//     leveldb::Iterator* it = wk->leveldb->NewIterator(leveldb::ReadOptions());    
//     for (it->SeekToFirst(); it->Valid(); it->Next())
//     {
//       string offsetinfo = it->value().ToString();
//       std::string value_offset;
//       std::string value_length;
//       std::string s = offsetinfo;
//       std::string delimiter = "&&";
//       size_t pos = 0;
//       std::string token;
//       while ((pos = s.find(delimiter)) != std::string::npos) {
//         token = s.substr(0, pos);
//         value_offset = token;
//         s.erase(0, pos + delimiter.length());
//       }
//       value_length = s;
//       std::string::size_type sz;
//       long offset = std::stol (value_offset,&sz);
//       long length = std::stol (value_length,&sz);
//       // cout << ftell(wk->logfile) << endl;
//       fseek(wk->logfile,offset,SEEK_SET);
//       // cout << ftell(wk->logfile) << endl;
//       char buffer[length+1];
//       fread(buffer,length+1,1,wk->logfile);
//       // cout << buffer << endl;
//       value = buffer;
//       rewind(wk->logfile);
//     }
//     clock_t dt4 = clock() - t3;
//     cout << "Range Query time elapsed: " << dt4 * 1.0e-6 << " seconds" << endl;
// }
  close_wisckey(wk);
  destroy_leveldb("wisckey_test_dir");
  remove("logfile");
  exit(0);
}
