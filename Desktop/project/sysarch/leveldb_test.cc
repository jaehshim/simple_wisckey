#include "wisckey_test.h"

int main(int argc, char ** argv)
{
  if (argc < 2) {
    cout << "Usage: " << argv[0] << " <value-size>" << endl;
    exit(0);
  }
  // value size is provided in bytes
  const size_t value_size = std::stoull(argv[1], NULL, 10);
  if (value_size < 1 || value_size > 100000) {
    cout << "  <value-size> must be positive and less then 100000" << endl;
    exit(0);
  }

  DB * db = open_leveldb("leveldb_test_dir");
  if (db == NULL) {
    cerr << "Open LevelDB failed!" << endl;
    exit(1);
  }
  char * vbuf = new char[value_size];
  for (size_t i = 0; i < value_size; i++) {
    vbuf[i] = rand();
  }

  string value = string(vbuf, value_size);
  // cout << vbuf << endl;
  // cout << value_size << endl;
  // cout << value.length() << endl;

  size_t nfill = 536870912/ (value_size + 16); //16 means key-size 2147483648 means 2GB
  size_t nread = nfill;
  // size_t nfill = 10;
  int entries_per_batch_ = 1;
  char seq_tmp1 = argv[2][0]; //sequential or random write
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
      batch.Put(key, value);
    }
    wopt.sync = false;
    s = db->Write(wopt, &batch);
    assert(s.ok());

  //   if (i >= p1) {
  //     clock_t dt = clock() - t0;
  //     cout << "progress: " << i+1 << "/" << nfill << " time elapsed: " << dt * 1.0e-6 << endl << std::flush;
  //     p1 += (nfill / 40);
  //   }
  }
  clock_t dt1 = clock() - t0;
  cout << "Write time elapsed: " << dt1 * 1.0e-6 << " seconds" << endl;
  
  // if(seq1==1){
  //   clock_t t1 = clock();
  //   string return_value;
  //   size_t found = 0;
  //   for(i = 0; i < nread; i++){
  //       string key = std::to_string(i);
  //       if(leveldb_get(db, key, return_value))
  //         found++;
  //   }
  //   clock_t dt2 = clock() - t1;
  //   // cout << "Found number:" << found << endl;
  //   cout << "Read Seqence time elapsed: " << dt2 * 1.0e-6 << " seconds" << endl;

  //   clock_t t2 = clock();
  //   found = 0;
  //   for(i = 0; i < nread; i++){
  //     string key = std::to_string(((size_t)rand())*((size_t)rand()) % nread);
  //     if(leveldb_get(db, key, return_value))
  //       found++;
  //   }
  //   clock_t dt3 = clock() - t2;
  //   // cout << "Found number:" << found << endl;
  //   cout << "Read Random time elapsed: " << dt3 * 1.0e-6 << " seconds" << endl;


  //   clock_t t3 = clock();
  //   leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());    
  //   for (it->SeekToFirst(); it->Valid(); it->Next()){
  //     // cout << it->key().ToString() << ": " << it->value().ToString() << endl;
  //   }
  //   clock_t dt4 = clock() - t3;
  //   cout << "Range Query time elapsed: " << dt4 * 1.0e-6 << " seconds" << endl;

  //   assert(it->status().ok());
  // }

  delete db;
  destroy_leveldb("leveldb_test_dir");
  exit(0);
}

