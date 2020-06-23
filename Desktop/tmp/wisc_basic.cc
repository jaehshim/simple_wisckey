#include "wisc.h"
#include <fstream>
#include <algorithm>
#include <vector>
#include <ctime>
#include <cstdlib>

using namespace std;

void wisc_put(WK *wk, string &key, string &value)
{
	ostringstream logStream;
	logStream << key << deli_key_addr << value;
	string logstr = logStream.str();
	fwrite(&logstr, value.length() + 1, 1, wk->logfile); // write value to vlog


	long offset = ftell(wk->logfile);
	long size = value.length();
	string value_addr ;
	value_addr = to_string(offset);
	string value_size ;
	value_size = to_string(size);
	ostringstream lsmStream;
	lsmStream << value_addr << deli_addr_size << value_size ; // lsmt string (value_addr, value_size)
	string lsmstr = lsmStream.str();
	lsmt_put(wk->leveldb, key, lsmstr); // write to lsmt
}

static bool wisc_get(WK *wk, string &key, string &value)
{
	string lsmstr;
	const bool found = lsmt_get(wk->leveldb, key, lsmstr);
	if (!found)
		return false;
	string value_addr;
	string value_size;
	size_t pos = 0;
	string token;
	if((pos = lsmstr.find(deli_addr_size)) != string::npos) // find delimeter
	{
		value_addr = lsmstr.substr(0, pos);
		lsmstr.erase(0, pos + deli_length);
		value_size = lsmstr;
	}
	else
	{
		cout << "lsmt error" << endl ;
		exit(1);	
	}
	
	// cout << ftell(wk->logfile) << endl;
	fseek(wk->logfile, stol(value_addr), SEEK_SET);
	// cout << ftell(wk->logfile) << endl;

	string * buff;
	fread(buff, stol(value_size), 1, wk->logfile);
	// cout << buffer << endl;
	value = *buff;
	
	//rewind(wk->logfile);
	return true;
}