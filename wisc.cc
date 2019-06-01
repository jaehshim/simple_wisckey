#include "wisc.h"



void wisc_put(WK *wk, string &key, string &value)
{
	long offset = ftell(wk->logfile);
	long size = value.length();
	//cout << "wiscput" << endl;
	ostringstream logStream;
	logStream << key << deli_key_addr << value;
	string logstr = logStream.str();
	fwrite(&logstr, value.length() + 1, 1, wk->logfile); // write value to vlog


	
	cout << "offset " << offset << "size " << size << endl;
	string value_addr ;
	string value_size ;
	value_addr = to_string(offset);
	value_size = to_string(size);
	ostringstream lsmStream;
	lsmStream << value_addr << deli_addr_size << value_size ; // lsmt string (value_addr, value_size)
	string lsmstr = lsmStream.str();
	lsmt_put(wk->leveldb, key, lsmstr); // write to lsmt
}

bool wisc_get(WK *wk, string &key, string &value)
{
	//cout << "wiscget" << endl;
	string lsmstr;
	//cout << "1" << endl;
	cout << key << endl;
	const bool found = lsmt_get(wk->leveldb, key, lsmstr);
	if (!found)
		return false;
	string value_addr_str;
	string value_size_str;
	size_t pos = 0;
	string token;
	if((pos = lsmstr.find(deli_addr_size)) != string::npos) // find delimeter
	{
		value_addr_str = lsmstr.substr(0, pos);
		cout << value_addr_str << endl;
		lsmstr.erase(0, pos + deli_length);
		value_size_str = lsmstr;
		cout << value_size_str << endl;

	}
	else
	{
		cout << "lsmt error" << endl ;
		exit(1);	
	}
	//cout << "2" << endl;

	long value_addr = stol(value_addr_str);
	long value_size = stol(value_size_str);
	
		// cout << ftell(wk->logfile) << endl;
	fseek(wk->logfile, value_addr, SEEK_SET);
	// cout << ftell(wk->logfile) << endl;
	//cout << "3" << endl;

	char buff[value_size+1];

	fread(buff, value_size, 1, wk->logfile);
	// cout << buffer << endl;
	//cout << "4" << endl;
	cout << buff << endl;
	exit(1);
	
	//rewind(wk->logfile);
	return true;
}
