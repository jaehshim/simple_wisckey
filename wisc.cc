#include "wisc.h"



void wisc_put(WK *wk, string &key, string &value)
{
	fstream logStream(wk->logfile, fstream::out | fstream::app);

    long offset = logStream.tellp();
    string input = key + DELIMITER + value;
    long size = input.length();

	logStream << input;

    logStream.flush();
    logStream.sync();

    logStream.close();

//	cout << "offset " << offset << "size " << size << endl;

	string value_addr ;
	string value_size ;
	value_addr = to_string(offset);
	value_size = to_string(size);
	ostringstream lsmStream;
	lsmStream << value_addr << DELIMITER << value_size ; // lsmt string (value_addr, value_size)
	string lsmstr = lsmStream.str();
	lsmt_put(wk->leveldb, key, lsmstr); // write to lsmt
}

bool wisc_get(WK *wk, string &key, string &value)
{
	//cout << "wiscget" << endl;
	string lsmstr;

	const bool found = lsmt_get(wk->leveldb, key, lsmstr);
	if (!found)
		return false;
	string value_addr_str;
	string value_size_str;
	size_t pos = 0;
	string token;
    int num;

	if((pos = lsmstr.find(DELIMITER)) != string::npos) // find delimeter
	{
		value_addr_str = lsmstr.substr(0, pos);

		lsmstr.erase(0, pos + DELI_LENGTH);
		value_size_str = lsmstr;
	}
	else
	{
		cout << "lsmt error" << endl ;
		exit(1);	
	}

	long value_addr = stol(value_addr_str);
	long value_size = stol(value_size_str);
	
    fstream infile(wk->logfile);

    infile.seekg(value_addr, ios::beg);
    string data(value_size, '\0');

    infile.read(&data[0], value_size);

    infile.close();

    if((pos = data.find(DELIMITER)) != string::npos) // find delimeter
	{
		data.erase(0, pos + DELI_LENGTH);
		value = data;
	}
	else
	{
		cout << "lsmt error" << endl ;
		exit(1);	
	}

	return true;
}

void timer (bool start = true, const char *label = 0) {
    static chrono::system_clock::time_point startTime;
    if (start) {
        startTime = chrono::system_clock::now();
    } else {
        chrono::system_clock::time_point endTime = chrono::system_clock::now();
        printf("Elapsed Time (%s): %.6lf s\n", label, chrono::duration_cast<chrono::microseconds>(endTime - startTime).count() / 1000.0 / 1000.0);
    }
}

void startTimer() {
    timer(true);
}

void stopTimer(const char *label) {
    timer(false, label);
}