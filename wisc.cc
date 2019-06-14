#include "wisc.h"

void wisc_put(WK *wk, string &key, string &value)
{
//    fstream logStream(wk->logfile, fstream::out | fstream::in);

    string input = key + DELIMITER + value + GC_DELIMITER;

    long long offset = wk->head;
    long long size = input.length();
    char ch[size];
    strcpy(ch, input.c_str());

    if (FILE_SIZE - (wk->head%FILE_SIZE) > size) {
        long long off = wk->head % FILE_SIZE;
    //    logStream.seekp(off, ios::beg);
        wk->logStream.write(ch, size);
        wk->head += size;
    }
    else
    {
        cout << "else write" << endl;
        // TODO: write 2번으로 끝나게 최적화
        for (int j = 0; j < size; j++)
        {
            long long off = wk->head % FILE_SIZE;
            wk->logStream.seekp(off, ios::beg);
            wk->logStream.write(&ch[j], 1);
            wk->head++;
        }
    }

    wk->logStream.flush();
    wk->logStream.sync();

    //    logStream.close();

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
    int offset = value_addr;
    string data(value_size, '\0');
    
    // TODO : write이랑 똑같게
    for (int i = 0; i < value_size; i++)
    {
        offset = offset % FILE_SIZE;
        infile.seekg(offset, ios::beg);

        infile.read(&data[i], 1);
        offset++;
    }

    infile.close();

    if ((pos = data.find(DELIMITER)) != string::npos) // find delimeter
    {
        data.erase(0, pos + DELI_LENGTH);

        data.erase(data.end()-2, data.end());

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
