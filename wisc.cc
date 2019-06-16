#include "wisc.h"
#include <unordered_map>

string str_head = "head";
string str_tail = "tail";

map<string, int> ref_key_store;

template<typename K, typename V>
void print_map(std::unordered_map<K,V> const &m)
{
    for (auto const& pair: m) {
        cout << "{" << pair.first << ": " << pair.second << "}\n";
    }
}


void wisc_put(WK *wk, string &key, string &value)
{
    if (ref_key_store.find(key) != ref_key_store.end()) // key가 storage에 있으면
    {
        ref_key_store[key]++;
    }
    else
    {
        ref_key_store.insert(pair<string,int>(key, 0));
    }

    if (value.length() <= SELECTIVE_THRESHOLD) // selective KV
    {
        string input = SELECRIVE + value;

	    lsmt_put(wk->leveldb, key, input); // write to lsmt

        return;
    }

    //cout << "wisc_put" << endl;

    

    string input = key + DELIMITER + value + GC_DELIMITER;

    long long size = input.length();

    char ch[size];

    int gc_flag = 0;

    do
    {
        gc_flag = gc_check(wk, (key.length() + value.length() + DELI_LENGTH * 2));
        if (gc_flag)
        {
            //cout << wk->head << ":::::" << wk->tail << endl;
            gc_proc(wk);
        }
    }
    while (gc_flag); // GC를 해도 충분한 공간을 만들지 못한 경우

    long long offset = wk->head;

    strcpy(ch, input.c_str());
    vlog_write(wk, size, ch);

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
    //cout << "wisc_get" << endl;
    if (ref_key_store.find(key) != ref_key_store.end()) // key가 storage에 있으면
    {
        ref_key_store[key]++;
    }
    else
    {
        cout << "get before put?"<< key << endl;
        //print_map(ref_key_store);
        ref_key_store.insert(pair<string,int>(key, 0));
    }

	//cout << "wiscget" << endl;
	string lsmstr;

	const bool found = lsmt_get(wk->leveldb, key, lsmstr);
	if (!found)
		return false;
	string value_addr_str;
	string value_size_str;
	size_t pos = 0;
	string token;
    long long num;

    if((lsmstr.find(SELECRIVE)) != string::npos) // selective KV
	{
        lsmstr.erase(0, 1);
        value = lsmstr;

        // cout << "selective KV" << endl;
        
        return true;
    }

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

    //cout << "1 " << value_addr_str << "####" << value_size_str <<endl;
	long value_addr = stol(value_addr_str);
	long value_size = stol(value_size_str);

    long long offset = value_addr;
    string data(value_size, '\0');
    
    // TODO : write이랑 똑같게

    vlog_read(wk, offset, value_size, data, 0);
    
    if ((pos = data.find(DELIMITER)) != string::npos) // find delimeter
    {
        data.erase(0, pos + DELI_LENGTH);

        data.erase(data.end()-2, data.end());

        value = data;
    }
    else
    {
		cout << "vlog error" << endl ;
        cout << data << endl;
		exit(1);	
	}

	return true;
}

void vlog_read(WK *wk, long long offset, long long value_size, string &data, int gc_mode)
{
    //cout << "vlog_read" << endl;
    long long pos1, pos2, pos3, cold_addr, cold_size;
    string temp;
    fstream infile(wk->logfile);
    if (FILE_SIZE - (offset % FILE_SIZE) > value_size)
    {
        offset = offset % FILE_SIZE;
        infile.seekg(offset, ios::beg);
        infile.read(&data[0], value_size);
    }
    else
    {
        // TODO: write 2번으로 끝나게 최적화
        for (long long i = 0; i < value_size; i++)
        {
            offset = offset % FILE_SIZE;
            infile.seekg(offset, ios::beg);

            infile.read(&data[i], 1);
            offset++;
        }
    }
    infile.close();

    if(!gc_mode) // gc 과정 중에 읽는 경우에는 vlog data 그대로 돌려줌, 그 외에는 clog data 가져와서 돌려줌
    {
        pos3 = data.find(GC_DELIMITER);
        temp = data.substr(0, pos3+DELI_LENGTH);
        if ((pos2 = temp.find(COLD_DELIMITER)) != string::npos) // find cold_delimeter
        {
            pos1 = temp.find(DELIMITER);
           // cout << "2:: " << pos1 << "####" << pos2 << "data: "<< temp <<endl;

            cold_addr = stol(temp.substr(pos1+DELI_LENGTH, pos2));
            cold_size = stol(temp.substr(pos2+DELI_LENGTH, string::npos));
           // cout << cold_addr << "//" << cold_size << endl;
            clog_read(wk, cold_addr, cold_size, data);
            //cout << "cold data" << data << endl;
        }
    }


}

void clog_read(WK *wk, long long offset, long long value_size, string &data)
{
    //cout << "clogread" << endl;
    string test(value_size, '\0');
    fstream infile(wk->coldfile);
    infile.seekg(offset, ios::beg);
    infile.read(&test[0], value_size);
    
    data = test;

    infile.close();
    
}

void vlog_write(WK *wk, long long size, char *ch)
{
    //cout << "vlogwrite" << endl;
    long long off = wk->head % FILE_SIZE;
    if (FILE_SIZE - off > size) {
        wk->logStream.seekp(off, ios::beg);
        wk->logStream.write(ch, size);
        wk->head += size;
    }
    else
    {
        //cout << "else write" << endl;
        // TODO: write 2번으로 끝나게 최적화
        for (long long j = 0; j < size; j++)
        {            
            off = wk->head % FILE_SIZE;
            wk->logStream.seekp(off, ios::beg);
            wk->logStream.write(&ch[j], 1);
            wk->head++;
        }
    }

    wk->logStream.flush();
    wk->logStream.sync();
}

void clog_write(WK *wk, long long size, char *ch)
{
    //cout << "clogwrite" << endl;
    long long off = wk->chead;
    wk->coldStream.seekp(off, ios::beg);
    wk->coldStream.write(ch, size);
    wk->chead += size;

    wk->coldStream.flush();
    wk->coldStream.sync();
}



int gc_check(WK *wk, long long valuesize)
{
    //cout << "gccheck" << endl;
    int gc_policy = GC_DEMAND;
    long long remain_space;
    long long head = wk->head % FILE_SIZE;
    long long tail = wk->tail % FILE_SIZE;

    // cout << "wkhead " << wk->head << " wktail " << wk->tail << endl;

    if (head >= tail)
    {
        remain_space = FILE_SIZE - (head - tail); // head front, tail back
    }
    else
    {
        remain_space = tail - head; // head is chasing the tail
    }           
     //cout << "head " << head << " tail " << tail << " rem " << remain_space << " val " << valuesize << endl;

    
    switch (gc_policy)
    {
    case GC_DEMAND:
        
        if(remain_space < valuesize+5)
        {
            cout << "gc trig" << endl;
            //cout << "wkhead " << wk->head << " wktail " << wk->tail << endl;
            return 1;
        }
        else return 0;
        break;
    
    default:
        break;
    }
}

int gc_proc(WK *wk)
{
    //cout << "gcproc" << endl;
    long long bias = 0; // 0 부터 시작하여 KVpair size 만큼씩 증가
    long long length;
    bool validity;
    string key, temp;
    char *key_buff;
    char *temp_buff;
    long long tail = 0;

    while (bias < GC_CHUNK_SIZE)
    {
        key_buff = (char *)calloc(GC_CHUNK_SIZE, sizeof(char)); // 임시 저장
        temp_buff = (char *)calloc(GC_CHUNK_SIZE, sizeof(char)); // 임시 저장
        validity = vlog_parser(wk, bias, length, key_buff, temp_buff);
        if (validity)
        {
            key = key_buff;
            temp = temp_buff;
            
            if ((ref_key_store[key] > 0) || (temp.find(COLD_DELIMITER) != string::npos )) // hot data 또는 이미 cold로 분류된 데이터
            {
                string value_addr ;
                string value_size ;
                value_addr = to_string(wk->head);
                value_size = to_string(length);
                ostringstream lsmStream;
                lsmStream << value_addr << DELIMITER << value_size ; // lsmt string (value_addr, value_size)
                string lsmstr = lsmStream.str();
                lsmt_put(wk->leveldb, key, lsmstr);
                vlog_write(wk, length, temp_buff);
            }
            else // cold data
            {
                
                cout << "cold separation" << endl;
                cout << key << ":tail:" << wk->tail << endl;
                string tag_value_addr ;
                string tag_value_size ;
                string tagged_value;
                char *tag_buff;
                tag_buff = (char *)calloc(GC_CHUNK_SIZE, sizeof(char));

                tagged_value = key + DELIMITER + to_string(wk->chead) + COLD_DELIMITER + to_string(length) + GC_DELIMITER;
                strcpy(tag_buff, tagged_value.c_str());
                tag_value_addr = to_string(wk->head);
                tag_value_size = to_string(tagged_value.length());
                ostringstream lsmStream;
                lsmStream << tag_value_addr << DELIMITER << tag_value_size ; // lsmt string (value_addr, value_size)
                string lsmstr = lsmStream.str();
                lsmt_put(wk->leveldb, key, lsmstr);
                long long tag_value_size_long = stoll(tag_value_size);
                vlog_write(wk, tag_value_size_long, tag_buff);
                clog_write(wk, length, temp_buff);  

                free(tag_buff);    

            }            
        }
        tail += length;
        free(key_buff);
        free(temp_buff);
    }

    wk->tail += tail;
    // string head_offset = to_string(wk->head);
    // string tail_offset = to_string(wk->tail);
    // lsmt_put(wk->leveldb, str_head, head_offset);
    // lsmt_put(wk->leveldb, str_tail, tail_offset);
    return 0;
}



int vlog_parser(WK *wk, long long &bias, long long &length, char *key_buff, char *temp_buff) // vlog의 KVpair을 읽은 뒤 valid check하여 gc_buff에 담아줌, 다음 KVpair로 offset 증가
{
   // cout << "vlogpar" << endl;
   // cout << "2.5" << endl;
    string data, key;
    bool validity;
    long long pos;
    long long size = GC_DEFAULT_READ_SIZE;
    long long offset = wk->tail + bias;

    //cout << wk-> tail << "(((((" << offset << ":::::" << length << endl;
    while (1)
    {
        string temp(size, '\0');
        vlog_read(wk, offset, size, temp, 1);
        size += GC_INCR;
        //cout << temp.find(GC_DELIMITER) << endl;
        if (temp.find(GC_DELIMITER) != string::npos)
        {
            data = temp;
            break;
        }
    }


    length = data.find(GC_DELIMITER) + DELI_LENGTH; // 해당 pair의 총 길이
    
    bias += length; // bias update    
    data = data.substr(0, length); // target KV pair
    pos = data.find(DELIMITER);
    key = data.substr(0, pos);
    // cout << data << endl;
    validity = valid_check(wk, key, offset);
    if (validity)
    {
        strcpy(key_buff, key.c_str());
        strcpy(temp_buff, data.c_str());
    }
    return validity;
}


int valid_check(WK *wk, string &key, long long &offset)
{
    //cout << "valch" << endl;
    string lsmstr;
    bool validity;

    bool found = lsmt_get(wk->leveldb, key, lsmstr);
	if (!found) // vlog에 있는 key가 lsmt에 없는 상황
    {
        //cout << key << "::lsmstr::" << lsmstr << endl;
        printf("WTF\n");
        exit(1);
    }
    string value_addr_str;
	//string value_size_str;
	size_t pos = 0;
	string token;
    int num;

	if((pos = lsmstr.find(DELIMITER)) != string::npos) // find delimeter
	{
		value_addr_str = lsmstr.substr(0, pos);

		//lsmstr.erase(0, pos + DELI_LENGTH);
		//value_size_str = lsmstr;
	}
	else
	{
		cout << "lsmt error" << endl ;
		exit(1);	
	}
    //cout << "3 " << value_addr_str <<endl;

	long value_addr = stol(value_addr_str);
	//long value_size = stol(value_size_str);
    validity = (offset == value_addr);

    return validity;

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
