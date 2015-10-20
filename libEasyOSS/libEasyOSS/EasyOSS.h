#pragma once
#include <string>
#include <oss_api.h>
#include <aos_http_io.h>
#include <aos_log.h>
#include <aos_define.h>

using namespace std;

class EasyOSS
{
public:
	EasyOSS(const char* bucket_name, const char* object_name, string oss_endpoint, size_t oss_port, string access_key_id, string access_key_secret);
	~EasyOSS();

public:
	void Initialize();
	void Deinitialize();

	int Read(char* data, size_t size);
	int Write(const void* data, size_t size);
	int Delete();
	unsigned long Size();
private:
	string object_name_;
	string bucket_name_;
	string oss_endpoint_;
	size_t oss_port_;
	string access_key_id_;
	string access_key_secret_;
	size_t position_;	
	unsigned long size_;
	bool get_size_;
};

