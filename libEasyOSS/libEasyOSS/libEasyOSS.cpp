#include "libEasyOSS.h"
#include "EasyOSS.h"
#include <oss_api.h>
#include <aos_http_io.h>
#include <aos_log.h>
#include <aos_define.h>
#include <string>

using namespace std;

#define EasyOSS_Object(x) ((EasyOSS*)x)

static string BUCKET_NAME;
static string OSS_ENDPOINT;
static size_t OSS_PORT;
static string ACCESS_KEY_ID;
static string ACCESS_KEY_SECRET;


int EasyOSS_Initialize(const char* bucket_name, const char* oss_endpoint, size_t oss_port, const char* access_key_id, const char* access_key_secret)
{
	if (aos_http_io_initialize("EasyDarwin", 0) != AOSE_OK) 
	{
		aos_error_log("EasyOSS_Initialize error\n");
		return -1;
	}

	BUCKET_NAME = bucket_name;
	OSS_ENDPOINT = oss_endpoint;
	OSS_PORT = oss_port;
	ACCESS_KEY_ID = access_key_id;
	ACCESS_KEY_SECRET = access_key_secret;

	aos_pool_t *p;
	int is_oss_domain = 1;//是否使用三级域名，可通过is_oss_domain函数初始化
	oss_request_options_t * oss_request_options;
	aos_status_t *s;
	aos_table_t *resp_headers;
	oss_acl_e oss_acl = OSS_ACL_PRIVATE;
	aos_string_t bucket;		
	aos_pool_create(&p, NULL);
	// init_ oss_request_options		
	oss_request_options = oss_request_options_create(p);
	oss_request_options->config = oss_config_create(oss_request_options->pool);
	aos_str_set(&oss_request_options->config->host, OSS_ENDPOINT.c_str());
	oss_request_options->config->port = OSS_PORT;
	aos_str_set(&oss_request_options->config->id, ACCESS_KEY_ID.c_str());
	aos_str_set(&oss_request_options->config->key, ACCESS_KEY_SECRET.c_str());
	oss_request_options->config->is_oss_domain = is_oss_domain;
	oss_request_options->ctl = aos_http_controller_create(oss_request_options->pool, 0);

	aos_str_set(&bucket, bucket_name);
	resp_headers = aos_table_make(p, 0);
	s = aos_status_create(p);
	s = oss_create_bucket(oss_request_options, &bucket, oss_acl, &resp_headers);
	int ret = -1;
	if (aos_status_is_ok(s))
	{
		ret = 0;
	}
	else
	{
		aos_error_log("EasyOSS_Initialize error: %s, error code %d\n", s->error_msg, s->code);
	}
	aos_pool_destroy(p);

	return ret;
}

EasyOSS_Handle EasyOSS_Open(const char* object_name)
{
	EasyOSS *oss = new EasyOSS(BUCKET_NAME.c_str(), object_name, OSS_ENDPOINT, OSS_PORT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
	oss->Initialize();
	return oss;
}

int EasyOSS_Write(EasyOSS_Handle handle, const void* data, size_t size)
{
	return EasyOSS_Object(handle)->Write(data, size);
}

int EasyOSS_Read(EasyOSS_Handle handle, void* data, size_t size)
{
	return EasyOSS_Object(handle)->Read((char*)data, size);
}

int EasyOSS_Delete(EasyOSS_Handle handle)
{
	return EasyOSS_Object(handle)->Delete();
}

unsigned long EasyOSS_Size(EasyOSS_Handle handle)
{
	return EasyOSS_Object(handle)->Size();
}

int EasyOSS_Close(EasyOSS_Handle handle)
{
	EasyOSS_Object(handle)->Deinitialize();
	delete EasyOSS_Object(handle);	
	handle = NULL;
	return 0;
}

void EasyOSS_Deinitialize()
{
	aos_http_io_deinitialize();
}