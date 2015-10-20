#include "EasyOSS.h"
#include <oss_api.h>
#include <aos_http_io.h>
#include <aos_log.h>
#include <aos_define.h>


EasyOSS::EasyOSS(const char* bucket_name, const char* object_name, string oss_endpoint, size_t oss_port, string access_key_id, string access_key_secret)
: bucket_name_(bucket_name)
, object_name_(object_name)
, oss_endpoint_(oss_endpoint)
, oss_port_(oss_port)
, access_key_id_(access_key_id)
, access_key_secret_(access_key_secret)
, position_(0)
, get_size_(false)
, size_(0)
{
}


EasyOSS::~EasyOSS()
{
}

void EasyOSS::Initialize()
{
}

void EasyOSS::Deinitialize()
{	
}

int EasyOSS::Read(char * data, size_t size)
{
	if (position_ >= Size())
	{
		aos_warn_log("EasyOSS::Read finish");
		return -1;
	}
	aos_pool_t *p;
	oss_request_options_t *oss_request_options;
	aos_status_t *s;
	aos_table_t *headers;
	aos_table_t *resp_headers;
	aos_string_t bucket;
	aos_string_t object;

	aos_pool_create(&p, NULL);

	int is_oss_domain = 1;
	oss_request_options = oss_request_options_create(p);
	oss_request_options->config = oss_config_create(oss_request_options->pool);
	aos_str_set(&oss_request_options->config->host, oss_endpoint_.c_str());
	oss_request_options->config->port = oss_port_;
	aos_str_set(&oss_request_options->config->id, access_key_id_.c_str());
	aos_str_set(&oss_request_options->config->key, access_key_secret_.c_str());
	oss_request_options->config->is_oss_domain = is_oss_domain;
	oss_request_options->ctl = aos_http_controller_create(oss_request_options->pool, 0);

	aos_str_set(&object, object_name_.c_str());
	aos_str_set(&bucket, bucket_name_.c_str());
	headers = aos_table_make(p, 1);
	resp_headers = aos_table_make(p, 0);
	s = aos_status_create(p);
	
	char range[128] = { 0 };
	sprintf(range, " bytes=%lu-%lu", position_, min((unsigned long)(position_ + size - 1), Size() - 1));
	apr_table_set(headers, "Range", range);	
	aos_list_t buffer;
	aos_buf_t *content;
	size_t len = 0;
	size_t pos = 0;
	aos_list_init(&buffer);

	s = oss_get_object_to_buffer(oss_request_options, &bucket, &object, headers, &buffer, &resp_headers);
	
	//aos_list
	if (aos_status_is_ok(s))
	{
		//copy buffer content to memory
		aos_list_for_each_entry(aos_buf_t, content, &buffer, node)
		{
			len = aos_buf_size(content);
			if (len > size || pos + len > size)
			{
				aos_fatal_log("EasyOSS::Read warn: read size[%d/%d] large than buffer size[%d]\n", len, pos + len, size);			
				aos_pool_destroy(p);
				return -1;
			}

			if (position_ + pos + len > Size())
			{
				aos_error_log("EasyOSS::Read error: read size[%d] large than buffer size[%d], total[%d, %d]", pos + len, size, position_ + pos + len, Size());				
				aos_pool_destroy(p);
				return pos;
			}
			memcpy(data + pos, content->pos, len);
			pos += len;
		}
		position_ += pos;
	}
	else
	{
		aos_error_log("EasyOSS::Read error: %s, error code %d\n", s->error_msg, s->code);
		pos = -1;
	}
	aos_pool_destroy(p);
	return pos;
}

int EasyOSS::Write(const void * data, size_t size)
{
	aos_pool_t *p;
	oss_request_options_t *oss_request_options;
	aos_status_t *s;
	aos_table_t *headers;
	aos_table_t *resp_headers;
	aos_string_t bucket;
	aos_string_t object;
	
	aos_pool_create(&p, NULL);
	
	int is_oss_domain = 1;
	oss_request_options = oss_request_options_create(p);
	oss_request_options->config = oss_config_create(oss_request_options->pool);
	aos_str_set(&oss_request_options->config->host, oss_endpoint_.c_str());
	oss_request_options->config->port = oss_port_;
	aos_str_set(&oss_request_options->config->id, access_key_id_.c_str());
	aos_str_set(&oss_request_options->config->key, access_key_secret_.c_str());
	oss_request_options->config->is_oss_domain = is_oss_domain;
	oss_request_options->ctl = aos_http_controller_create(oss_request_options->pool, 0);

	aos_str_set(&object, object_name_.c_str());
	aos_str_set(&bucket, bucket_name_.c_str());
	headers = aos_table_make(p, 0);
	resp_headers = aos_table_make(p, 0);
	s = aos_status_create(p);

	aos_list_t buffer;
	aos_buf_t *content;
	aos_list_init(&buffer);
	content = aos_buf_pack(oss_request_options->pool, data, size);
	aos_list_add_tail(&content->node, &buffer);
	s = oss_append_object_from_buffer(oss_request_options, &bucket, &object, position_, &buffer, headers, &resp_headers);
	if (aos_status_is_ok(s))
	{
		position_ += size;		
	}
	else
	{
		aos_error_log("EasyOSS::Write error: %s, error code %d\n", s->error_msg, s->code);
		size = -1;
	}
	aos_pool_destroy(p);
	return size;
}

int EasyOSS::Delete()
{
	aos_pool_t *p;	
	oss_request_options_t * oss_request_options;
	aos_status_t *s;
	aos_table_t *resp_headers;
	aos_string_t bucket;
	aos_string_t object;

	aos_pool_create(&p, NULL);

	int is_oss_domain = 1;
	oss_request_options = oss_request_options_create(p);
	oss_request_options->config = oss_config_create(oss_request_options->pool);
	aos_str_set(&oss_request_options->config->host, oss_endpoint_.c_str());
	oss_request_options->config->port = oss_port_;
	aos_str_set(&oss_request_options->config->id, access_key_id_.c_str());
	aos_str_set(&oss_request_options->config->key, access_key_secret_.c_str());
	oss_request_options->config->is_oss_domain = is_oss_domain;
	oss_request_options->ctl = aos_http_controller_create(oss_request_options->pool, 0);

	aos_str_set(&object, object_name_.c_str());
	aos_str_set(&bucket, bucket_name_.c_str());
	resp_headers = aos_table_make(p, 0);
	s = aos_status_create(p);

	s = oss_delete_object(oss_request_options, &bucket, &object, &resp_headers);
	int ret = -1;
	if (aos_status_is_ok(s))
	{
		ret = 0;
	}
	else
	{
		aos_error_log("EasyOSS::Delete error: %s, error code %d\n", s->error_msg, s->code);		
	}
	aos_pool_destroy(p);
	return ret;
}

unsigned long EasyOSS::Size()
{
	if (get_size_) return size_;

	aos_pool_t *p;
	oss_request_options_t *oss_request_options;
	aos_status_t *s;
	aos_table_t *headers;
	aos_table_t *resp_headers;
	aos_string_t bucket;
	aos_string_t object;

	aos_pool_create(&p, NULL);

	int is_oss_domain = 1;
	oss_request_options = oss_request_options_create(p);
	oss_request_options->config = oss_config_create(oss_request_options->pool);
	aos_str_set(&oss_request_options->config->host, oss_endpoint_.c_str());
	oss_request_options->config->port = oss_port_;
	aos_str_set(&oss_request_options->config->id, access_key_id_.c_str());
	aos_str_set(&oss_request_options->config->key, access_key_secret_.c_str());
	oss_request_options->config->is_oss_domain = is_oss_domain;
	oss_request_options->ctl = aos_http_controller_create(oss_request_options->pool, 0);

	aos_str_set(&object, object_name_.c_str());
	aos_str_set(&bucket, bucket_name_.c_str());
	headers = aos_table_make(p, 0);
	resp_headers = aos_table_make(p, 0);
	s = aos_status_create(p);
		
	s = oss_head_object(oss_request_options, &bucket, &object, headers, &resp_headers);
	size_ = 0;
	if (aos_status_is_ok(s))
	{
		const char* content_length = apr_table_get(resp_headers, "Content-Length");
		if (content_length != NULL)
		{
			size_ = std::atol(content_length);
			get_size_ = true;
		}
		else
		{
			aos_warn_log("EasyOSS::Read apr_table_get Content-Length error\n");
		}		
	}
	else
	{
		aos_error_log("EasyOSS::Write error: %s, error code %d\n", s->error_msg, s->code);		
	}
	aos_pool_destroy(p);
	return size_;

}

