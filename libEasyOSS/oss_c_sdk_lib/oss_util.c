#include "aos_string.h"
#include "aos_util.h"
#include "aos_log.h"
#include "aos_status.h"
#include "oss_auth.h"
#include "oss_util.h"

static char *OSS_HOSTNAME_SUFFIX[] = {
    ".aliyun-inc.com", 
    ".aliyuncs.com", 
    ".alibaba.net"};

int is_oss_domain(const aos_string_t *str)
{
    int i = 0;
    aos_string_t oss_domain_suffix;

    for ( ; i < sizeof(OSS_HOSTNAME_SUFFIX)/sizeof(OSS_HOSTNAME_SUFFIX[0]); i++) {
        aos_str_set(&oss_domain_suffix, OSS_HOSTNAME_SUFFIX[i]);
        if (aos_ends_with(str, &oss_domain_suffix)) {
            return 1;
        }
    }

    return 0;

}

oss_config_t *oss_config_create(aos_pool_t *p)
{
    return (oss_config_t *)aos_pcalloc(p, sizeof(oss_config_t));
}

oss_request_options_t *oss_request_options_create(aos_pool_t *p)
{
    int s;
    oss_request_options_t *options;

    if(p == NULL) {
        if ((s = aos_pool_create(&p, NULL)) != APR_SUCCESS) {
            aos_fatal_log("aos_pool_create failure.");
            return NULL;
        }
    }

    options = (oss_request_options_t *)aos_pcalloc(p, sizeof(oss_request_options_t));
    options->pool = p;

    return options;
}

void oss_get_object_uri(const oss_request_options_t *options, const aos_string_t *bucket, 
    const aos_string_t *object, aos_http_request_t *req)
{
    req->resource = apr_psprintf(options->pool, "%.*s/%.*s", 
            bucket->len, bucket->data, object->len, object->data);
    if (options->config->is_oss_domain) {
        req->host = apr_psprintf(options->pool, "%.*s.%.*s", 
            bucket->len, bucket->data, options->config->host.len, options->config->host.data);
        req->uri = object->data;
    } else {
        req->host = options->config->host.data;
        req->uri = req->resource;
    }
}

void oss_get_bucket_uri(const oss_request_options_t *options, const aos_string_t *bucket,
    aos_http_request_t *req)
{
    req->resource = apr_psprintf(options->pool, "%.*s/", bucket->len, bucket->data);
    if (options->config->is_oss_domain) {
        req->host = apr_psprintf(options->pool, "%.*s.%.*s", 
            bucket->len, bucket->data, options->config->host.len, options->config->host.data);
        req->uri = apr_psprintf(options->pool, "%s", "");
    } else {
        req->host = options->config->host.data;
        req->uri = req->resource;
    }
}

void oss_write_request_body_from_buffer(aos_list_t *buffer, aos_http_request_t *req)
{
    aos_list_movelist(buffer, &req->body);
    req->body_len = aos_buf_list_len(&req->body);
}

int oss_write_request_body_from_file(aos_pool_t *p, const aos_string_t *filename, aos_http_request_t *req)
{
    int res = AOSE_OK;
    aos_file_buf_t *fb = aos_create_file_buf(p);
    res = aos_open_file_for_all_read(p, filename->data, fb);
    if (res != AOSE_OK) {
        aos_error_log("Open read file fail, filename:%s\n", filename->data);
        return res;
    }

    req->body_len = fb->file_last;
    req->file_path = filename->data;
    req->file_buf = fb;
    req->type = BODY_IN_FILE;
    req->read_body = aos_read_http_body_file;

    return res;
}

int oss_write_request_body_from_upload_file(aos_pool_t *p, oss_upload_file_t *upload_file, aos_http_request_t *req)
{
    int res = AOSE_OK;
    aos_file_buf_t *fb = aos_create_file_buf(p);
    res = aos_open_file_for_range_read(p, upload_file->filename.data, upload_file->file_pos, upload_file->file_last, fb);
    if (res != AOSE_OK) {
        aos_error_log("Open read file fail, filename:%s\n", upload_file->filename.data);
        return res;
    }

    req->body_len = fb->file_last - fb->file_pos;
    req->file_path = upload_file->filename.data;
    req->file_buf = fb;
    req->type = BODY_IN_FILE;
    req->read_body = aos_read_http_body_file;

    return res;
}

void oss_init_read_response_body_to_buffer(aos_list_t *buffer, aos_http_response_t *resp)
{
    aos_list_movelist(&resp->body, buffer);
}

int oss_init_read_response_body_to_file(aos_pool_t *p, const aos_string_t *filename, aos_http_response_t *resp)
{
    int res = AOSE_OK;
    aos_file_buf_t *fb = aos_create_file_buf(p);
    res = aos_open_file_for_write(p, filename->data, fb);
    if (res != AOSE_OK) {
        aos_error_log("Open write file fail, filename:%s\n", filename->data);
        return res;
    }
    resp->file_path = filename->data;
    resp->file_buf = fb;
    resp->write_body = aos_write_http_body_file;
    resp->type = BODY_IN_FILE;

    return res;
}

void *oss_create_api_result_content(aos_pool_t *p, size_t size)
{
    void *result_content = aos_palloc(p, size);
    if (NULL == result_content) {
        return NULL;
    }
    
    aos_list_init((aos_list_t *)result_content);

    return result_content;
}

oss_list_object_content_t *oss_create_list_object_content(aos_pool_t *p)
{
    oss_list_object_content_t *list_object_content = NULL;
    list_object_content = (oss_list_object_content_t *)oss_create_api_result_content(p, 
        sizeof(oss_list_object_content_t));
    
    return list_object_content;
}

oss_list_object_common_prefix_t *oss_create_list_object_common_prefix(aos_pool_t *p)
{
    oss_list_object_common_prefix_t *list_object_common_prefix = NULL;
    list_object_common_prefix = (oss_list_object_common_prefix_t *)oss_create_api_result_content(p,
        sizeof(oss_list_object_common_prefix_t));

    return list_object_common_prefix;
}

oss_list_multipart_upload_content_t *oss_create_list_multipart_upload_content(aos_pool_t *p)
{
    oss_list_multipart_upload_content_t *list_multipart_upload_content = NULL;
    list_multipart_upload_content = (oss_list_multipart_upload_content_t*)oss_create_api_result_content(p,
        sizeof(oss_list_multipart_upload_content_t));

    return list_multipart_upload_content;
}

oss_list_part_content_t *oss_create_list_part_content(aos_pool_t *p)
{
    oss_list_part_content_t *list_part_content = NULL;
    list_part_content = (oss_list_part_content_t*)oss_create_api_result_content(p,
        sizeof(oss_list_part_content_t));

    return list_part_content;
}

oss_complete_part_content_t *oss_create_complete_part_content(aos_pool_t *p)
{
    oss_complete_part_content_t *complete_part_content = NULL;
    complete_part_content = (oss_complete_part_content_t*)oss_create_api_result_content(p,
        sizeof(oss_complete_part_content_t));

    return complete_part_content;
}

oss_list_object_params_t *oss_create_list_object_params(aos_pool_t *p)
{
    oss_list_object_params_t * params;
    params = (oss_list_object_params_t *)aos_pcalloc(p, sizeof(oss_list_object_params_t));
    aos_list_init(&params->object_list);
	aos_list_init(&params->common_prefix_list);
    aos_str_set(&params->prefix, "");
    aos_str_set(&params->marker, "");
    aos_str_set(&params->delimiter, "");
    params->truncated = 0;
    return params;
}

oss_list_upload_part_params_t *oss_create_list_upload_part_params(aos_pool_t *p)
{
    oss_list_upload_part_params_t *params;
    params = (oss_list_upload_part_params_t *)aos_pcalloc(p, sizeof(oss_list_upload_part_params_t));
    aos_list_init(&params->part_list);
    aos_str_set(&params->part_number_marker, "");
    params->truncated = 0;
    return params;
}

oss_list_multipart_upload_params_t *oss_create_list_multipart_upload_params(aos_pool_t *p)
{
    oss_list_multipart_upload_params_t *params;
    params = (oss_list_multipart_upload_params_t *)aos_pcalloc(p, sizeof(oss_list_multipart_upload_params_t));
    aos_list_init(&params->upload_list);
    aos_str_set(&params->prefix, "");
    aos_str_set(&params->key_marker, "");
    aos_str_set(&params->upload_id_marker, "");
    aos_str_set(&params->delimiter, "");
    params->truncated = 0;
    return params;
}

oss_upload_part_copy_params_t *oss_create_upload_part_copy_params(aos_pool_t *p)
{
    return (oss_upload_part_copy_params_t *)aos_pcalloc(p, sizeof(oss_upload_part_copy_params_t));
}

oss_lifecycle_rule_content_t *oss_create_lifecycle_rule_content(aos_pool_t *p)
{
    oss_lifecycle_rule_content_t *rule;
    rule = (oss_lifecycle_rule_content_t *)aos_pcalloc(p, sizeof(oss_lifecycle_rule_content_t));
    aos_str_set(&rule->id, "");
    aos_str_set(&rule->prefix, "");
    aos_str_set(&rule->status, "");
    aos_str_set(&rule->date, "");
    rule->days = INT_MAX;
    return rule;
}

oss_upload_file_t *oss_create_upload_file(aos_pool_t *p)
{
    return (oss_upload_file_t *)aos_pcalloc(p, sizeof(oss_upload_file_t));
}

void oss_set_multipart_content_type(aos_table_t *headers)
{
    const char *content_type;
    content_type = (char*)(apr_table_get(headers, OSS_CONTENT_TYPE));
    content_type = content_type == NULL ? OSS_MULTIPART_CONTENT_TYPE : content_type;
    apr_table_set(headers, OSS_CONTENT_TYPE, content_type);
}

const char *get_oss_acl_str(oss_acl_e oss_acl)
{
    switch (oss_acl) {
        case OSS_ACL_PRIVATE:
            return  "private";
        case OSS_ACL_PUBLIC_READ:
            return "public-read";
        case OSS_ACL_PUBLIC_READ_WRITE:
            return "public-read-write";
        default:
            return NULL;
    }
}

void oss_init_request(const oss_request_options_t *options, http_method_e method,
    aos_http_request_t **req, aos_table_t *params, aos_table_t *headers, aos_http_response_t **resp)
{
    *req = aos_http_request_create(options->pool);
    *resp = aos_http_response_create(options->pool);
    (*req)->method = method;
    init_sts_token_header();
    (*req)->headers = headers;
    (*req)->query_params = params;
}

void oss_init_bucket_request(const oss_request_options_t *options, const aos_string_t *bucket,
    http_method_e method, aos_http_request_t **req, aos_table_t *params, aos_table_t *headers,
    aos_http_response_t **resp)
{
    oss_init_request(options, method, req, params, headers, resp);
    oss_get_bucket_uri(options, bucket, *req);
}

void oss_init_object_request(const oss_request_options_t *options, const aos_string_t *bucket,
    const aos_string_t *object, http_method_e method, aos_http_request_t **req, 
    aos_table_t *params, aos_table_t *headers, aos_http_response_t **resp)
{
    oss_init_request(options, method, req, params, headers, resp);
    oss_get_object_uri(options, bucket, object, *req);
}

void oss_init_signed_url_request(const oss_request_options_t *options, const aos_string_t *signed_url,
    http_method_e method, aos_http_request_t **req, aos_table_t *params, 
    aos_table_t *headers, aos_http_response_t **resp)
{
    oss_init_request(options, method, req, params, headers, resp);
    (*req)->signed_url = signed_url->data;
}

aos_status_t *oss_send_request(aos_http_controller_t *ctl, aos_http_request_t *req,
    aos_http_response_t *resp)
{
    aos_status_t *s;
    const char *reason;
    int res = AOSE_OK;


    s = aos_status_create(ctl->pool);
    res = aos_http_send_request(ctl, req, resp);

    if (res != AOSE_OK) {
        reason = aos_http_controller_get_reason(ctl);
        aos_status_set(s, res, AOS_HTTP_IO_ERROR_CODE, reason);
    } else if (!aos_http_is_ok(resp->status)) {
        s = aos_status_parse_from_body(ctl->pool, &resp->body, resp->status, s);
    } else {
        s->code = resp->status;
    }

	s->req_id = (char*)(apr_table_get(resp->headers, "x-oss-request-id"));
	if (s->req_id == NULL) {
		s->req_id = (char*)(apr_table_get(resp->headers, "x-img-request-id"));
		if (s->req_id == NULL) {
			s->req_id = "";
		}
	}

    return s;
}

aos_status_t *oss_process_request(const oss_request_options_t *options,
    aos_http_request_t *req, aos_http_response_t *resp)
{
    int res = AOSE_OK;
    aos_status_t *s;

    s = aos_status_create(options->pool);
    res = oss_sign_request(req, options->config);
    if (res != AOSE_OK) {
        aos_status_set(s, res, AOS_CLIENT_ERROR_CODE, NULL);
        return s;
    }

    return oss_send_request(options->ctl, req, resp);
}

aos_status_t *oss_process_signed_request(const oss_request_options_t *options,
    aos_http_request_t *req, aos_http_response_t *resp)
{
    return oss_send_request(options->ctl, req, resp);
}
