#include "aos_log.h"
#include "aos_define.h"
#include "aos_util.h"
#include "aos_string.h"
#include "aos_status.h"
#include "oss_auth.h"
#include "oss_util.h"
#include "oss_xml.h"
#include "oss_api.h"

aos_status_t *oss_create_bucket(const oss_request_options_t *options, 
    const aos_string_t *bucket, oss_acl_e oss_acl, aos_table_t **resp_headers)
{
    const char *oss_acl_str;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *headers;
    aos_table_t *query_params;

    //init query_params
    query_params = aos_table_make(options->pool, 0);

    //init headers
    headers = aos_table_make(options->pool, 1);
    oss_acl_str = get_oss_acl_str(oss_acl);
    if (oss_acl_str) {
        apr_table_set(headers, OSS_CANNONICALIZED_HEADER_ACL, oss_acl_str);
    }

    oss_init_bucket_request(options, bucket, HTTP_PUT, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_delete_bucket(const oss_request_options_t *options,
    const aos_string_t *bucket, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 0);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_bucket_request(options, bucket, HTTP_DELETE, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_get_bucket_acl(const oss_request_options_t *options, 
    const aos_string_t *bucket, aos_string_t *oss_acl, aos_table_t **resp_headers)
{
    aos_status_t *s;
    int res;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_ACL, "");

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_bucket_request(options, bucket, HTTP_GET, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_acl_parse_from_body(options->pool, &resp->body, oss_acl);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_list_object(const oss_request_options_t *options,
    const aos_string_t *bucket, oss_list_object_params_t *params, aos_table_t **resp_headers)
{
    int res;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 4);
    apr_table_add(query_params, OSS_PREFIX, params->prefix.data);
    apr_table_add(query_params, OSS_DELIMITER, params->delimiter.data);
    apr_table_add(query_params, OSS_MARKER, params->marker.data);
    aos_table_add_int(query_params, OSS_MAX_KEYS, params->max_ret);
    
    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_bucket_request(options, bucket, HTTP_GET, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_list_objects_parse_from_body(options->pool, &resp->body, &params->object_list, 
            &params->common_prefix_list, &params->next_marker, &params->truncated);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_put_bucket_lifecycle(const oss_request_options_t *options,
        const aos_string_t *bucket, aos_list_t *lifecycle_rule_list, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    apr_table_t *query_params;
    aos_table_t *headers;
    aos_list_t body;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_LIFECYCLE, "");

    //init headers
    headers = aos_table_make(options->pool, 5);

    oss_init_bucket_request(options, bucket, HTTP_PUT, &req, query_params, headers, &resp);

    build_lifecycle_body(options->pool, lifecycle_rule_list, &body);
    oss_write_request_body_from_buffer(&body, req);
    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_get_bucket_lifecycle(const oss_request_options_t *options,
        const aos_string_t *bucket, aos_list_t *lifecycle_rule_list, aos_table_t **resp_headers)
{
    int res;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_LIFECYCLE, "");

    //init headers
    headers = aos_table_make(options->pool, 5);

    oss_init_bucket_request(options, bucket, HTTP_GET, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_lifecycle_rules_parse_from_body(options->pool, &resp->body, lifecycle_rule_list);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_delete_bucket_lifecycle(const oss_request_options_t *options,
        const aos_string_t *bucket, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_LIFECYCLE, "");

    //init headers
    headers = aos_table_make(options->pool, 5);

    oss_init_bucket_request(options, bucket, HTTP_DELETE, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}
