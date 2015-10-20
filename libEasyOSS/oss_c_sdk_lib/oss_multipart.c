#include "aos_log.h"
#include "aos_define.h"
#include "aos_util.h"
#include "aos_string.h"
#include "aos_status.h"
#include "oss_auth.h"
#include "oss_util.h"
#include "oss_xml.h"
#include "oss_api.h"

aos_status_t *oss_init_multipart_upload(const oss_request_options_t *options, 
    const aos_string_t *bucket, const aos_string_t *object, 
    aos_table_t *headers, aos_string_t *upload_id, aos_table_t **resp_headers)
{
    int res;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_UPLOADS, "");

    //init headers
    oss_set_multipart_content_type(headers);

    oss_init_object_request(options, bucket, object, HTTP_POST, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_upload_id_parse_from_body(options->pool, &resp->body, upload_id);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_abort_multipart_upload(const oss_request_options_t *options, 
    const aos_string_t *bucket, const aos_string_t *object, 
    aos_string_t *upload_id, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_UPLOAD_ID, upload_id->data);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_object_request(options, bucket, object, HTTP_DELETE, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_list_upload_part(const oss_request_options_t *options, 
    const aos_string_t *bucket, const aos_string_t *object, 
    const aos_string_t *upload_id, oss_list_upload_part_params_t *params,
    aos_table_t **resp_headers)
{
    int res;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 3);
    apr_table_add(query_params, OSS_UPLOAD_ID, upload_id->data);
    aos_table_add_int(query_params, OSS_MAX_PARTS, params->max_ret);
    apr_table_add(query_params, OSS_PART_NUMBER_MARKER, params->part_number_marker.data);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_object_request(options, bucket, object, HTTP_GET, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_list_parts_parse_from_body(options->pool, &resp->body, 
        &params->part_list, &params->next_part_number_marker, &params->truncated);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_list_multipart_upload(const oss_request_options_t *options, 
    const aos_string_t *bucket, oss_list_multipart_upload_params_t *params, 
    aos_table_t **resp_headers)
{
    int res;
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 6);
    apr_table_add(query_params, OSS_UPLOADS, "");
    apr_table_add(query_params, OSS_PREFIX, params->prefix.data);
    apr_table_add(query_params, OSS_DELIMITER, params->delimiter.data);
    apr_table_add(query_params, OSS_KEY_MARKER, params->key_marker.data);
    apr_table_add(query_params, OSS_UPLOAD_ID_MARKER, params->upload_id_marker.data);
    aos_table_add_int(query_params, OSS_MAX_UPLOADS, params->max_ret);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_bucket_request(options, bucket, HTTP_GET, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;
    if (!aos_status_is_ok(s)) {
        return s;
    }

    res = oss_list_multipart_uploads_parse_from_body(options->pool, &resp->body, &params->upload_list, 
        &params->next_key_marker, &params->next_upload_id_marker, &params->truncated);
    if (res != AOSE_OK) {
        aos_xml_error_status_set(s, res);
    }

    return s;
}

aos_status_t *oss_complete_multipart_upload(const oss_request_options_t *options, 
    const aos_string_t *bucket, const aos_string_t *object, 
    const aos_string_t *upload_id, aos_list_t *part_list, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    apr_table_t *query_params;
    aos_table_t *headers;
    aos_list_t body;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_UPLOAD_ID, upload_id->data);

    //init headers
    headers = aos_table_make(options->pool, 1);
    apr_table_set(headers, OSS_CONTENT_TYPE, OSS_MULTIPART_CONTENT_TYPE);

    oss_init_object_request(options, bucket, object, HTTP_POST, &req, query_params, headers, &resp);

    build_complete_multipart_upload_body(options->pool, part_list, &body);
    oss_write_request_body_from_buffer(&body, req);
   
    s = oss_process_request(options, req, resp); 
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_upload_part_from_buffer(const oss_request_options_t *options, 
    const aos_string_t *bucket, const aos_string_t *object, 
    const aos_string_t *upload_id, int part_num, aos_list_t *buffer, 
    aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    aos_table_t *headers;

    //init query_params
    query_params = aos_table_make(options->pool, 1);
    apr_table_add(query_params, OSS_UPLOAD_ID, upload_id->data);
    aos_table_add_int(query_params, OSS_PARTNUMBER, part_num);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_object_request(options, bucket, object, HTTP_PUT, &req, query_params, headers, &resp);

    oss_write_request_body_from_buffer(buffer, req);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_upload_part_from_file(const oss_request_options_t *options,
    const aos_string_t *bucket, const aos_string_t *object,
    const aos_string_t *upload_id, int part_num, oss_upload_file_t *upload_file,
    aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp; 
    aos_table_t *query_params;
    aos_table_t *headers;
    int res = AOSE_OK;

    s = aos_status_create(options->pool);

    //init query_params
    query_params = aos_table_make(options->pool, 2);
    apr_table_add(query_params, OSS_UPLOAD_ID, upload_id->data);
    aos_table_add_int(query_params, OSS_PARTNUMBER, part_num);

    //init headers
    headers = aos_table_make(options->pool, 0);

    oss_init_object_request(options, bucket, object, HTTP_PUT, &req, query_params, headers, &resp);

    res = oss_write_request_body_from_upload_file(options->pool, upload_file, req);
    if (res != AOSE_OK) {
        aos_file_error_status_set(s, res);
        return s;
    }

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}

aos_status_t *oss_upload_part_copy(const oss_request_options_t *options,
    oss_upload_part_copy_params_t *params, aos_table_t *headers, aos_table_t **resp_headers)
{
    aos_status_t *s;
    aos_http_request_t *req;
    aos_http_response_t *resp;
    aos_table_t *query_params;
    char *copy_source;
    char *copy_source_range;
    int res = AOSE_OK;

    s = aos_status_create(options->pool);

    //init query_params
    query_params = aos_table_make(options->pool, 2);
    apr_table_add(query_params, OSS_UPLOAD_ID, params->upload_id.data);
    aos_table_add_int(query_params, OSS_PARTNUMBER, params->part_num);

    //init headers
    headers = aos_table_make(options->pool, 2);
    copy_source = apr_psprintf(options->pool, "/%.*s/%.*s", 
        params->source_bucket.len, params->source_bucket.data, 
        params->source_object.len, params->source_object.data);
    apr_table_add(headers, OSS_COPY_SOURCE, copy_source);
    copy_source_range = apr_psprintf(options->pool, "bytes=%" APR_INT64_T_FMT "-%" APR_INT64_T_FMT,
        params->range_start, params->range_end);
    apr_table_add(headers, OSS_COPY_SOURCE_RANGE, copy_source_range);

    oss_init_object_request(options, &params->dest_bucket, &params->dest_object, HTTP_PUT, &req, query_params, headers, &resp);

    s = oss_process_request(options, req, resp);
    *resp_headers = resp->headers;

    return s;
}
