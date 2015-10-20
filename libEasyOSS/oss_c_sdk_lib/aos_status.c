#include "aos_log.h"
#include "aos_util.h"
#include "aos_status.h"

const char AOS_XML_PARSE_ERROR_CODE[] = "ParseXmlError";
const char AOS_OPEN_FILE_ERROR_CODE[] = "OpenFileFail";
const char AOS_HTTP_IO_ERROR_CODE[] = "HttpIoError";
const char AOS_UNKNOWN_ERROR_CODE[] = "UnkonwnError";
const char AOS_CLIENT_ERROR_CODE[] = "ClientError";
const char AOS_UTF8_ENCODE_ERROR_CODE[] = "Utf8EncodeFail";

aos_status_t *aos_status_create(aos_pool_t *p)
{
    return (aos_status_t *)aos_pcalloc(p, sizeof(aos_status_t));
}

aos_status_t *aos_status_parse_from_body(aos_pool_t *p, aos_list_t *bc, int code, aos_status_t *s)
{
    int res;
    xmlDoc *doc;
    xmlNode *root;
    xmlNode *cur_node;

    if (s == NULL) {
        s = aos_status_create(p);
    }
    s->code = code;

    if (aos_http_is_ok(code)) {
        return s;
    }

    if (aos_list_empty(bc)) {
        s->error_code = (char *)AOS_UNKNOWN_ERROR_CODE;
        return s;
    }

    if ((res = aos_parse_xml_body(bc, &doc, &root)) != AOSE_OK) {
        s->error_code = (char *)AOS_UNKNOWN_ERROR_CODE;
        return s;
    }

    if (apr_strnatcasecmp((char *)root->name, "Error") != 0) {
        aos_error_log("Xml format invalid, root node name: %s.", (char *)root->name);
        s->error_code = (char *)AOS_UNKNOWN_ERROR_CODE;
        xmlFreeDoc(doc);
        return s;
    }
    
    for (cur_node = root->children; cur_node != NULL; cur_node = cur_node->next) {
        if (cur_node->type == XML_ELEMENT_NODE) {
            xmlChar *node_content = xmlNodeGetContent(cur_node);
            if (apr_strnatcasecmp((char *)cur_node->name, "Code") == 0) {
                s->error_code = apr_pstrdup(p, (char *)node_content);
            } else if (apr_strnatcasecmp((char *)cur_node->name, "Message") == 0) {
                s->error_msg = apr_pstrdup(p, (char *)node_content);
            }
            xmlFree(node_content);
        }
    }

    xmlFreeDoc(doc);
    return s;
}

