#include "aos_string.h"
#include "aos_list.h"
#include "aos_buf.h"
#include "aos_util.h"
#include "aos_log.h"
#include "aos_status.h"
#include "oss_util.h"
#include "oss_auth.h"
#include "oss_xml.h"

#include "oss_define.h"

static int get_truncated_from_xml(aos_pool_t *p, xmlDocPtr doc, const char *truncated_xml_path);

static int get_truncated_from_xml(aos_pool_t *p, xmlDocPtr doc, const char *truncated_xml_path)
{
    char *is_truncated;
    int truncated = 0;
    is_truncated = get_xmlnode_value(p, doc, truncated_xml_path);
    if (is_truncated) {
        truncated = strcasecmp(is_truncated, "false") == 0 ? 0 : 1;
    }
    return truncated;
}

int get_xmldoc(aos_list_t *bc, xmlDocPtr *doc)
{
    int res;
    xmlNodePtr root;

    if (aos_list_empty(bc)) {
        return AOSE_XML_PARSE_ERROR;
    }

    if ((res = aos_parse_xml_body(bc, doc, &root)) != AOSE_OK) {
        return AOSE_XML_PARSE_ERROR;
    }

    return AOSE_OK;
}

xmlXPathObjectPtr get_nodeset(xmlDocPtr doc, xmlChar *xpath)
{
    xmlXPathContextPtr context;
    xmlXPathObjectPtr result;

    context = xmlXPathNewContext(doc);
    if(NULL == context) {
        aos_debug_log("Error in xmlXPathNewContent\n");
        return NULL;
    }

    result = xmlXPathEvalExpression(xpath, context);
    xmlXPathFreeContext(context);
    if(NULL == result) {
        aos_debug_log("Error in xmlXPathEvalExpression\n");
        return NULL;
    }

    if(xmlXPathNodeSetIsEmpty(result->nodesetval)) {
        xmlXPathFreeObject(result);
        return NULL;
    }

    return result;
}

char *get_xmlnode_value(aos_pool_t *p, xmlDocPtr doc, const char *xml_path)
{
    char *value = NULL;
    xmlChar *xpath;
    xmlXPathObjectPtr result;
    xmlNodeSetPtr nodeset;
    xmlNodePtr cur_node;
    xmlChar *node_content;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);

    if(result) {
        nodeset = result->nodesetval;
        cur_node = nodeset->nodeTab[0]->xmlChildrenNode;
        node_content = xmlNodeGetContent(cur_node);
        value = apr_pstrdup(p, (char *)node_content);
        xmlFree(node_content);
        xmlXPathFreeObject(result);
    }
    
    return value;
}

int oss_acl_parse_from_body(aos_pool_t *p, aos_list_t *bc, aos_string_t *oss_acl)
{
    int res;
    xmlDocPtr doc = NULL;
    const char xml_path[] = "/AccessControlPolicy/AccessControlList/Grant";
    char *acl;

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        acl = get_xmlnode_value(p, doc, xml_path);
        if (acl) {
            aos_str_set(oss_acl, acl);
        }
        xmlFreeDoc(doc);
    }

    return res;
}

void oss_list_objects_owner_parse(aos_pool_t *p, xmlNodePtr xml_node, oss_list_object_content_t *content)
{
    xmlNodePtr owner_node;
    char *owner_id;
    char *owner_display_name;

    for (owner_node = xml_node->xmlChildrenNode; owner_node != NULL; owner_node = owner_node->next) {
        xmlChar *node_content = xmlNodeGetContent(owner_node);
        if (apr_strnatcasecmp((char *)owner_node->name, "ID") == 0) {
            owner_id = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->owner_id, owner_id);
        } else if (apr_strnatcasecmp((char *)owner_node->name, "DisplayName") == 0) {
            owner_display_name = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->owner_display_name, owner_display_name);
        }
        xmlFree(node_content);
    }
}

void oss_list_objects_content_parse(aos_pool_t *p, xmlNodePtr xml_node, oss_list_object_content_t *content)
{
    char *key;
    char *last_modified;
    char *etag;
    char *size;

    while (xml_node) {
        xmlChar *node_content = xmlNodeGetContent(xml_node);
        if (apr_strnatcasecmp((char *)xml_node->name, "Key") == 0) {
            key = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->key, key);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "LastModified") == 0) {
            last_modified = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->last_modified, last_modified);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "ETag") == 0) {
            etag = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->etag, etag);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Size") == 0) {
            size = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->size, size);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Owner") == 0) {
            oss_list_objects_owner_parse(p, xml_node, content);
        }
        xmlFree(node_content);
        xml_node = xml_node->next;
    }
}

void oss_list_objects_contents_parse(aos_pool_t *p, xmlDocPtr doc, const char *xml_path,
    aos_list_t *object_list)
{
    int i = 0;
    xmlNodePtr cur_node;
    xmlNodeSetPtr nodeset = NULL;
    oss_list_object_content_t *content;
    xmlChar *xpath;
    xmlXPathObjectPtr result;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);
    if (result == NULL)  {
        return;
    }

    nodeset = result->nodesetval;

    for ( ; i < nodeset->nodeNr; ++i) {
        content = oss_create_list_object_content(p);
        cur_node = nodeset->nodeTab[i]->xmlChildrenNode;
        oss_list_objects_content_parse(p, cur_node, content);
        aos_list_add_tail(&content->node, object_list);
    }
    xmlXPathFreeObject(result);
}

void oss_list_objects_prefix_parse(aos_pool_t *p, xmlNodePtr prefix_node, oss_list_object_common_prefix_t *common_prefix)
{
    char *prefix;

    for (; prefix_node != NULL; prefix_node = prefix_node->next) {
        xmlChar *node_content = xmlNodeGetContent(prefix_node);
        if (apr_strnatcasecmp((char *)prefix_node->name, "Prefix") == 0) {
            prefix = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&common_prefix->prefix, prefix);
        }
        xmlFree(node_content);
    }
}

void oss_list_objects_common_prefix_parse(aos_pool_t *p, xmlDocPtr doc, const char *xml_path,
            aos_list_t *common_prefix_list)
{
    int i = 0;
    xmlNodePtr cur_node;
    xmlNodeSetPtr nodeset = NULL;
    oss_list_object_common_prefix_t *common_prefix;
    xmlChar *xpath;
    xmlXPathObjectPtr result;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);
    if (result == NULL)  {
        return;
    }

    nodeset = result->nodesetval;

    for ( ; i < nodeset->nodeNr; ++i) {
        common_prefix = oss_create_list_object_common_prefix(p);
        cur_node = nodeset->nodeTab[i]->xmlChildrenNode;
        oss_list_objects_prefix_parse(p, cur_node, common_prefix);
        aos_list_add_tail(&common_prefix->node, common_prefix_list);
    }
    xmlXPathFreeObject(result);
}

int oss_list_objects_parse_from_body(aos_pool_t *p, aos_list_t *bc,
    aos_list_t *object_list, aos_list_t *common_prefix_list, aos_string_t *marker, int *truncated)
{
    int res;
    xmlDocPtr doc = NULL;
    const char next_marker_xml_path[] = "/ListBucketResult/NextMarker";
    const char truncated_xml_path[] = "/ListBucketResult/IsTruncated";
    const char buckets_xml_path[] = "/ListBucketResult/Contents";
	const char common_prefix_xml_path[] = "/ListBucketResult/CommonPrefixes";
    char* next_marker;

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        next_marker = get_xmlnode_value(p, doc, next_marker_xml_path);
        if (next_marker) {
            aos_str_set(marker, next_marker);
        }

        *truncated = get_truncated_from_xml(p, doc, truncated_xml_path);
        
        oss_list_objects_contents_parse(p, doc, buckets_xml_path, object_list);
		oss_list_objects_common_prefix_parse(p, doc, common_prefix_xml_path, common_prefix_list);
        xmlFreeDoc(doc);
    }
    
    return res;
}

int oss_upload_id_parse_from_body(aos_pool_t *p, aos_list_t *bc, aos_string_t *upload_id)
{
    int res;
    xmlDocPtr doc;
    const char xml_path[] = "/InitiateMultipartUploadResult/UploadId";
    char *id;

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        id = get_xmlnode_value(p, doc, xml_path);
        xmlFreeDoc(doc);
        if (id) {
            aos_str_set(upload_id, id);
        }
    }

    return res;
}

void oss_list_parts_contents_parse(aos_pool_t *p, xmlDocPtr doc, const char *xml_path, 
    aos_list_t *part_list)
{
    int i = 0;
    xmlNodePtr cur_node;
    xmlNodeSetPtr nodeset = NULL;
    oss_list_part_content_t *content;
    xmlChar *xpath;
    xmlXPathObjectPtr result;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);
    if (result == NULL)  {
        return;
    }

    nodeset = result->nodesetval;

    for ( ; i < nodeset->nodeNr; ++i) {
        content = oss_create_list_part_content(p);
        cur_node = nodeset->nodeTab[i]->xmlChildrenNode;
        oss_list_parts_content_parse(p, cur_node, content);
        aos_list_add_tail(&content->node, part_list);
    }

    xmlXPathFreeObject(result);    
}

void oss_list_parts_content_parse(aos_pool_t *p, xmlNodePtr xml_node, oss_list_part_content_t *content)
{
    char *part_number;
    char *last_modified;
    char *etag;
    char *size;

    while (xml_node) {
        xmlChar *node_content = xmlNodeGetContent(xml_node);
        if (apr_strnatcasecmp((char *)xml_node->name, "PartNumber") == 0) {
            part_number = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->part_number, part_number);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "LastModified") == 0) {
            last_modified = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->last_modified, last_modified);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "ETag") == 0) {
            etag = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->etag, etag);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Size") == 0) {
            size = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->size, size);
        }
        xmlFree(node_content);
        xml_node = xml_node->next;
    }
}

int oss_list_parts_parse_from_body(aos_pool_t *p, aos_list_t *bc,
    aos_list_t *part_list, aos_string_t *partnumber_marker, int *truncated)
{
    int res;
    xmlDocPtr doc;
    const char next_partnumber_marker_xml_path[] = "/ListPartsResult/NextPartNumberMarker";
    const char truncated_xml_path[] = "/ListPartsResult/IsTruncated";
    const char parts_xml_path[] = "/ListPartsResult/Part";
    char *next_partnumber_marker;

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        next_partnumber_marker = get_xmlnode_value(p, doc, next_partnumber_marker_xml_path);
        if (next_partnumber_marker) {
            aos_str_set(partnumber_marker, next_partnumber_marker);
        }

        *truncated = get_truncated_from_xml(p, doc, truncated_xml_path);

        oss_list_parts_contents_parse(p, doc, parts_xml_path, part_list);

        xmlFreeDoc(doc);
    }

    return res;
}

void oss_list_multipart_uploads_contents_parse(aos_pool_t *p, xmlDocPtr doc, const char *xml_path,
    aos_list_t *upload_list)
{
    int i = 0;
    xmlNodePtr cur_node;
    xmlNodeSetPtr nodeset = NULL;
    oss_list_multipart_upload_content_t *content;
    xmlChar *xpath;
    xmlXPathObjectPtr result;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);
    if (result == NULL)  {
        return;
    }

    nodeset = result->nodesetval;

    for ( ; i < nodeset->nodeNr; ++i) {
        content = oss_create_list_multipart_upload_content(p);
        cur_node = nodeset->nodeTab[i]->xmlChildrenNode;
        oss_list_multipart_uploads_content_parse(p, cur_node, content);
        aos_list_add_tail(&content->node, upload_list);
    }

    xmlXPathFreeObject(result);
}

void oss_list_multipart_uploads_content_parse(aos_pool_t *p, xmlNodePtr xml_node, 
    oss_list_multipart_upload_content_t *content)
{
    char *key;
    char *upload_id;
    char *initiated;

    while (xml_node) {
        xmlChar *node_content = xmlNodeGetContent(xml_node);
        if (apr_strnatcasecmp((char *)xml_node->name, "Key") == 0) {
            key = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->key, key);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "UploadId") == 0) {
            upload_id = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->upload_id, upload_id);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Initiated") == 0) {
            initiated = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->initiated, initiated);
        }
        xmlFree(node_content);
        xml_node = xml_node->next;
    } 
}

int oss_list_multipart_uploads_parse_from_body(aos_pool_t *p, aos_list_t *bc,
    aos_list_t *upload_list, aos_string_t *key_marker,
    aos_string_t *upload_id_marker, int *truncated)
{
    int res;
    xmlDocPtr doc;
    const char next_key_marker_xml_path[] = "/ListMultipartUploadsResult/NextKeyMarker";
    const char next_upload_id_marker_xml_path[] = "/ListMultipartUploadsResult/NextUploadIdMarker";
    const char truncated_xml_path[] = "/ListMultipartUploadsResult/IsTruncated";
    const char uploads_xml_path[] = "/ListMultipartUploadsResult/Upload";
    char *next_key_marker;
    char *next_upload_id_marker;

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        next_key_marker = get_xmlnode_value(p, doc, next_key_marker_xml_path);
        if (next_key_marker) {
            aos_str_set(key_marker, next_key_marker);
        }

        next_upload_id_marker = get_xmlnode_value(p, doc, next_upload_id_marker_xml_path);
        if (next_upload_id_marker) {
            aos_str_set(upload_id_marker, next_upload_id_marker);
        }

        *truncated = get_truncated_from_xml(p, doc, truncated_xml_path);

        oss_list_multipart_uploads_contents_parse(p, doc, uploads_xml_path, upload_list);
        xmlFreeDoc(doc);
    }

    return res;
}

char *build_complete_multipart_upload_xml(aos_pool_t *p, aos_list_t *bc)
{
    xmlChar *xml_buff;
    char *complete_part_xml;
    int size = 0;
    aos_string_t xml_doc;
    oss_complete_part_content_t *content;
    xmlDocPtr doc;
    xmlNodePtr root_node;
    xmlNodePtr part_node;

    doc = xmlNewDoc(BAD_CAST BAD_CAST"1.0");
    root_node = xmlNewNode(NULL, BAD_CAST "CompleteMultipartUpload");
    xmlDocSetRootElement(doc,root_node);

    aos_list_for_each_entry(oss_complete_part_content_t, content, bc, node) {
        part_node = xmlNewTextChild(root_node, NULL, BAD_CAST "Part", NULL);
        xmlNewTextChild(part_node, NULL, BAD_CAST "PartNumber", BAD_CAST content->part_number.data);
        xmlNewTextChild(part_node, NULL, BAD_CAST "ETag", BAD_CAST content->etag.data);
    }

    xmlDocDumpMemoryEnc(doc, &xml_buff, &size, "UTF-8");
    aos_str_set(&xml_doc, (char*)xml_buff);
    complete_part_xml = aos_pstrdup(p, &xml_doc);
    xmlFreeDoc(doc);
    xmlFree(xml_buff);
    return complete_part_xml;
}

void build_complete_multipart_upload_body(aos_pool_t *p, aos_list_t *part_list, aos_list_t *body)
{
    char *complete_multipart_upload_xml;
    aos_buf_t *b;

    complete_multipart_upload_xml = build_complete_multipart_upload_xml(p, part_list);
    aos_list_init(body);
    b = aos_buf_pack(p, complete_multipart_upload_xml, strlen(complete_multipart_upload_xml));
    aos_list_add_tail(&b->node, body);
}

char *build_lifecycle_xml(aos_pool_t *p, aos_list_t *lifecycle_rule_list)
{
    xmlChar *xml_buff;
    char *lifecycle_xml;
    int size = 0;
    aos_string_t xml_doc;
    oss_lifecycle_rule_content_t *content;
    xmlDocPtr doc;
    xmlNodePtr root_node;
    xmlNodePtr rule_node;
    xmlNodePtr expire_node;

    doc = xmlNewDoc(BAD_CAST BAD_CAST"1.0");
    root_node = xmlNewNode(NULL, BAD_CAST "LifecycleConfiguration");
    xmlDocSetRootElement(doc,root_node);

    aos_list_for_each_entry(oss_lifecycle_rule_content_t, content, lifecycle_rule_list, node) {
        rule_node = xmlNewTextChild(root_node, NULL, BAD_CAST "Rule", NULL);
        xmlNewTextChild(rule_node, NULL, BAD_CAST "ID", BAD_CAST content->id.data);
        xmlNewTextChild(rule_node, NULL, BAD_CAST "Prefix", BAD_CAST content->prefix.data);
        xmlNewTextChild(rule_node, NULL, BAD_CAST "Status", BAD_CAST content->status.data);
        expire_node = xmlNewTextChild(rule_node, NULL, BAD_CAST "Expiration", NULL);
        if (content->days != INT_MAX) {
            char value_str[64];
            snprintf(value_str, sizeof(value_str), "%d", content->days);
            xmlNewTextChild(expire_node, NULL, BAD_CAST "Days", BAD_CAST value_str);
        }
        if (content->date.data != "")
        {
            xmlNewTextChild(expire_node, NULL, BAD_CAST "Date", BAD_CAST content->date.data);
        }
    }

    xmlDocDumpMemoryEnc(doc, &xml_buff, &size, "UTF-8");
    aos_str_set(&xml_doc, (char*)xml_buff);
    lifecycle_xml = aos_pstrdup(p, &xml_doc);
    xmlFreeDoc(doc);
    xmlFree(xml_buff);
	    return lifecycle_xml;
}

void build_lifecycle_body(aos_pool_t *p, aos_list_t *lifecycle_rule_list, aos_list_t *body)
{
    char *lifecycle_xml;
    aos_buf_t *b;
    lifecycle_xml = build_lifecycle_xml(p, lifecycle_rule_list);
    aos_list_init(body);
    b = aos_buf_pack(p, lifecycle_xml, strlen(lifecycle_xml));
    aos_list_add_tail(&b->node, body);
}

int oss_lifecycle_rules_parse_from_body(aos_pool_t *p, aos_list_t *bc, aos_list_t *lifecycle_rule_list)
{
    int res;
    xmlDocPtr doc;
    const char rule_xml_path[] = "/LifecycleConfiguration/Rule";

    res = get_xmldoc(bc, &doc);
    if (res == AOSE_OK) {
        oss_lifecycle_rule_contents_parse(p, doc, rule_xml_path, lifecycle_rule_list);
        xmlFreeDoc(doc);
    }

    return res;
}

void oss_lifecycle_rule_contents_parse(aos_pool_t *p, xmlDocPtr doc, const char *xml_path,
    aos_list_t *lifecycle_rule_list)
{
    int i = 0;
    xmlNodePtr cur_node;
    xmlNodeSetPtr nodeset = NULL;
    oss_lifecycle_rule_content_t *content;
    xmlChar *xpath;
    xmlXPathObjectPtr result;

    xpath = (xmlChar*) xml_path;
    result = get_nodeset(doc, xpath);
    if (result == NULL)  {
        return;
    }

    nodeset = result->nodesetval;

    for ( ; i < nodeset->nodeNr; ++i) {
        content = oss_create_lifecycle_rule_content(p);
        cur_node = nodeset->nodeTab[i]->xmlChildrenNode;
        oss_lifecycle_rule_content_parse(p, cur_node, content);
        aos_list_add_tail(&content->node, lifecycle_rule_list);
    }

    xmlXPathFreeObject(result);
}

void oss_lifecycle_rule_content_parse(aos_pool_t *p, xmlNodePtr xml_node,
    oss_lifecycle_rule_content_t *content)
{
    char *id;
    char *prefix;
    char *status;

    while (xml_node) {
        xmlChar *node_content = xmlNodeGetContent(xml_node);
        if (apr_strnatcasecmp((char *)xml_node->name, "ID") == 0) {
            id = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->id, id);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Prefix") == 0) {
            prefix = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->prefix, prefix);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Status") == 0) {
            status = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->status, status);
        } else if (apr_strnatcasecmp((char *)xml_node->name, "Expiration") == 0) {
            oss_lifecycle_rule_expire_parse(p, xml_node, content);
        }
        xmlFree(node_content);
        xml_node = xml_node->next;
    }
}

void oss_lifecycle_rule_expire_parse(aos_pool_t *p, xmlNodePtr xml_node,
    oss_lifecycle_rule_content_t *content)
{
    xmlNodePtr expire_node;
    char* days;
    char *date;

    for (expire_node = xml_node->xmlChildrenNode; expire_node != NULL; expire_node = expire_node->next) {
        xmlChar *node_content = xmlNodeGetContent(expire_node);
        if (apr_strnatcasecmp((char *)expire_node->name, "Days") == 0) {
            days = apr_pstrdup(p, (char *)node_content);
            content->days = atoi(days);
        } else if (apr_strnatcasecmp((char *)expire_node->name, "Date") == 0) {
            date = apr_pstrdup(p, (char *)node_content);
            aos_str_set(&content->date, date);
        }
        xmlFree(node_content);
    }
}
