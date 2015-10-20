#ifndef LIB_EASY_OSS_H
#define LIB_EASY_OSS_H

#include <stdio.h>

typedef void* EasyOSS_Handle;

int EasyOSS_Initialize(const char* bucket_name, const char* oss_endpoint, size_t oss_port, const char* access_key_id, const char* access_key_secret);

EasyOSS_Handle EasyOSS_Open(const char* object_name);

int EasyOSS_Write(EasyOSS_Handle handle, const void* data, size_t size);

int EasyOSS_Read(EasyOSS_Handle handle, void* data, size_t size);

unsigned long EasyOSS_Size(EasyOSS_Handle handle);

int EasyOSS_Delete(EasyOSS_Handle handle);

int EasyOSS_Close(EasyOSS_Handle handle);

void EasyOSS_Deinitialize();

#endif

