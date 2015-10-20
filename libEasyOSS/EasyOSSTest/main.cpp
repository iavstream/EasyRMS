#include <libEasyOSS.h>
#include <string>
#include <stdio.h>
#include <sys/types.h>  
#include <sys/stat.h>  
#include <stdio.h>  
#include <errno.h> 

using namespace std;

void testUploadFile()
{
	
	EasyOSS_Initialize("easydarwin-easyrms-bucket", "oss-cn-hangzhou.aliyuncs.com", 80, "ayO28eQpxOntWuzV", "MJQD5mE27JCTIwBdrbofmSPjgDoAkG");
		
	string object_name = "dms.log";
	EasyOSS_Handle handle = EasyOSS_Open(object_name.c_str());
	const size_t READ_BUF_SIZE = 1024;

	struct _stat32 buf;
	if (_stat(object_name.c_str(), &buf) < 0)
	{
		printf("call stat Error : %s £¡", strerror(errno));
		return;
	}
	
	printf("testUploadFile begin...\n");

	size_t lSize = (buf.st_size < 0 ? 0 : (unsigned long)buf.st_size);

	FILE *fFile = fopen(object_name.c_str(), "rb");
	char sFileReadBuf[READ_BUF_SIZE] = { 0 };
	size_t nRead = 0;
	while (lSize > 0)
	{		
		nRead = fread(sFileReadBuf, 1, std::min(READ_BUF_SIZE, lSize), fFile);
		
		if (nRead <= 0)
		{
			printf("fread %s Failure£¡%s\n", object_name.c_str(), strerror(errno));
			break;
		}		EasyOSS_Write(handle, sFileReadBuf, nRead);
		lSize -= nRead;
	}	
	EasyOSS_Close(handle);
	fclose(fFile);
	printf("testUploadFile finish\n");
}

void testReadFile()
{
	EasyOSS_Initialize("easydarwin-easyrms-bucket", "oss-cn-hangzhou.aliyuncs.com", 80, "ayO28eQpxOntWuzV", "MJQD5mE27JCTIwBdrbofmSPjgDoAkG");

	string object_name = "dms.log";
	EasyOSS_Handle handle = EasyOSS_Open(object_name.c_str());
	
	const size_t READ_BUF_SIZE = 4096;	

	

	printf("testReadFile begin, size = %d...\n", EasyOSS_Size(handle));
		

	char *sFileReadBuf = new char[READ_BUF_SIZE*1024];
	remove(object_name.c_str());
	int nRead = 0;
	while ((nRead = EasyOSS_Read(handle, sFileReadBuf, READ_BUF_SIZE)) > 0)
	{
		FILE *fFile = fopen(object_name.c_str(), "ab+");	
		fwrite(sFileReadBuf, 1, nRead, fFile);
		fflush(fFile);
		fclose(fFile);		
		//break;
	}
	EasyOSS_Close(handle);
	delete[]sFileReadBuf;
	printf("testReadFile finish\n");
}

void testMakeDir()
{
	EasyOSS_Initialize("easydarwin-easyrms-bucket", "oss-cn-hangzhou.aliyuncs.com", 80, "ayO28eQpxOntWuzV", "MJQD5mE27JCTIwBdrbofmSPjgDoAkG");

	string object_name = "test_dir1/subdir.txt";
	EasyOSS_Handle handle = EasyOSS_Open(object_name.c_str());
	string buf = "asdfghjkl;12345678915665885556585555";
	//char buf[1024] = {0};

	//EasyOSS_Write(handle, NULL, 0); //empty dir
	EasyOSS_Write(handle, buf.data(), buf.size()); //subdir.txt in test_dir1


	//EasyOSS_Read(handle, buf, 1024);	
	EasyOSS_Close(handle);
}

int main(int argc, char* argv[])
{
	//EasyOSS_Initialize("easydarwin-easyrms-bucket", "oss-cn-hangzhou.aliyuncs.com", 80, "ayO28eQpxOntWuzV", "MJQD5mE27JCTIwBdrbofmSPjgDoAkG");

	//string object_name = "EasyDarwin_test_object_name";
	//EasyOSS_Handle handle = EasyOSS_Open(object_name.c_str());
	////string buf = "asdfghjkl;12345678915665885556585555";
	//char buf[1024] = {0};

	////EasyOSS_Write(handle, buf.data(), buf.size());

	////EasyOSS_Read(handle, buf, 1024);
	//printf("read buf = %s\n", buf);
	//EasyOSS_Close(handle);

	//testUploadFile();
	
	//testReadFile();

	testMakeDir();

	EasyOSS_Deinitialize();
	getchar();
}