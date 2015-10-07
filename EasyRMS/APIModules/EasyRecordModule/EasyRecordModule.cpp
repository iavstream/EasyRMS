/*
	Copyright (c) 2013-2015 EasyDarwin.ORG.  All rights reserved.
	Github: https://github.com/EasyDarwin
	WEChat: EasyDarwin
	Website: http://www.easydarwin.org
*/
/*
    File:       EasyRecordModule.cpp
    Contains:   Implementation of EasyRecordModule class. 
*/

#ifndef __Win32__
// For gethostbyname
#include <netdb.h>
#endif

#include "EasyRecordModule.h"
#include "QTSSModuleUtils.h"
#include "OSArrayObjectDeleter.h"
#include "OSMemory.h"
#include "QTSSMemoryDeleter.h"
#include "QueryParamList.h"
#include "OSRef.h"
#include "StringParser.h"
#include "EasyRecordSession.h"

#include "QTSServerInterface.h"

class RecordSessionCheckingTask : public Task
{
    public:
        // Task that just polls on all the sockets in the pool, sending data on all available sockets
        RecordSessionCheckingTask() : Task() {this->SetTaskName("RecordSessionCheckingTask");  this->Signal(Task::kStartEvent); }
        virtual ~RecordSessionCheckingTask() {}
    
    private:
        virtual SInt64 Run();
        
        enum
        {
            kProxyTaskPollIntervalMsec = 60*1000
        };
};

// STATIC DATA
static QTSS_PrefsObject         sServerPrefs		= NULL;
static RecordSessionCheckingTask*	sCheckingTask		= NULL;

static QTSS_ServerObject sServer					= NULL;
static QTSS_ModulePrefsObject       sModulePrefs	= NULL;

static StrPtrLen	sHLSSuffix("EasyRecordModule");

#define QUERY_STREAM_NAME	"name"
#define QUERY_STREAM_URL	"url"
#define QUERY_STREAM_CMD	"cmd"
#define QUERY_STREAM_CMD_START "start"
#define QUERY_STREAM_CMD_STOP "stop"

// FUNCTION PROTOTYPES
static QTSS_Error EasyHLSModuleDispatch(QTSS_Role inRole, QTSS_RoleParamPtr inParams);
static QTSS_Error Register(QTSS_Register_Params* inParams);
static QTSS_Error Initialize(QTSS_Initialize_Params* inParams);

static QTSS_Error RereadPrefs();

static QTSS_Error EasyHLSOpen(Easy_RecordOpen_Params* inParams);
static QTSS_Error EasyHLSClose(Easy_RecordClose_Params* inParams);
static char* GetHLSUrl(char* inSessionName);

// FUNCTION IMPLEMENTATIONS
QTSS_Error EasyRecordModule_Main(void* inPrivateArgs)
{
    return _stublibrary_main(inPrivateArgs, EasyHLSModuleDispatch);
}

QTSS_Error  EasyHLSModuleDispatch(QTSS_Role inRole, QTSS_RoleParamPtr inParams)
{
    switch (inRole)
    {
        case QTSS_Register_Role:
            return Register(&inParams->regParams);
        case QTSS_Initialize_Role:
            return Initialize(&inParams->initParams);
        case QTSS_RereadPrefs_Role:
            return RereadPrefs();
		case Easy_RecordOpen_Role:		//Start HLS Streaming
			return EasyHLSOpen(&inParams->easyRecordOpenParams);
		case Easy_RecordClose_Role:	//Stop HLS Streaming
			return EasyHLSClose(&inParams->easyRecordCloseParams);
    }
    return QTSS_NoErr;
}

QTSS_Error Register(QTSS_Register_Params* inParams)
{
    // Do role & attribute setup
    (void)QTSS_AddRole(QTSS_Initialize_Role);
    (void)QTSS_AddRole(QTSS_RereadPrefs_Role);   
    (void)QTSS_AddRole(Easy_RecordOpen_Role); 
	(void)QTSS_AddRole(Easy_RecordClose_Role); 
    
    // Tell the server our name!
    static char* sModuleName = "EasyRecordModule";
    ::strcpy(inParams->outModuleName, sModuleName);

    return QTSS_NoErr;
}

QTSS_Error Initialize(QTSS_Initialize_Params* inParams)
{
    // Setup module utils
    QTSSModuleUtils::Initialize(inParams->inMessages, inParams->inServer, inParams->inErrorLogStream);

    // Setup global data structures
    sServerPrefs = inParams->inPrefs;
    sCheckingTask = NEW RecordSessionCheckingTask();

    sServer = inParams->inServer;
    
    sModulePrefs = QTSSModuleUtils::GetModulePrefsObject(inParams->inModule);

    // Call helper class initializers
    EasyRecordSession::Initialize(sModulePrefs);

	RereadPrefs();

    return QTSS_NoErr;
}

QTSS_Error RereadPrefs()
{
	return QTSS_NoErr;
}


SInt64 RecordSessionCheckingTask::Run()
{
	return kProxyTaskPollIntervalMsec;
}

QTSS_Error EasyHLSOpen(Easy_RecordOpen_Params* inParams)
{	
	OSRefTable* sHLSSessionMap =  QTSServerInterface::GetServer()->GetRecordSessionMap();

	OSMutexLocker locker (sHLSSessionMap->GetMutex());

	EasyRecordSession* session = NULL;
	//首先查找MAP里面是否已经有了对应的流
	StrPtrLen streamName(inParams->inStreamName);
	OSRef* clientSesRef = sHLSSessionMap->Resolve(&streamName);
	if(clientSesRef != NULL)
	{
		session = (EasyRecordSession*)clientSesRef->GetObject();
	}
	else
	{
		session = NEW EasyRecordSession(&streamName);

		OS_Error theErr = sHLSSessionMap->Register(session->GetRef());
		Assert(theErr == QTSS_NoErr);

		//增加一次对RelaySession的无效引用，后面会统一释放
		OSRef* debug = sHLSSessionMap->Resolve(&streamName);
		Assert(debug == session->GetRef());
	}
	
	//到这里，肯定是有一个EasyRecordSession可用的
	session->HLSSessionStart(inParams->inRTSPUrl, inParams->inTimeout);

	sHLSSessionMap->Release(session->GetRef());

	return QTSS_NoErr;
}

QTSS_Error EasyHLSClose(Easy_RecordClose_Params* inParams)
{
	OSRefTable* sHLSSessionMap =  QTSServerInterface::GetServer()->GetRecordSessionMap();

	OSMutexLocker locker (sHLSSessionMap->GetMutex());

	//首先查找Map里面是否已经有了对应的流
	StrPtrLen streamName(inParams->inStreamName);

	OSRef* clientSesRef = sHLSSessionMap->Resolve(&streamName);

	if(NULL == clientSesRef) return QTSS_RequestFailed;

	EasyRecordSession* session = (EasyRecordSession*)clientSesRef->GetObject();

	session->HLSSessionRelease();

	sHLSSessionMap->Release(session->GetRef());

    if (session->GetRef()->GetRefCount() == 0)
    {   
        qtss_printf("EasyRecordModule.cpp:EasyHLSClose UnRegister and delete session =%p refcount=%"_U32BITARG_"\n", session->GetRef(), session->GetRef()->GetRefCount() ) ;       
        sHLSSessionMap->UnRegister(session->GetRef());
		session->Signal(Task::kKillEvent);
    }
	return QTSS_NoErr;
}

char* GetHLSUrl(char* inSessionName)
{
	OSRefTable* sHLSSessionMap =  QTSServerInterface::GetServer()->GetRecordSessionMap();

	OSMutexLocker locker (sHLSSessionMap->GetMutex());

	char* hlsURL = NULL;
	//首先查找Map里面是否已经有了对应的流
	StrPtrLen streamName(inSessionName);

	OSRef* clientSesRef = sHLSSessionMap->Resolve(&streamName);

	if(NULL == clientSesRef) return NULL;

	EasyRecordSession* session = (EasyRecordSession*)clientSesRef->GetObject();

	hlsURL = session->GetHLSURL();

	sHLSSessionMap->Release(session->GetRef());

	return hlsURL;
}