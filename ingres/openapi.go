package ingres

/*
#cgo pkg-config: iiapi

#include <stdlib.h>
#include <iiapi.h>

IIAPI_INITPARM InitParm = {0, IIAPI_VERSION, 0, NULL};

*/
import "C"
import (
	"errors"
	"fmt"
	"log"
)

type OpenAPIEnv struct {
	handle C.II_PTR
}

type OpenAPIConn struct {
	handle C.II_PTR
}

type ConnParams struct {
	dbname   string
	username string
	password string
	timeout  int
}

func InitOpenAPI() (*OpenAPIEnv, error) {
	C.IIapi_initialize(&C.InitParm)

	if C.InitParm.in_status != 0 {
		return nil, errors.New("could not initialize Ingres OpenAPI")
	}

	return &OpenAPIEnv{handle: C.InitParm.in_envHandle}, nil
}

func ReleaseOpenAPI(env *OpenAPIEnv) {
	var rel C.IIAPI_RELENVPARM
	var term C.IIAPI_TERMPARM

	rel.re_envHandle = env.handle
	C.IIapi_releaseEnv(&rel)
	C.IIapi_terminate(&term)
}

func Connect(env *OpenAPIEnv, params ConnParams) (*OpenAPIConn, error) {
	var connParm C.IIAPI_CONNPARM

	connParm.co_genParm.gp_callback = nil
	connParm.co_genParm.gp_closure = nil
	connParm.co_type = C.IIAPI_CT_SQL
	connParm.co_target = C.CString(params.dbname)
	connParm.co_connHandle = env.handle
	connParm.co_tranHandle = nil
	connParm.co_username = nil
	connParm.co_password = nil
	if len(params.username) > 0 {
		connParm.co_username = C.CString(params.username)
	}
	if len(params.password) > 0 {
		connParm.co_password = C.CString(params.password)
	}
	connParm.co_timeout = C.int(params.timeout)

	C.IIapi_connect(&connParm)
	Wait(&connParm.co_genParm)
	err := checkError("IIapi_connect()", &connParm.co_genParm)

    if ( connParm.co_genParm.gp_status == C.IIAPI_ST_SUCCESS ) {
        return &OpenAPIConn{handle: connParm.co_connHandle}, nil
    }

    if (connParm.co_connHandle != nil) {
        var abortParm C.IIAPI_ABORTPARM

        abortParm.ab_genParm.gp_callback = nil;
        abortParm.ab_genParm.gp_closure  = nil;
        abortParm.ab_connHandle = connParm.co_connHandle;

        /*
        ** Make sync request.
        */
        C.IIapi_abort( &abortParm );
        Wait(&abortParm.ab_genParm);

        abortErr := checkError("IIapi_abort()", &abortParm.ab_genParm );
        if (abortErr != nil) {
            log.Printf("could not abort connection: %v", abortErr)
        }
    }

    if err == nil {
        err = errors.New("undefined error")
    }

    return nil, err
}

func Wait(genParm *C.IIAPI_GENPARM) {
	var waitParm C.IIAPI_WAITPARM

	for genParm.gp_completed == 0 {
		waitParm.wt_timeout = -1
		C.IIapi_wait(&waitParm)

		if waitParm.wt_status != C.IIAPI_ST_SUCCESS {
			genParm.gp_status = waitParm.wt_status
			break
		}
	}
}

func checkError(location string, genParm *C.IIAPI_GENPARM) error {
	var desc string
	var err error

	if genParm.gp_status >= C.IIAPI_ST_ERROR {
		switch genParm.gp_status {
		case C.IIAPI_ST_ERROR:
			desc = "IIAPI_ST_ERROR"
		case C.IIAPI_ST_FAILURE:
			desc = "IIAPI_ST_FAILURE"
		case C.IIAPI_ST_NOT_INITIALIZED:
			desc = "IIAPI_ST_NOT_INITIALIZED"
		case C.IIAPI_ST_INVALID_HANDLE:
			desc = "IIAPI_ST_INVALID_HANDLE"
		case C.IIAPI_ST_OUT_OF_MEMORY:
			desc = "IIAPI_ST_OUT_OF_MEMORY"
		default:
			desc = fmt.Sprintf("%d", genParm.gp_status)
		}

		err = errors.New(fmt.Sprintf("%s status = %s", location, desc))
	}

	if genParm.gp_errorHandle != nil {
		var getErrParm C.IIAPI_GETEINFOPARM
		/*
		 ** Provide initial error handle.
		 */
		getErrParm.ge_errorHandle = genParm.gp_errorHandle

		/*
		 ** Call IIapi_getErrorInfo() in loop until no data.
		 */
		for {
			C.IIapi_getErrorInfo(&getErrParm)
			if getErrParm.ge_status != C.IIAPI_ST_SUCCESS {
				break
			}

			/*
			 ** Print error message info.
			 */
			switch getErrParm.ge_type {
			case C.IIAPI_GE_ERROR:
				desc = "ERROR"
			case C.IIAPI_GE_WARNING:
				desc = "WARNING"
			case C.IIAPI_GE_MESSAGE:
				desc = "USER MESSAGE"
			default:
				desc = "?"
			}

			var msg string = "NULL"
			if getErrParm.ge_message != nil {
				msg = C.GoString(getErrParm.ge_message)
			}

			log.Printf("Type: %s '%s' 0x%x: '%s'",
				desc, getErrParm.ge_SQLSTATE, getErrParm.ge_errorCode, msg)
		}
	}

	return err
}
