package ds2bq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// DecodeGCSObject decodes a GCSObject from r.
func DecodeGCSObject(r io.Reader) (*GCSObject, error) {
	decoder := json.NewDecoder(r)
	var obj *GCSObject
	err := decoder.Decode(&obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// DecodeGCSObjectToBQJobReq decodes a GCSObjectToBQJobReq from r.
func DecodeGCSObjectToBQJobReq(r io.Reader) (*GCSObjectToBQJobReq, error) {
	decoder := json.NewDecoder(r)
	var req *GCSObjectToBQJobReq
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// ReceiveOCNHandleFunc returns a http.HandlerFunc that receives OCN.
// The path is for
func ReceiveOCNHandleFunc(bucketName, queueName, path string, kindNames []string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		obj, err := DecodeGCSObject(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		if !obj.IsImportTarget(c, r, bucketName, kindNames) {
			return
		}

		err = ReceiveOCN(c, obj, queueName, path)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to receive OCN: %s", err)
			return
		}
	}
}

func ReceiveOCNHandleAllKindsFunc(bucketName, queueName, path string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		obj, err := DecodeGCSObject(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		kinds, err := allKinds(c)
		if err != nil {
			panic(err)
		}

		if !obj.IsImportTarget(c, r, bucketName, kinds) {
			return
		}

		err = ReceiveOCN(c, obj, queueName, path)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to receive OCN: %s", err)
			return
		}
	}
}

func allKinds(ctx context.Context) ([]string, error) {
	ret, err := datastore.NewQuery("__kind__").KeysOnly().GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}

	kinds := make([]string, 0, len(ret))
	for i := range ret {
		n := ret[i].StringID()
		// 除外したいKindを指定
		if strings.HasPrefix(n, "_") {
			continue
		}
		kinds = append(kinds, n)
	}

	return kinds, nil
}

// ImportBigQueryHandleFunc returns a http.HandlerFunc that imports GCSObject to BigQuery.
func ImportBigQueryHandleFunc(datasetID string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		req, err := DecodeGCSObjectToBQJobReq(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		err = insertImportJob(c, req, datasetID)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to import BigQuery: %s", err)
			return
		}
	}
}
