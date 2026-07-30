package export_wasi_http_handler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	client "wit_component/wasi_http_client"
	HT "wit_component/wasi_http_types"

	WT "go.bytecodealliance.org/pkg/wit/types"
)

// Handle the specified `Request`, returning a `Response`
func Handle(request *HT.Request) WT.Result[*HT.Response, HT.ErrorCode] {
     	var method uint8
	var path string 
	method = request.GetMethod().Tag()
	path   = request.GetPathWithQuery().SomeOr("/")
     	fmt.Printf("method<%d> path<%s> in func " +
		"export_wasi_http_handler/Handler \n", method, path)

	if method == HT.MethodGet && path == "/hello" {
		// Say hello!

		tx, rx := HT.MakeStreamU8()

		go func() {
			defer tx.Drop()
			tx.WriteAll([]uint8("Hello, world!"))
		}()

		response, send := HT.ResponseNew(
			HT.FieldsFromList([]WT.Tuple2[string, []byte]{
				{F0: "content-type", F1: []byte("text/plain")},
			}).Ok(),
			WT.Some(rx),
			trailersFuture(),
		)
		send.Drop()

		return WT.Ok[*HT.Response, HT.ErrorCode](response)

	} else if method == HT.MethodGet && path == "/hash-all" {
	
		// Collect one or more "url" headers, download their contents
		// concurrently, compute their SHA-256 hashes incrementally
		// (i.e. without buffering the response bodies), and stream 
		// the results back to the client as they become available.

		urls := make([]string, 0)
		for _, pair := range request.GetHeaders().CopyAll() {
			if pair.F0 == "url" {
				urls = append(urls, string(pair.F1))
			}
		}

		tx, rx := HT.MakeStreamU8()

		go func() {
			defer tx.Drop()

			channel := make(chan WT.Tuple2[string, string])
			for _, url := range urls {
				go func() {
					channel <- WT.Tuple2[string, string]{F0: url, F1: getSha256(url)}
				}()
			}

			for i := 0; i < len(urls); i++ {
				pair := (<-channel)
				tx.WriteAll([]uint8(fmt.Sprintf("%v: %v\n", pair.F0, pair.F1)))
			}
		}()

		response, send := HT.ResponseNew(
			HT.FieldsFromList([]WT.Tuple2[string, []uint8]{
				{F0: "content-type", F1: []uint8("text/plain")},
			}).Ok(),
			WT.Some(rx),
			trailersFuture(),
		)
		send.Drop()

		return WT.Ok[*HT.Response, HT.ErrorCode](response)

	} else if method == HT.MethodPost && path == "/echo" {
	
		// Echo the request body back to the client without buffering.

		requestHeaders := request.GetHeaders().CopyAll()

		rx, trailers := HT.RequestConsumeBody(request, unitFuture())

		responseHeaders := make([]WT.Tuple2[string, []uint8], 0, 1)
		for _, pair := range requestHeaders {
			if pair.F0 == "content-type" {
				responseHeaders = append(responseHeaders, pair)
			}
		}

		response, send := HT.ResponseNew(
			HT.FieldsFromList(responseHeaders).Ok(),
			WT.Some(rx),
			trailers,
		)
		send.Drop()

		return WT.Ok[*HT.Response, HT.ErrorCode](response)

	} else {
		// Bad request

		response, send := HT.ResponseNew(
			HT.MakeFields(),
			WT.None[*WT.StreamReader[uint8]](),
			trailersFuture(),
		)
		send.Drop()
		response.SetStatusCode(400).Ok()

		return WT.Ok[*HT.Response, HT.ErrorCode](response)

	}
}

// Download the contents of the specified URL, computing the 
// SHA-256 incrementally as the response body arrives.
//
// This returns a tuple of the original URL and either the 
// hex-encoded hash or an error message.
func getSha256(urlString string) string {
	parsed, err := url.Parse(urlString)
	if err != nil {
		return err.Error()
	}

	var scheme HT.Scheme
	switch parsed.Scheme {
	case "http":
		scheme = HT.MakeSchemeHttp()
	case "https":
		scheme = HT.MakeSchemeHttps()
	default:
		scheme = HT.MakeSchemeOther(parsed.Scheme)
	}

	request, send := HT.RequestNew(
		HT.MakeFields(),
		WT.None[*WT.StreamReader[uint8]](),
		trailersFuture(),
		WT.None[*HT.RequestOptions](),
	)
	send.Drop()
	request.SetScheme(WT.Some(scheme)).Ok()
	request.SetAuthority(WT.Some(parsed.Host)).Ok()
	request.SetPathWithQuery(WT.Some(parsed.Path)).Ok()

	result := client.Send(request)
	switch result.Tag() {
	case WT.ResultOk:
		response := result.Ok()
		status := response.GetStatusCode()
		if status < 200 || status > 299 {
			return fmt.Sprintf("unexpected status: %v", status)
		}

		rx, trailers := HT.ResponseConsumeBody(response, unitFuture())
		trailers.Drop()
		defer rx.Drop()

		buffer := make([]uint8, 16*1024)
		hash := sha256.New()
		for !rx.WriterDropped() {
			count := rx.Read(buffer)
			writeCount, err := hash.Write(buffer[:count])
			if err != nil || uint32(writeCount) != count {
				panic("unreachable")
			}
		}
		return hex.EncodeToString(hash.Sum([]uint8{}))

	case WT.ResultErr:
		return "error sending request"

	default:
		panic("unreachable")
	}
}

func trailersFuture() *WT.FutureReader[WT.Result[WT.Option[*HT.Fields], HT.ErrorCode]] {
	tx, rx := HT.MakeFutureResultOptionFieldsErrorCode()
	go tx.Write(WT.Ok[WT.Option[*HT.Fields], HT.ErrorCode](WT.None[*HT.Fields]()))
	return rx
}

func unitFuture() *WT.FutureReader[WT.Result[WT.Unit, HT.ErrorCode]] {
	tx, rx := HT.MakeFutureResultUnitErrorCode()
	go tx.Write(WT.Ok[WT.Unit, HT.ErrorCode](WT.Unit{}))
	return rx
}
