//go:build linux

package objectstore

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

const (
	signatureAlgorithm  = "AWS4-HMAC-SHA256"
	signatureTimeFormat = "20060102T150405Z"
	maximumClockSkew    = 15 * time.Minute
)

type signatureHeader struct {
	accessKey     string
	date          string
	region        string
	signedHeaders []string
	signature     []byte
}

func (c Config) authenticate(r *http.Request, now time.Time) (Credential, error) {
	header, err := parseSignatureHeader(r.Header.Get("Authorization"))
	if err != nil {
		return Credential{}, err
	}
	credential, ok := c.credential(header.accessKey)
	if !ok {
		return Credential{}, fmt.Errorf("unknown access key")
	}
	requestTime, err := time.Parse(signatureTimeFormat, r.Header.Get("X-Amz-Date"))
	if err != nil {
		return Credential{}, fmt.Errorf("invalid x-amz-date")
	}
	if requestTime.Sub(now) > maximumClockSkew || now.Sub(requestTime) > maximumClockSkew {
		return Credential{}, fmt.Errorf("request time is outside the allowed clock skew")
	}
	if header.date != requestTime.Format("20060102") || header.region != c.RegionName() {
		return Credential{}, fmt.Errorf("credential scope does not match the request")
	}
	canonicalHeaders, err := signedCanonicalHeaders(r, header.signedHeaders)
	if err != nil {
		return Credential{}, err
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	query := strings.ReplaceAll(r.URL.Query().Encode(), "+", "%20")
	canonicalRequest := strings.Join([]string{
		r.Method,
		s3utils.EncodePath(r.URL.Path),
		query,
		canonicalHeaders,
		strings.Join(header.signedHeaders, ";"),
		payloadHash,
	}, "\n")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	scope := strings.Join([]string{header.date, header.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		signatureAlgorithm,
		requestTime.Format(signatureTimeFormat),
		scope,
		hex.EncodeToString(requestHash[:]),
	}, "\n")
	expected := signature(credential.SecretKey, header.date, header.region, stringToSign)
	if subtle.ConstantTimeCompare(header.signature, expected) != 1 {
		return Credential{}, fmt.Errorf("signature does not match")
	}
	return credential, nil
}

func parseSignatureHeader(value string) (signatureHeader, error) {
	value, ok := strings.CutPrefix(value, signatureAlgorithm+" ")
	if !ok {
		return signatureHeader{}, fmt.Errorf("missing signature v4 authorization")
	}
	parts := make(map[string]string, 3)
	for part := range strings.SplitSeq(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || key == "" || val == "" {
			return signatureHeader{}, fmt.Errorf("invalid signature v4 authorization")
		}
		parts[key] = val
	}
	credentialScope := strings.Split(parts["Credential"], "/")
	if len(credentialScope) != 5 || credentialScope[2] == "" || credentialScope[3] != "s3" || credentialScope[4] != "aws4_request" {
		return signatureHeader{}, fmt.Errorf("invalid credential scope")
	}
	signedHeaders := strings.Split(parts["SignedHeaders"], ";")
	if len(signedHeaders) == 0 || !sort.StringsAreSorted(signedHeaders) {
		return signatureHeader{}, fmt.Errorf("invalid signed headers")
	}
	hasHost := false
	hasDate := false
	for i, name := range signedHeaders {
		if name == "" || name != strings.ToLower(name) || i > 0 && name == signedHeaders[i-1] {
			return signatureHeader{}, fmt.Errorf("invalid signed headers")
		}
		hasHost = hasHost || name == "host"
		hasDate = hasDate || name == "x-amz-date"
	}
	if !hasHost || !hasDate {
		return signatureHeader{}, fmt.Errorf("required headers are not signed")
	}
	sig, err := hex.DecodeString(parts["Signature"])
	if err != nil || len(sig) != sha256.Size {
		return signatureHeader{}, fmt.Errorf("invalid signature")
	}
	return signatureHeader{
		accessKey:     credentialScope[0],
		date:          credentialScope[1],
		region:        credentialScope[2],
		signedHeaders: signedHeaders,
		signature:     sig,
	}, nil
}

func signedCanonicalHeaders(r *http.Request, names []string) (string, error) {
	var canonical strings.Builder
	for _, name := range names {
		var values []string
		if name == "host" {
			values = []string{r.Host}
		} else {
			values = r.Header.Values(name)
		}
		if len(values) == 0 {
			return "", fmt.Errorf("signed header %q is missing", name)
		}
		canonical.WriteString(name)
		canonical.WriteByte(':')
		for i, value := range values {
			if i > 0 {
				canonical.WriteByte(',')
			}
			canonical.WriteString(strings.Join(strings.Fields(value), " "))
		}
		canonical.WriteByte('\n')
	}
	return canonical.String(), nil
}

func signature(secret, date, region, stringToSign string) []byte {
	dateKey := sumHMAC([]byte("AWS4"+secret), []byte(date))
	regionKey := sumHMAC(dateKey, []byte(region))
	serviceKey := sumHMAC(regionKey, []byte("s3"))
	signingKey := sumHMAC(serviceKey, []byte("aws4_request"))
	return sumHMAC(signingKey, []byte(stringToSign))
}

func sumHMAC(key, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write(data)
	return hash.Sum(nil)
}
