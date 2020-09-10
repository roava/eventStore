package bifrost

import (
	"crypto/tls"
)

type Options struct {
	ServiceName    string
	Address        string
	TLSCertificate *tls.Certificate
}
