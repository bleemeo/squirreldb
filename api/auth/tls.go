// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/prometheus/exporter-toolkit/web"
)

var ErrInvalidTLS = errors.New("invalid TLS config")

// tlsVersions are the accepted values of min_version, the same as Prometheus web config.
var tlsVersions = map[string]uint16{ //nolint:gochecknoglobals
	"TLS10": tls.VersionTLS10,
	"TLS11": tls.VersionTLS11,
	"TLS12": tls.VersionTLS12,
	"TLS13": tls.VersionTLS13,
}

// TLSOptions configures the TLS of the HTTP servers. The fields have the same meaning
// as in the tls_server_config of the Prometheus web configuration file.
type TLSOptions struct {
	CertFile string
	KeyFile  string
	// ClientCAFile is the CA used to verify the client certificates (mTLS).
	ClientCAFile string
	// ClientAuthType is one of NoClientCert (the default), RequestClientCert,
	// RequireAnyClientCert, VerifyClientCertIfGiven or RequireAndVerifyClientCert.
	ClientAuthType string
	// MinVersion is one of TLS10, TLS11, TLS12 (the default) or TLS13.
	MinVersion string
}

// Enabled returns whether TLS is configured.
func (o TLSOptions) Enabled() bool {
	return o.CertFile != "" || o.KeyFile != "" || o.ClientCAFile != "" || o.ClientAuthType != ""
}

// NewTLSConfig returns the server TLS config, or nil when TLS is not enabled.
// It reads the files to check they are valid. The certificate, key and client CA
// are read again on each TLS handshake, so they can be renewed without restarting
// SquirrelDB (like the Prometheus web configuration).
func NewTLSConfig(opts TLSOptions) (*tls.Config, error) {
	if !opts.Enabled() {
		return nil, nil //nolint:nilnil // nil means TLS is disabled.
	}

	webConfig, err := toWebTLSConfig(opts)
	if err != nil {
		return nil, err
	}

	// exporter-toolkit ignores invalid client CA certificates, check them here
	// to report a configuration error instead of rejecting every client.
	if opts.ClientCAFile != "" {
		data, err := os.ReadFile(opts.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidTLS, err)
		}

		if !x509.NewCertPool().AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("%w: no valid certificate in client_ca_file %s", ErrInvalidTLS, opts.ClientCAFile)
		}
	}

	cfg, err := web.ConfigToTLSConfig(webConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTLS, err)
	}

	cfg.NextProtos = []string{"h2", "http/1.1"}

	// Rebuild the config on each connection to reload the client CA, as
	// exporter-toolkit does. The certificate is also reloaded by GetCertificate.
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		clientCfg, err := web.ConfigToTLSConfig(webConfig)
		if err != nil {
			return nil, err
		}

		clientCfg.NextProtos = cfg.NextProtos

		return clientCfg, nil
	}

	return cfg, nil
}

// ServerCertificate reads and parses the first (leaf) certificate of CertFile. Like
// the TLS handshake, it reads the file on each call, so it reflects a renewed
// certificate.
func (o TLSOptions) ServerCertificate() (*x509.Certificate, error) {
	data, err := os.ReadFile(o.CertFile)
	if err != nil {
		return nil, err
	}

	for {
		var block *pem.Block

		block, data = pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("%w: no certificate in cert_file %s", ErrInvalidTLS, o.CertFile)
		}

		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
}

func toWebTLSConfig(opts TLSOptions) (*web.TLSConfig, error) {
	minVersion := uint16(tls.VersionTLS12)

	if opts.MinVersion != "" {
		v, ok := tlsVersions[opts.MinVersion]
		if !ok {
			return nil, fmt.Errorf(
				"%w: unknown min_version %q (expected TLS10, TLS11, TLS12 or TLS13)",
				ErrInvalidTLS, opts.MinVersion,
			)
		}

		minVersion = v
	}

	return &web.TLSConfig{
		TLSCertPath:              opts.CertFile,
		TLSKeyPath:               opts.KeyFile,
		ClientCAs:                opts.ClientCAFile,
		ClientAuth:               opts.ClientAuthType,
		MinVersion:               web.TLSVersion(minVersion),
		MaxVersion:               web.TLSVersion(tls.VersionTLS13),
		PreferServerCipherSuites: true,
	}, nil
}
