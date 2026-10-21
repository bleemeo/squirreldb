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

package auth_test

import (
	"bytes"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/api/auth/authtest"
)

func TestNewTLSConfig(t *testing.T) {
	t.Parallel()

	certs := authtest.WriteCertificates(t, t.TempDir())
	notPEM := filepath.Join(t.TempDir(), "not-pem")

	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	valid := auth.TLSOptions{CertFile: certs.ServerCertFile, KeyFile: certs.ServerKeyFile}

	cases := []struct {
		name    string
		mutate  func(o *auth.TLSOptions)
		wantErr bool
	}{
		{name: "valid", mutate: func(*auth.TLSOptions) {}},
		{name: "min version", mutate: func(o *auth.TLSOptions) { o.MinVersion = "TLS13" }},
		{
			name: "mTLS",
			mutate: func(o *auth.TLSOptions) {
				o.ClientCAFile, o.ClientAuthType = certs.CAFile, "RequireAndVerifyClientCert"
			},
		},
		{name: "missing key", mutate: func(o *auth.TLSOptions) { o.KeyFile = "" }, wantErr: true},
		{name: "missing cert", mutate: func(o *auth.TLSOptions) { o.CertFile = "" }, wantErr: true},
		{name: "unreadable cert", mutate: func(o *auth.TLSOptions) { o.CertFile = "/does/not/exist" }, wantErr: true},
		{name: "key mismatch", mutate: func(o *auth.TLSOptions) { o.KeyFile = certs.OtherKeyFile }, wantErr: true},
		{name: "unknown min version", mutate: func(o *auth.TLSOptions) { o.MinVersion = "SSL3" }, wantErr: true},
		{name: "unknown client auth", mutate: func(o *auth.TLSOptions) { o.ClientAuthType = "Maybe" }, wantErr: true},
		{
			name:    "client CA without client auth type",
			mutate:  func(o *auth.TLSOptions) { o.ClientCAFile = certs.CAFile },
			wantErr: true,
		},
		{
			name: "invalid client CA",
			mutate: func(o *auth.TLSOptions) {
				o.ClientCAFile, o.ClientAuthType = notPEM, "RequireAndVerifyClientCert"
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		opts := valid
		tc.mutate(&opts)

		cfg, err := auth.NewTLSConfig(opts)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}

		if err == nil && cfg == nil {
			t.Errorf("%s: nil config", tc.name)
		}
	}

	cfg, err := auth.NewTLSConfig(auth.TLSOptions{MinVersion: "TLS13"})
	if cfg != nil || err != nil {
		t.Errorf("TLS without certificate: got %v, %v, want disabled", cfg, err)
	}
}

// TestTLSCertificateReload checks that a renewed certificate is used by the new
// connections without rebuilding the config.
func TestTLSCertificateReload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certs := authtest.WriteCertificates(t, dir)

	cfg, err := auth.NewTLSConfig(auth.TLSOptions{CertFile: certs.ServerCertFile, KeyFile: certs.ServerKeyFile})
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}

	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			_ = conn.(*tls.Conn).HandshakeContext(t.Context()) //nolint:forcetypeassert
			_ = conn.Close()
		}
	}()

	peerCert := func(certs authtest.Certificates) []byte {
		dialer := &tls.Dialer{Config: certs.ClientConfig(t, false)}

		conn, err := dialer.DialContext(t.Context(), "tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}

		defer conn.Close()

		return conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Raw //nolint:forcetypeassert
	}

	before := peerCert(certs)

	// Renew the CA and certificates at the same paths.
	renewed := authtest.WriteCertificates(t, dir)
	after := peerCert(renewed)

	if bytes.Equal(before, after) {
		t.Error("the server still uses the old certificate")
	}
}
