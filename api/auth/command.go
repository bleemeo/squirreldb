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
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const commandUsage = `Usage: squirreldb auth <command>

Commands:
  hash-password  Read a password on stdin and print its bcrypt hash, for the
                 password_hash of a user. Example:
                   printf '%s' "$PASSWORD" | squirreldb auth hash-password
  gen-token      Generate a random bearer token and print it with its sha256,
                 for the token_sha256 of a user.
`

var errEmptyPassword = errors.New("empty password")

// RunCommand runs the "squirreldb auth" subcommand and returns the exit code.
func RunCommand(args []string) int {
	return runCommand(args, os.Stdin, os.Stdout, os.Stderr)
}

func runCommand(args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = io.WriteString(stderr, commandUsage)

		return 2
	}

	var err error

	switch args[0] {
	case "hash-password":
		err = hashPassword(stdin, stdout)
	case "gen-token":
		err = genToken(stdout)
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, commandUsage)

		return 0
	default:
		_, _ = io.WriteString(stderr, commandUsage)

		return 2
	}

	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)

		return 1
	}

	return 0
}

// hashPassword reads the password from the first line of stdin.
func hashPassword(stdin io.Reader, stdout io.Writer) error {
	line, err := bufio.NewReader(stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	password := strings.TrimRight(line, "\r\n")
	if password == "" {
		return errEmptyPassword
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(stdout, string(hash))

	return err
}

func genToken(stdout io.Writer) error {
	// rand.Text returns 26 base32 characters, 130 bits of entropy.
	token := rand.Text() + rand.Text()
	sum := sha256.Sum256([]byte(token))

	_, err := fmt.Fprintf(stdout, "token: %s\ntoken_sha256: %s\n", token, hex.EncodeToString(sum[:]))

	return err
}
