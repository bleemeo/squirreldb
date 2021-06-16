package facts

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"gopkg.in/yaml.v3"
	"honnef.co/go/tools/version"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[facts] ", log.LstdFlags)

// FactProvider provider information about system. Mostly static facts like OS version, architecture, ...
//
// It also possible to define fixed facts that this provider won't discover. This is useful for
// fact like "featureX_enabled" that other part of the code may set.
//
// There is also the possibility to add callback that are called on each update.
//
// There is one special fact named "fact_updated_at" which contains the last update of the facts.
type FactProvider struct {
	l sync.Mutex

	factPath     string
	hostRootPath string

	manualFact map[string]string
	callbacks  []FactCallback

	facts           map[string]string
	lastFactsUpdate time.Time
}

// FactCallback is a function called on each update of facts that may return additional facts.
//
// It returns the list of new or updated facts.
type FactCallback func(ctx context.Context, currentFact map[string]string) map[string]string

// NewFacter creates a new Fact provider
//
// factPath is the path to a yaml file that contains additional facts, usually
// facts that require root privilege to be read.
//
// hostRootPath is the path where host filesystem is visible. When running outside
// any container, it should be "/". When running inside a container it should be the path
// where host root is mounted.
func NewFacter(factPath, hostRootPath string) *FactProvider {
	return &FactProvider{
		factPath:     factPath,
		hostRootPath: hostRootPath,
	}
}

// AddCallback adds a FactCallback to provide additional facts.
// It currently not possible to remove a callback.
func (f *FactProvider) AddCallback(cb FactCallback) {
	f.l.Lock()
	defer f.l.Unlock()

	f.callbacks = append(f.callbacks, cb)
}

// Facts returns the list of facts for this system.
func (f *FactProvider) Facts(ctx context.Context, maxAge time.Duration) (facts map[string]string, err error) {
	f.l.Lock()
	defer f.l.Unlock()

	if time.Since(f.lastFactsUpdate) >= maxAge {
		t := time.Now()

		f.updateFacts(ctx)

		logger.Printf("facts: updateFacts() took %v", time.Since(t))
	}

	return f.facts, nil
}

// FastFacts returns an incomplete list of facts for this system. The slowest facts
// are not executed in order to improve the starting time.
func (f *FactProvider) FastFacts(ctx context.Context) (facts map[string]string, err error) {
	f.l.Lock()
	defer f.l.Unlock()

	newFacts := make(map[string]string)
	t := time.Now()

	f.fastUpdateFacts(ctx)

	logger.Printf("Fastfacts: FastUpdateFacts() took %v", time.Since(t))

	return newFacts, nil
}

// SetFact override/add a manual facts
//
// Any fact set using this method is valid until next call to SetFact.
func (f *FactProvider) SetFact(key string, value string) {
	f.l.Lock()
	defer f.l.Unlock()

	if f.manualFact == nil {
		f.manualFact = make(map[string]string)
	}

	if f.facts == nil {
		f.facts = make(map[string]string)
	}

	f.manualFact[key] = value
	f.facts[key] = value
}

func (f *FactProvider) updateFacts(ctx context.Context) {
	newFacts := f.fastUpdateFacts(ctx)

	cleanFacts(newFacts)

	f.facts = newFacts
	f.lastFactsUpdate = time.Now()
}

//nolint: gocyclo
func (f *FactProvider) fastUpdateFacts(ctx context.Context) map[string]string {
	newFacts := make(map[string]string)

	// get a copy of callbacks while lock is held
	callbacks := make([]FactCallback, len(f.callbacks))
	copy(callbacks, f.callbacks)

	if f.factPath != "" {
		if data, err := ioutil.ReadFile(f.factPath); err != nil {
			logger.Printf("unable to read fact file: %v", err)
		} else {
			var fileFacts map[string]string

			if err := yaml.Unmarshal(data, &fileFacts); err != nil {
				logger.Printf("fact file is invalid: %v", err)
			} else {
				for k, v := range fileFacts {
					newFacts[k] = v
				}
			}
		}
	}

	for k, v := range f.platformFacts() {
		newFacts[k] = v
	}

	newFacts["architecture"] = runtime.GOARCH

	if f.hostRootPath != "" {
		if v, err := ioutil.ReadFile(filepath.Join(f.hostRootPath, "etc/timezone")); err == nil {
			newFacts["timezone"] = strings.TrimSpace(string(v))
		}
	}

	newFacts["squirreldb_version"] = version.Version
	newFacts["fact_updated_at"] = time.Now().UTC().Format(time.RFC3339)

	cpu, err := cpu.Info()

	if err == nil && len(cpu) > 0 {
		newFacts["cpu_model_name"] = cpu[0].ModelName
		newFacts["cpu_cores"] = strconv.Itoa(len(cpu))
	}

	mem, err := mem.VirtualMemory()

	if err == nil && mem != nil {
		newFacts["memory"] = byteCountDecimal(mem.Total)
	}

	for _, c := range callbacks {
		for k, v := range c(ctx, newFacts) {
			newFacts[k] = v
		}
	}

	for k, v := range f.manualFact {
		newFacts[k] = v
	}

	cleanFacts(newFacts)

	return newFacts
}

// cleanFacts will remove key with empty values and truncate value
// with 100 characters or more.
func cleanFacts(facts map[string]string) {
	for k, v := range facts {
		if v == "" {
			delete(facts, k)
		}

		if len(v) >= 100 {
			facts[k] = v[:97] + "..."
		}
	}
}

func byteCountDecimal(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := int64(unit), 0

	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
