package mutable

import (
	"fmt"

	"github.com/gocql/gocql"
)

// LabelWriter allows writing mutable labels to Cassandra.
type LabelWriter struct {
	session *gocql.Session
}

// Label represents a mutable label with its associated non mutable labels.
type Label struct {
	Tenant string
	Name   string
	Value  string
	Labels LabelValues
}

func NewLabelWriter(session *gocql.Session) LabelWriter {
	return LabelWriter{session}
}

func (lw *LabelWriter) WriteLabels(mutLabels []Label) error {
	for _, mutLabel := range mutLabels {
		if err := lw.insertMutableLabel(mutLabel); err != nil {
			return err
		}
	}

	return nil
}

// insertMutableLabel inserts or modifies the non mutable labels associated to a mutable label name and value.
func (lw *LabelWriter) insertMutableLabel(mutLabel Label) error {
	query := lw.session.Query(`
		INSERT INTO mutable_labels (tenant, name, value, labels)
		VALUES (?, ?, ?, ?)
	`, mutLabel.Tenant, mutLabel.Name, mutLabel.Value, mutLabel.Labels)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert labels: %w", err)
	}

	return nil
}
