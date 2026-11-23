//go:build integration

package pgstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/RatulSikder97/agentflow/store/pgstore"
	"github.com/RatulSikder97/agentflow/store/storetest"
)

func open(t *testing.T) *pgstore.Store {
	t.Helper()
	dsn := os.Getenv("AGENTFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AGENTFLOW_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Store { return open(t) })
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := open(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}
