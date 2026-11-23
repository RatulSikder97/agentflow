package memstore_test

import (
	"testing"

	"github.com/RatulSikder97/agentflow/store/memstore"
	"github.com/RatulSikder97/agentflow/store/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Store { return memstore.New() })
}
