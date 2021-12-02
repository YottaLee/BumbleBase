package concurrency

import (
	"errors"
	"sync"

	db "github.com/brown-csci1270/db/pkg/db"
	uuid "github.com/google/uuid"
)

// Transaction Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// WLock Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// WUnlock Release the write lock on the tx
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// RLock Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// RUnlock Release the read lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// GetClientID Get the transaction id.
func (t *Transaction) GetClientID() uuid.UUID {
	return t.clientId
}

// GetResources Get the transaction's resources.
func (t *Transaction) GetResources() map[Resource]LockType {
	return t.resources
}

// TransactionManager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// NewTransactionManager Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// GetLockManager Get the transactions.
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lm
}

// GetTransactions Get the transactions.
func (tm *TransactionManager) GetTransactions() map[uuid.UUID]*Transaction {
	return tm.transactions
}

// GetTransaction Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (*Transaction, bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	t, found := tm.transactions[clientId]
	return t, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Lock the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	tm.tmMtx.RLock()
	transaction, found := tm.GetTransaction(clientId)
	tm.tmMtx.RUnlock()

	if !found {
		return errors.New("transaction not found")
	}

	// get the resource to lock
	tableName := table.GetName()
	r := Resource{resourceKey: resourceKey, tableName: tableName}

	transaction.RLock()
	curType, found := transaction.resources[r]
	transaction.RUnlock()

	if found {
		// The transaction already holds a same or higher level lock
		// ignore the request and return
		if curType >= lType {
			return nil
		} else {
			return errors.New("illegal lock type")
		}
	}
	// An upgrade on the current lock or a new lock is needed.

	// 1. detect cycle
	conflictTransactions := tm.discoverTransactions(r, lType)

	// add edges to the precedence graph
	for _, t := range conflictTransactions {
		if transaction == t {
			continue
		}

		tm.pGraph.AddEdge(transaction, t)
		defer tm.pGraph.RemoveEdge(transaction, t)
	}

	// detect cycle in the precedence graph
	containCycle := tm.pGraph.DetectCycle()

	if containCycle {
		return errors.New("contains cycle")
	}

	// 2. No cycle in the precedence graph, we can now lock the resource with lType

	// 2.1 we can lock the resource via the LockManager.
	err := tm.lm.Lock(r, lType)
	if err != nil {
		return err
	}

	// 2.2 Finally, add the resource to the transaction's resource map
	transaction.WLock()
	transaction.resources[r] = lType
	transaction.WUnlock()

	return nil
}

// Unlock the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	tm.tmMtx.RLock()
	transaction, found := tm.GetTransaction(clientId)
	tm.tmMtx.RUnlock()

	if !found {
		return errors.New("transaction not found")
	}

	// get the resource to lock
	tableName := table.GetName()
	resource := Resource{resourceKey: resourceKey, tableName: tableName}

	transaction.WLock()
	defer transaction.WUnlock()

	removed := false
	for storedResource, storedType := range transaction.resources {
		// if type mismatch, throw an error
		if storedType != lType {
			return errors.New("lock type mismatch")
		}

		if storedResource == resource {
			delete(transaction.resources, resource)
			removed = true
			break
		}
	}

	if !removed {
		return errors.New("resource not found")
	}

	err := tm.lm.Unlock(resource, lType)
	return err
}

// Commit the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) []*Transaction {
	ret := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				ret = append(ret, t)
				break
			}
		}
		t.RUnlock()
	}
	return ret
}
