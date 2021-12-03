package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	db "github.com/brown-csci1270/db/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// RecoveryManager Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// NewRecoveryManager Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Table Write a table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	// write the log using the manager
	l := tableLog{tblType: tblType, tblName: tblName}
	_ = rm.writeToBuffer(l.toString())
}

// Edit Write an edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	// make and log
	l := editLog{
		id:        clientId,
		tablename: table.GetName(),
		action:    action,
		key:       key,
		oldval:    oldval,
		newval:    newval,
	}

	// append the log to the corresponding array
	_, ok := rm.txStack[clientId]
	if ok {
		rm.txStack[clientId] = append(rm.txStack[clientId], &l)
	}
	//rm.txStack[clientId] = append(rm.txStack[clientId], &l)

	_ = rm.writeToBuffer(l.toString())
}

// Start Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	// make the log
	l := startLog{id: clientId}

	// make the log array and add to txStack
	var logs []Log
	logs = append(logs, &l)
	rm.txStack[clientId] = logs
	_ = rm.writeToBuffer(l.toString())
}

// Commit Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	// make the log
	l := commitLog{id: clientId}

	// delete the log array from txStack
	delete(rm.txStack, clientId)

	_ = rm.writeToBuffer(l.toString())
}

// Checkpoint Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	// make the log
	allUUIDs := make([]uuid.UUID, 0)
	for id, _ := range rm.txStack {
		allUUIDs = append(allUUIDs, id)
	}

	// write the log to the disk
	l := checkpointLog{ids: allUUIDs}

	// flush all the tables
	tables := rm.d.GetTables()
	for _, table := range tables {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllUpdates()
	}

	_ = rm.writeToBuffer(l.toString())

	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Recover Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	logs, checkpointPos, err := rm.readLogs()
	if err != nil {
		return err
	}

	length := len(logs)
	if checkpointPos >= length {
		return nil
	}

	// iterate from the checkpoint to redo all the log
	// while examining which transaction is still active at crash
	undoSet := make(map[uuid.UUID]bool)
	switch checkPoint := logs[checkpointPos].(type) {
	case *checkpointLog:
		// add all current active transactions
		for _, id := range checkPoint.ids {
			undoSet[id] = true
			err = rm.tm.Begin(id)
			if err != nil {
				return err
			}
		}
	default:
	}

	// keep track of which transaction has ended
	for i := checkpointPos; i < length; i += 1 {
		switch l := logs[i].(type) {
		case *startLog:
			// a new active transaction
			undoSet[l.id] = true
			err = rm.tm.Begin(l.id)
			if err != nil {
				return err
			}
		case *editLog:
			err = rm.Redo(l)
			if err != nil {
				return err
			}
		case *tableLog:
			err = rm.Redo(l)
			if err != nil {
				return err
			}
		case *commitLog:
			// transaction has finished, no need to undo
			delete(undoSet, l.id)
			err = rm.tm.Commit(l.id)
			if err != nil {
				return err
			}
		default:
			continue
		}
	}

	for i := length - 1; i >= 0; i -= 1 {
		if len(undoSet) == 0 {
			// no more transaction to undo, break the loop
			break
		}

		switch l := logs[i].(type) {
		case *startLog:
			if _, exist := undoSet[l.id]; exist {
				delete(undoSet, l.id)
				rm.Commit(l.id)
				err = rm.tm.Commit(l.id)
				if err != nil {
					return err
				}
			}
		case *editLog:
			if _, exist := undoSet[l.id]; exist {
				err = rm.Undo(l)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Rollback Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	logs := rm.txStack[clientId]

	if len(logs) == 0 {
		rm.Commit(clientId)
		err := rm.tm.Commit(clientId)
		return err
	}

	if _, ok := logs[0].(*startLog); !ok {
		return errors.New("transaction does not begin with startLog")
	}

	for i := len(logs) - 1; i > 0; i -= 1 {
		err := rm.Undo(logs[i])
		if err != nil {
			return err
		}
	}

	// commit the transaction after the rollback
	rm.Commit(clientId)
	err := rm.tm.Commit(clientId)
	return err
}

// Prime the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Delta should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
