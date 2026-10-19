package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrWorkflowNotFound = errors.New("workflow not found")

// Workflow is the accepted batch and its nodes in submission order. Request is
// encoded by the manager; the store preserves it without interpreting it.
type Workflow struct {
	ID         string
	TenantID   string
	State      string
	Request    []byte
	CreatedAt  time.Time
	FinishedAt *time.Time
	Version    int64
	Nodes      []WorkflowNode
}

// WorkflowNode links an execution to the batch. Event is the execution's
// initial event on creation and on reads; later events use EventsAfter.
type WorkflowNode struct {
	NodeID    string
	DependsOn []string
	Execution Execution
	Event     Event
}

// CreateWorkflow commits the workflow, every node execution, and each first
// event in one transaction. A failed node write leaves no accepted batch.
func (s *Store) CreateWorkflow(ctx context.Context, record Workflow) error {
	if record.ID == "" || record.TenantID == "" || record.State == "" || len(record.Request) == 0 || record.CreatedAt.IsZero() || record.FinishedAt != nil || len(record.Nodes) == 0 {
		return errors.New("incomplete workflow")
	}
	dependencies := make([][]byte, len(record.Nodes))
	for i, node := range record.Nodes {
		if node.NodeID == "" || node.Execution.TenantID != record.TenantID {
			return errors.New("incomplete workflow node")
		}
		if err := validateCreate(node.Execution, node.Event); err != nil {
			return err
		}
		var err error
		dependencies[i], err = json.Marshal(node.DependsOn)
		if err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO workflows
		(id, tenant_id, state, request, created_at_ns, version)
		VALUES (?, ?, ?, ?, ?, 1)`, record.ID, record.TenantID, record.State, record.Request, record.CreatedAt.UnixNano()); err != nil {
		return err
	}
	for i, node := range record.Nodes {
		if err := insertExecution(ctx, tx, node.Execution, node.Event); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_nodes
			(workflow_id, node_id, position, depends_on, execution_id)
			VALUES (?, ?, ?, ?, ?)`, record.ID, node.NodeID, i, dependencies[i], node.Execution.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetWorkflow reads one committed batch and its initial node events from a
// consistent database snapshot.
func (s *Store) GetWorkflow(ctx context.Context, id string) (Workflow, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Workflow{}, err
	}
	defer tx.Rollback()
	var record Workflow
	var createdAtNS int64
	var finishedAtNS sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT id, tenant_id, state, request, created_at_ns, finished_at_ns, version
		FROM workflows WHERE id = ?`, id).Scan(&record.ID, &record.TenantID, &record.State, &record.Request, &createdAtNS, &finishedAtNS, &record.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return Workflow{}, ErrWorkflowNotFound
	}
	if err != nil {
		return Workflow{}, err
	}
	record.CreatedAt = time.Unix(0, createdAtNS).UTC()
	if finishedAtNS.Valid {
		finished := time.Unix(0, finishedAtNS.Int64).UTC()
		record.FinishedAt = &finished
	}
	var nodeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_nodes WHERE workflow_id = ?`, id).Scan(&nodeCount); err != nil {
		return Workflow{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT n.node_id, n.depends_on,
		e.id, e.tenant_id, e.state, e.snapshot, e.request, e.created_at_ns, e.version,
		v.kind, v.payload, v.occurred_at_ns
		FROM workflow_nodes AS n
		JOIN executions AS e ON e.id = n.execution_id
		JOIN execution_events AS v ON v.execution_id = e.id AND v.sequence = 1
		WHERE n.workflow_id = ? ORDER BY n.position`, id)
	if err != nil {
		return Workflow{}, err
	}
	for rows.Next() {
		var node WorkflowNode
		var rawDependencies []byte
		var executionCreatedAtNS, eventOccurredAtNS int64
		if err := rows.Scan(&node.NodeID, &rawDependencies,
			&node.Execution.ID, &node.Execution.TenantID, &node.Execution.State,
			&node.Execution.Snapshot, &node.Execution.Request, &executionCreatedAtNS, &node.Execution.Version,
			&node.Event.Kind, &node.Event.Payload, &eventOccurredAtNS); err != nil {
			rows.Close()
			return Workflow{}, err
		}
		if err := json.Unmarshal(rawDependencies, &node.DependsOn); err != nil {
			rows.Close()
			return Workflow{}, err
		}
		node.Execution.CreatedAt = time.Unix(0, executionCreatedAtNS).UTC()
		node.Event.ExecutionID = node.Execution.ID
		node.Event.Sequence = 1
		node.Event.OccurredAt = time.Unix(0, eventOccurredAtNS).UTC()
		record.Nodes = append(record.Nodes, node)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Workflow{}, err
	}
	if nodeCount == 0 || len(record.Nodes) != nodeCount {
		return Workflow{}, fmt.Errorf("workflow %q has incomplete nodes: read %d of %d", id, len(record.Nodes), nodeCount)
	}
	if err := tx.Commit(); err != nil {
		return Workflow{}, err
	}
	return record, nil
}

// ListWorkflows returns complete batches in admission order for startup
// reconstruction. Startup holds the runtime claim, so no other manager writes.
func (s *Store) ListWorkflows(ctx context.Context) ([]Workflow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM workflows ORDER BY created_at_ns, id`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	workflows := make([]Workflow, 0, len(ids))
	for _, id := range ids {
		workflow, err := s.GetWorkflow(ctx, id)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, workflow)
	}
	return workflows, nil
}

// TransitionWorkflow updates the summary with version comparison. Node state
// and event transitions use Transition; startup can reconstruct a summary if
// the process stops between a node commit and this summary update.
func (s *Store) TransitionWorkflow(ctx context.Context, id string, expectedVersion int64, state string, finishedAt *time.Time) (int64, error) {
	if id == "" || expectedVersion < 1 || state == "" {
		return 0, errors.New("incomplete workflow transition")
	}
	var finishedAtNS any
	if finishedAt != nil {
		finishedAtNS = finishedAt.UnixNano()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE workflows SET state = ?, finished_at_ns = ?, version = version + 1
		WHERE id = ? AND version = ?`, state, finishedAtNS, id, expectedVersion)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflows WHERE id = ?)`, id).Scan(&exists); err != nil {
			return 0, err
		}
		if !exists {
			return 0, ErrWorkflowNotFound
		}
		return 0, ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return expectedVersion + 1, nil
}
