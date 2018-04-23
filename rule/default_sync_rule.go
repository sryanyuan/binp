package rule

import (
	"github.com/juju/errors"
)

// Sync errors
var (
	ErrRuleConflict = errors.New("Rule conflicts")
)

type defaultSchemaRule struct {
	ruleContainer
	desc SyncDesc
	// Table rule
	tablesRule map[string]*SyncDesc
	passAll    bool
}

func (r *defaultSchemaRule) addRule(desc *SyncDesc) error {
	var err error

	if nil == r.tablesRule {
		r.tablesRule = make(map[string]*SyncDesc)
	}
	// Initialize schema info
	if "" == r.desc.Schema && "" == r.desc.RewriteSchema {
		r.desc.Schema = desc.Schema
		r.desc.RewriteSchema = desc.RewriteSchema
	}
	if r.desc.Schema != desc.Schema || r.desc.RewriteSchema != desc.RewriteSchema {
		return errors.Trace(ErrRuleConflict)
	}
	// If desc.Table is empty, we should handle the rule as full pass rule
	if "" == desc.Table {
		r.passAll = true
		return nil
	}
	ts, ok := r.tablesRule[desc.Table]
	if ok {
		return errors.Trace(ErrRuleConflict)
	}
	// Make a copy
	ndesc := *desc
	ts = &ndesc
	r.tablesRule[desc.Table] = ts

	if err = r.ruleContainer.add(desc.Table, ts); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (r *defaultSchemaRule) canSync(table string) *SyncDesc {
	if nil == r.tablesRule {
		return nil
	}
	// Do search
	desc := r.find(table)
	if nil != desc {
		return desc
	}
	if r.passAll {
		// Full pass
		return &SyncDesc{
			Schema:        r.desc.Schema,
			RewriteSchema: r.desc.RewriteSchema,
			Table:         table,
			RewriteTable:  table,
		}
	}
	return nil
}

// DefaultSyncRule implements the rule of sync
type DefaultSyncRule struct {
	ruleContainer
	// Schema rule
	schemasRule map[string]*defaultSchemaRule
}

// NewDefaultSyncRule creates a new NewDefaultSyncRule
func NewDefaultSyncRule() *DefaultSyncRule {
	r := &DefaultSyncRule{}
	r.init()
	return r
}

// NewDefaultSyncRuleWithRules creates a new DefaultSyncRule with rules
func NewDefaultSyncRuleWithRules(rs []*SyncDesc) (*DefaultSyncRule, error) {
	r := NewDefaultSyncRule()
	for _, v := range rs {
		err := r.NewRule(v)
		if nil != err {
			return nil, errors.Trace(err)
		}
	}
	return r, nil
}

// NewDefaultSyncRuleFromJSON creates a new DefaultSyncRule from json data
func NewDefaultSyncRuleFromJSON(data []byte) (*DefaultSyncRule, error) {
	rc := NewDefaultSyncConfig()
	rs, err := rc.ParseJSON(data)
	if nil != err {
		return nil, errors.Trace(err)
	}
	return NewDefaultSyncRuleWithRules(rs)
}

// CanSyncTable implements ISyncRule CanSyncTable
func (r *DefaultSyncRule) CanSyncTable(schema, table string) *SyncDesc {
	if nil == r.schemasRule {
		// If is nil, always pass all schema and table
		return &SyncDesc{
			Schema:        schema,
			Table:         table,
			RewriteSchema: schema,
			RewriteTable:  table,
		}
	}
	// Find schema rule
	schemaDesc := r.find(schema)
	if nil == schemaDesc {
		return nil
	}
	schemaRule, ok := r.schemasRule[schemaDesc.Schema]
	if !ok {
		return nil
	}
	return schemaRule.canSync(table)
}

// NewRule implements ISyncRule NewRule
func (r *DefaultSyncRule) NewRule(desc *SyncDesc) error {
	if err := desc.Validate(); nil != err {
		return errors.Trace(err)
	}
	if nil == r.schemasRule {
		r.schemasRule = make(map[string]*defaultSchemaRule)
	}
	sr, ok := r.schemasRule[desc.Schema]
	if !ok {
		sr = &defaultSchemaRule{}
		sr.init()
	}
	if err := sr.addRule(desc); nil != err {
		return errors.Trace(err)
	}
	r.schemasRule[desc.Schema] = sr
	// Add schema search
	cpy := *desc
	if err := r.add(cpy.Schema, &cpy); nil != err {
		return errors.Trace(err)
	}
	return nil
}
