package rule

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
)

// Sync errors
var (
	ErrRuleConflict = errors.New("Rule conflicts")
)

type syncDescRegexp struct {
	*SyncDesc
	reg *regexp.Regexp
}

type ruleContainer struct {
	consts map[string]*SyncDesc
	regs   []*syncDescRegexp
}

func (c *ruleContainer) init() {
	c.consts = make(map[string]*SyncDesc)
	c.regs = make([]*syncDescRegexp, 0, 64)
}

func (c *ruleContainer) add(key string, desc *SyncDesc) error {
	if strings.HasPrefix(key, "^") && strings.HasPrefix(key, "$") {
		// Key with regexp expression
		r, err := regexp.Compile(key)
		if nil != err {
			return errors.Trace(err)
		}
		c.regs = append(c.regs, &syncDescRegexp{reg: r, SyncDesc: desc})
	} else {
		c.consts[key] = desc
	}

	return nil
}

func (c *ruleContainer) find(key string) *SyncDesc {
	// Search for constant map
	v, ok := c.consts[key]
	if ok {
		return v
	}
	// Search for regexps
	for _, v := range c.regs {
		if v.reg.MatchString(key) {
			return v.SyncDesc
		}
	}
	return nil
}

func newRuleContainer() *ruleContainer {
	c := &ruleContainer{}
	c.init()
	return c
}

type defaultSchemaRule struct {
	ruleContainer
	desc SyncDesc
	// Table rule
	tablesRule map[string]*SyncDesc
}

func (r *defaultSchemaRule) addRule(desc *SyncDesc) error {
	var err error

	if nil == r.tablesRule {
		r.tablesRule = make(map[string]*SyncDesc)
	}
	ts, ok := r.tablesRule[desc.Table]
	if ok {
		return errors.Trace(ErrRuleConflict)
	}
	if "" == r.desc.Schema && "" == r.desc.RewriteSchema {
		r.desc.Schema = desc.Schema
		r.desc.RewriteSchema = desc.RewriteSchema
	}
	if r.desc.Schema != desc.Schema || r.desc.RewriteSchema != desc.RewriteSchema {
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

// CanSyncTable implements ISyncRule CanSyncTable
func (r *DefaultSyncRule) CanSyncTable(schema, table string) (string, string, bool) {
	if nil == r.schemasRule {
		// If is nil, always pass
		return schema, table, true
	}
	return schema, table, false
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
	if ok {
		return errors.Trace(ErrRuleConflict)
	}
	sr = &defaultSchemaRule{}
	sr.init()
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
