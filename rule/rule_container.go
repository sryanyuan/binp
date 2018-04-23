package rule

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
)

type syncDescRegexp struct {
	*SyncDesc
	reg *regexp.Regexp
}

// ruleContainer support constant key and regexp key search
type ruleContainer struct {
	consts map[string]*SyncDesc
	regs   []*syncDescRegexp
}

func (c *ruleContainer) init() {
	c.consts = make(map[string]*SyncDesc)
	c.regs = make([]*syncDescRegexp, 0, 64)
}

func (c *ruleContainer) add(key string, desc *SyncDesc) error {
	if strings.HasPrefix(key, "^") && strings.HasSuffix(key, "$") {
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
	if nil == c.consts && nil == c.regs {
		return nil
	}
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
