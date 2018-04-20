package rule

import "testing"

func TestDefaultSyncRule(t *testing.T) {
	type Testcase struct {
		r      *DefaultSyncRule
		schema string
		table  string
		result bool
	}

	executeTestcase := func(r *DefaultSyncRule, ts []Testcase) {
		for i := range ts {
			testcase := &ts[i]
			_, _, result := r.CanSyncTable(testcase.schema, testcase.table)
			if result != testcase.result {
				t.Errorf("%v.%v should %v, but got %v",
					testcase.schema, testcase.table, testcase.result, result)
			}
		}
	}
}
