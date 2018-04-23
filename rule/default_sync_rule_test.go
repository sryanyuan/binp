package rule

import "testing"

func TestDefaultSyncRule(t *testing.T) {
	type Testcase struct {
		schema string
		table  string
		result bool
	}

	executeTestcase := func(r *DefaultSyncRule, ts []Testcase) {
		for i := range ts {
			testcase := &ts[i]
			result := r.CanSyncTable(testcase.schema, testcase.table)
			pass := result != nil
			if pass != testcase.result {
				t.Errorf("%v.%v should be %v, but got %v",
					testcase.schema, testcase.table, testcase.result, pass)
			}
		}
	}

	r := NewDefaultSyncRule()
	// Empty sync rule will pass all case
	ts := []Testcase{
		Testcase{
			schema: "hello",
			table:  "go",
			result: true,
		},
		Testcase{
			schema: "hello_1",
			table:  "go",
			result: true,
		},
	}
	executeTestcase(r, ts)
	// Set table regexp
	r.NewRule(&SyncDesc{
		Schema: "constant",
		Table:  "^table_\\d+$",
	})
	ts = []Testcase{
		Testcase{
			schema: "constant",
			table:  "table_0",
			result: true,
		},
		Testcase{
			schema: "hello",
			table:  "table_0",
			result: false,
		},
		Testcase{
			schema: "constant",
			table:  "table_",
			result: false,
		},
	}
	executeTestcase(r, ts)
	// Set schema regexp
	r = NewDefaultSyncRule()
	r.NewRule(&SyncDesc{
		Schema: "^db_\\d+$",
		Table:  "",
	})
	ts = []Testcase{
		Testcase{
			schema: "db_0",
			table:  "table_0",
			result: true,
		},
		Testcase{
			schema: "db_1000",
			table:  "table_0",
			result: true,
		},
		Testcase{
			schema: "db",
			table:  "table_",
			result: false,
		},
	}
	executeTestcase(r, ts)
	// Set schema && table regexp
	r = NewDefaultSyncRule()
	r.NewRule(&SyncDesc{
		Schema: "^db_\\d+$",
		Table:  "^table_\\d+$",
	})
	ts = []Testcase{
		Testcase{
			schema: "db_0",
			table:  "table_0",
			result: true,
		},
		Testcase{
			schema: "db_1000",
			table:  "table_0",
			result: true,
		},
		Testcase{
			schema: "db",
			table:  "table_",
			result: false,
		},
		Testcase{
			schema: "db_3",
			table:  "table_",
			result: false,
		},
		Testcase{
			schema: "db",
			table:  "table_0",
			result: false,
		},
	}
	executeTestcase(r, ts)
}
