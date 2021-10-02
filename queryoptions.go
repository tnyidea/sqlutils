package sqlutils

import (
	"encoding/json"
	"reflect"
)

type QueryOptions struct {
	FilterColumn string
	FilterValue  string
	SortColumn   string
	SortOrder    string
	Count        int
	RangeStart   int
	RangeEnd     int
}

func (p *QueryOptions) IsZero() bool {
	return reflect.DeepEqual(*p, QueryOptions{})
}

func (p *QueryOptions) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *QueryOptions) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}
