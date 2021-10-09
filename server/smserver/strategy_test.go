package smserver

import (
	"reflect"
	"testing"
)

func Test_performAssignment(t *testing.T) {
	tests := []struct {
		totalLen     int
		shards       []string
		cws          containerWeightList
		expectResult map[string][]string
		expectTwice  []string
		expectErr    bool
	}{
		{
			totalLen: 2,
			shards:   []string{"s1", "s2"},
			cws: containerWeightList{
				&containerWeight{"c1", nil},
				&containerWeight{"c2", nil},
			},
			expectResult: map[string][]string{
				"c1": {"s1"},
				"c2": {"s2"},
			},
		},
		{
			totalLen: 3,
			shards:   []string{"s1", "s2", "s3"},
			cws: containerWeightList{
				&containerWeight{"c1", nil},
				&containerWeight{"c2", nil},
			},
			expectResult: map[string][]string{
				"c1": {"s1", "s2"},
				"c2": {"s3"},
			},
		},
		{
			totalLen: 2,
			shards:   []string{"s1", "s2"},
			cws: containerWeightList{
				&containerWeight{"c1", nil},
				&containerWeight{"c2", nil},
				&containerWeight{"c3", nil},
			},
			expectResult: map[string][]string{
				"c1": {"s1"},
				"c2": {"s2"},
			},
		},

		{
			totalLen: 3,
			shards:   []string{"s1", "s2", "s3"},
			cws: containerWeightList{
				&containerWeight{"c1", []string{"s3"}},
				&containerWeight{"c2", []string{"s2"}},
			},
			expectErr: true,
		},

		{
			totalLen: 5,
			shards:   []string{"s4", "s5"},
			cws: containerWeightList{
				&containerWeight{"c1", []string{"s1", "s3"}},
				&containerWeight{"c2", []string{"s2"}},
			},
			expectResult: map[string][]string{
				"c1": {"s4"},
				"c2": {"s5"},
			},
		},

		{
			totalLen: 4,
			shards:   []string{"s4"},
			cws: containerWeightList{
				&containerWeight{"c1", []string{"s1", "s2", "s3"}},
				&containerWeight{"c2", []string{}},
			},
			expectResult: map[string][]string{
				"c2": {"s4"},
			},
			expectTwice: []string{"s1"},
		},
	}

	for idx, tt := range tests {
		actualResult, actualTwice, actualErr := performAssignment(tt.totalLen, tt.shards, tt.cws)
		if tt.expectErr && actualErr == nil {
			t.Errorf("err idx %d expect %+v actual %+v", idx, tt.expectErr, actualErr)
			t.SkipNow()
		}
		if !reflect.DeepEqual(actualResult, tt.expectResult) {
			t.Errorf("result idx %d expect %+v actual %+v", idx, tt.expectResult, actualResult)
			t.SkipNow()
		}
		if !reflect.DeepEqual(actualTwice, tt.expectTwice) {
			t.Errorf("twice idx %d expect %+v actual %+v", idx, tt.expectTwice, actualTwice)
			t.SkipNow()
		}
	}
}
