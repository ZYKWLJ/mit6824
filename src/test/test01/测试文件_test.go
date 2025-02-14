package test01

import "testing"

func TestAge(t *testing.T) {
	var (
		input  = 0
		expect = 0
	)
	actual := Age(input)
	if actual != expect {
		t.Errorf("expect %d, actual %d", expect, actual)
	}
}
