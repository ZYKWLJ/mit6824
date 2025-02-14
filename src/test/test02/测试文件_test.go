package main

import "testing"

func TestIsPrime(t *testing.T) {
	var primeTests = []struct {
		input    int  // 输入
		expected bool // 期望结果
	}{
		{1, false},
		{2, true},
		{3, true},
		{4, false},
		{5, true},
		{6, false},
		{7, false}, // 这个是错误用例
	}
	for _, tt := range primeTests {
		actual := IsPrime(tt.input)
		if actual != tt.expected {
			t.Errorf("IsPrime(%d)=%v，预期为 %v", tt.input, actual, tt.expected)
		}
	}
}
