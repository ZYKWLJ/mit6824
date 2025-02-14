package main

import "fmt"

// 回调函数定义
// 简单来说，回调函数就是一个函数，它作为参数传递给另一个函数，然后在适当的时机被调用。在 Go 语言中，回调函数的定义通常是通过 函数类型 来实现的。

// 1.定义回调函数类型(这是在Go语言中特有的，能够将函数定义为类型,C++中只能传指针)
type Callback func(int) int

// 2.定义回调函数(符合Callback类型)
func square(x int) int {
	return x * x
}

func double(x int) int {
	return x + x
}

// 3.定义一个函数，将回调函数作为参数
func processData(x int, callback Callback) int { //这不就是C++中的可调用对象传参吗！
	return callback(x)
}

// 主函数
func main() {
	//4.使用回调函数
	res1 := processData(5, square)
	res2 := processData(5, double)
	fmt.Println("square of 5=", res1, "\ndouble of 5=", res2)
}
