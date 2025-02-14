package test01

func Age(age int) int {
	if age > 0 {
		println("年龄合规！")
		return age
	}
	println("年龄不合规！")
	return 0
}
