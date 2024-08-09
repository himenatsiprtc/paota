package utils

// Fibonacci calculates the nth Fibonacci number.
func Fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return Fibonacci(n-1) + Fibonacci(n-2)
}

// Fibonacci returns successive Fibonacci numbers starting from 1
func FibonacciSuccessive() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

// FibonacciNext returns next number in Fibonacci sequence greater than start
func FibonacciNext(start int) int {
	fib := FibonacciSuccessive()
	num := fib()
	for num <= start {
		num = fib()
	}
	return num
}
