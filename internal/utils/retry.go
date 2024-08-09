package utils

import (
	"fmt"
	"time"

	"github.com/surendratiwari3/paota/logger"
)

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := FibonacciSuccessive()
	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)

			logger.ApplicationLogger.Info("Retrying in " + string(retryIn) + " seconds")

			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}
