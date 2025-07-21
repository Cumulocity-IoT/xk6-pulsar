package pulsar

import "time"

const UnlimitedMessageCount int64 = -1

// invokeForDuration repeatedly invokes publishFunc at the given interval for
// a specified duration or until the maximum count is reached.
//
// Parameters:
//   - durationMillis: total duration in milliseconds to keep publishing.
//   - maxCount: maximum number times to invoke publishFunc. If set to -1, publishFunc is invoked until the duration elapses.
//   - funcToInvoke: Function to invoke.
//
// Returns:
//   - The number of successful invocations.
//     If maxCount is -1, publishFunc is invoked until the duration elapses, ignoring the count limit.
func (c *client) invokeForDuration(
	durationMillis, maxCount int64,
	funcToInvoke func() error,
) (int64, error) {
	var count int64
	deadline := time.Now().Add(time.Duration(durationMillis) * time.Millisecond)

	for time.Now().Before(deadline) && (maxCount == UnlimitedMessageCount || count < maxCount) {
		err := funcToInvoke()
		if err == nil {
			count++
		} else {
			return count, err
		}
	}

	if count == maxCount && time.Now().Before(deadline) {
		remaining := deadline.Sub(time.Now())
		if remaining > 0 {
			time.Sleep(remaining)
		}
	}

	return count, nil
}
