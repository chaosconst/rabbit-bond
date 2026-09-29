// Package trace logs when the first blocks of each connection pass each stage, with
// microsecond timestamps (RABBIT_TRACE=1). Used to find where a new connection's time goes:
// client send -> tunnel -> server receive -> dial target -> target answers -> back.
package trace

import (
	"fmt"
	"os"
	"time"
)

// On is RABBIT_TRACE=1.
var On = os.Getenv("RABBIT_TRACE") == "1"

// FirstBlocks: only blocks with an ID below this are traced (the start of each connection).
const FirstBlocks = 4

// Logf writes one trace line: unix microseconds, then the event.
func Logf(format string, v ...interface{}) {
	if On {
		fmt.Fprintf(os.Stderr, "TRACE %d "+format+"\n", append([]interface{}{time.Now().UnixNano() / 1000}, v...)...)
	}
}
