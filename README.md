### HTTP FROM TCP

## Learning Steps

### Step 1: Understanding Streams - File Reading vs TCP Connection Reading

**What are streams?**
A stream is a sequence of data that becomes available over time. Instead of loading all data into memory at once, you read it piece by piece. Think of it like drinking water from a tap - the water flows continuously, and you consume it bit by bit.

**Why files and TCP connections are similar:**
Both are streams of bytes that you read sequentially:
- **File**: Bytes stored on disk that you read from start to end
- **TCP connection**: Bytes arriving over the network that you read as they come in

The similarity isn't just because Go makes them implement the same interface - it's a fundamental concept. Whether data comes from disk or network, you're reading a sequence of bytes in chunks.

**Initial approach - Reading from a file:**
I first wrote code to read from [`messages.txt`](messages.txt) using a channel and goroutine:

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "log"
    "os"
)

func getLinesChannel(f io.ReadCloser) <-chan string {
    out := make(chan string, 1)

    go func() {
        defer f.Close()
        defer close(out)

        str := ""
        for {
            data := make([]byte, 8)  // Read 8 bytes at a time from the stream
            n, err := f.Read(data)
            if err != nil {
                break
            }
            
            data = data[:n]
            if i := bytes.IndexByte(data, '\n'); i != -1 {
                str += string(data[:i])
                data = data[i+1:]
                out <- str  // Send complete line
                str = ""
            } 

            str += string(data)
        }
        if len(str) != 0 {
            out <- str
        }
    }()

    return out
}

func main() {
    f, err := os.Open("messages.txt")
    if err != nil {
        log.Fatal(err)
    }

    lines := getLinesChannel(f)
    for line := range(lines) {
        fmt.Printf("read: %s\n", line)
    }
}
```

**Key insight about streams:**
Notice how we read small chunks (8 bytes) and accumulate them until we find a newline. This is exactly how network protocols work - data arrives in chunks, and you need to parse it incrementally. You don't know when the next chunk will arrive or how big it will be.

**Transition to TCP:**
Since both files and TCP connections are just streams of bytes, the same reading logic works! I replaced file opening with a TCP listener in [`main.go`](main.go). The [`getLinesChannel`](main.go) function doesn't care where the bytes come from - it just reads from the stream, finds line boundaries, and sends complete lines through the channel.

This pattern is universal in systems programming:
- HTTP servers read from TCP streams
- Database clients read from socket streams  
- Log parsers read from file streams
- All use the same core concept: **read chunks, parse boundaries, extract messages**