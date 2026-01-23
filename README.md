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

---

### WHAT is PROTOCOL
A protocol is a defined set of rules or specifications that determine how computers or devices communicate with each other.
It describes:
- How data is structured — how information is broken into packets
- How packets are formatted — headers, metadata, and payload layout
- How data is sent and received — order, timing, and expectations

By following the same protocol, both sides know exactly how to interpret and reconstruct the data correctly.

---

## TCP vs UDP: Reliability and Packet Delivery

### Why HTTP-1 uses TCP: Reliable In-order Packets

**TCP Sliding Window with ACK (Acknowledgment)**

TCP ensures reliable, in-order delivery using a sliding window mechanism:

```
Sender                                    Receiver
------                                    --------

[Packet 1][Packet 2][Packet 3]
    |
    |----------->  Packet 1 arrives
                         |
                   ACK 1 <-----------
    |
Window slides →
[Packet 2][Packet 3][Packet 4]
    |
    |----------->  Packet 2 arrives
                         |
                   ACK 2 <-----------
    |
Window slides →
[Packet 3][Packet 4][Packet 5]
```

**How TCP Sliding Window works:**
1. Sender transmits packets within a window (e.g., packets 1-3)
2. Receiver acknowledges each packet with an ACK
3. As ACKs arrive, the window "slides" forward
4. Sender can now send newer packets (e.g., packet 4, 5...)
5. If no ACK is received (timeout), sender retransmits the packet

**Result:** Packets always arrive in order, no data loss, but slower due to waiting for ACKs.

---

### UDP: Fast but Unreliable (~1% packet loss)

**UDP Send-All-At-Once with NACK (Negative Acknowledgment)**

UDP doesn't guarantee delivery or order. For reliability, we need a custom protocol:

```
Sender                                    Receiver
------                                    --------

Send ALL packets at once:
[1][2][3][4][5][6][7][8]...
 |  |  |  |  |  |  |  |
 |  |  |  X  |  |  X  |--------→  Packets: 1,2,3,5,6,8 arrive
 |  |  |                          (4 and 7 lost - ~1% loss)
 |  |  |                                 |
 |  |  |                                 ↓
 |  |  |                          Check sequence: missing 4, 7
 |  |  |                                 |
 |  |  |                    NACK [4,7] <-----------
 |  |  |
Resend [4][7] only
    |  |
    |  |------------------→  Packets 4, 7 arrive
                                   |
                                   ↓
                            All packets received
                            Reconstruct data in order
```

**UDP with NACK strategy:**
1. **Send all packets** simultaneously (no waiting)
2. Each packet has a **sequence number** (1, 2, 3...)
3. Receiver checks for **missing sequence numbers**
4. Receiver sends **NACK** with list of missing packet IDs
5. Sender **retransmits only missing packets**
6. Receiver reconstructs data in correct order

**Benefits:**
- Much faster than TCP (no waiting for individual ACKs)
- Only ~1% packet loss to handle
- Good for real-time applications (video streaming, gaming)

**Trade-offs:**
- Need to implement your own reliability layer
- More complex application logic
- May need multiple NACK rounds if retransmitted packets also get lost

### TCP (Transmission Control Protocol)

Transmission Control Protocol (TCP) is a primary communication protocol of the internet, though that is changing with HTTP3 (which is not built on TCP) gaining adoption.

TCP is great because it allows ordered data to be safely sent across the internet. For example, let's say we want to send the message "i am live":

| text | binary   |
|------|----------|
| i    | 01101001 |
| a    | 01100001 |
| m    | 01101101 |
| l    | 01101100 |
| i    | 01101001 |
| v    | 01110110 |
| e    | 01100101 |

When data is sent over a network, it is sent in packets. Each message is split into packets, the packets are sent, they arrive (potentially) out of order, and they are reassembled on the other side. And without a protocol like TCP, you can't guarantee that the order is correct...

You might end up with "i am evil" instead of "i am live"! TCP solves this problem.

### UDP (User Datagram Protocol)

User Datagram Protocol (UDP) is often compared to TCP, as they are both transport layer protocols. Here are the high-level differences between the two:

| Feature        | TCP | UDP |
|----------------|-----|-----|
| Connection     | Yes | No  |
| Handshake      | Yes | No  |
| In Order       | Yes | No  |
| Blazingly Fast | No  | Yes |

TCP establishes a connection between sender and receiver with a handshake, and ensures that all the data is sent in order. UDP yeets the data to the receiver and hopes they can make sense of it.

![alt text](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/r16Ur2O-1271x720.png)
![alt text](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/ANc5LWX-778x702.png)
![alt text](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/ANc5LWX-778x702.png)

---

