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
| ---- | -------- |
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
| -------------- | --- | --- |
| Connection     | Yes | No  |
| Handshake      | Yes | No  |
| In Order       | Yes | No  |
| Blazingly Fast | No  | Yes |

TCP establishes a connection between sender and receiver with a handshake, and ensures that all the data is sent in order. UDP yeets the data to the receiver and hopes they can make sense of it.

![alt text](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/r16Ur2O-1271x720.png)
![alt text](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/ANc5LWX-778x702.png)

---

### Files vs. Network

Files and network connections behave very similarly - that's why we started by simply reading and writing to files, then updated our code to be a bit more abstract (the `getLinesChannel` function) so that it can handle both. From the perspective of your code, files and network connections are both just streams of bytes that you can read from and write to.

All of a sudden, Go's `io.Reader` (and the very similar `io.ReadCloser`) and `io.Writer` interfaces make a lot more sense, right? They're designed to work with any type of stream, whether it's a file, a network connection, or something else entirely.

### Pull vs. Push

When you read from a file, you're in control of the reading process. You decide:

- When to read
- How much to read
- When to stop reading

You **pull** data from the file.

When you read from a network connection, the data is **pushed** to you by the remote server. You don't have control over when the data arrives, how much arrives, or when it stops arriving. Your code has to be ready to receive it when it comes.

---

## Why HTTP? Why Not Just Use TCP?

### The Problem: TCP Doesn't Tell You What the Data Is

TCP guarantees that your data arrives **in-order** and **complete** - no problem there. But it doesn't tell you **what the data is**:

- Am I sending JSON?
- Is this an image?
- What am I requesting?
- How should I parse this?

**Without HTTP, TCP data is meaningless.** You have no way to parse it or understand what's being asked.

### Example: HTTP Request

```
GET /cats HTTP/1.1\r\n
Host: boot.dev\r\n
Authorization: Api-Key 423423\r\n
Accept: image/*\r\n
User-Agent: CatFetcher/1.0\r\n
\r\n
```

**HTTP gives structure to raw TCP bytes:**

- **Method** (`GET`) - What action are we performing?
- **Resource path** (`/cats`) - What are we requesting?
- **Headers/Field lines** - Metadata about the request (from RFC specifications)
- **Body** (optional) - Not all requests have a body; responses mostly do

### The Shape of HTTP Messages

All HTTP messages follow this format:

```
METHOD /resource-path PROTOCOL-VERSION\r\n
field-name: value\r\n
field-name: value\r\n
field-name: value\r\n
\r\n
[optional body]
```

**Everything in HTTP uses `\r\n` (CRLF - Carriage Return Line Feed):**

- `\r` = Carriage Return (move cursor to start of line) - ASCII code 13
- `\n` = Line Feed (move to next line) - ASCII code 10
- Together `\r\n` = End of line in HTTP protocol
- An empty line `\r\n\r\n` signals the end of headers and start of body

### Handling the Body

For the body, we need to know **how long it will be**. There are a couple of different strategies:

1. **Content-Length**: Specifies exactly how many bytes the body is

    ```
    Content-Length: 1234\r\n
    \r\n
    [1234 bytes of data]
    ```

2. **Chunked Transfer Encoding**: Send body in chunks, each prefixed with its size
    ```
    Transfer-Encoding: chunked\r\n
    \r\n
    5\r\n
    hello\r\n
    6\r\n
    world!\r\n
    0\r\n
    \r\n
    ```

### HTTP Versions: Same Semantics, Different Implementation

- **[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)** - HTTP Semantics (applies to all HTTP versions)
- **[RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html)** - HTTP/1.1 Message Syntax and Routing

**HTTP/1, HTTP/2, and HTTP/3 are semantically the same** - they all follow the same request/response model. The implementation is what differs:

- **HTTP/1.1**: Text-based, human-readable
- **HTTP/2**: Binary protocol, uses **HPACK** compression to shrink headers down to a couple of bytes
- **HTTP/3**: Built on UDP (not TCP!), uses **QPACK** compression

**The shape of the HTTP message is the same** across all versions - they just encode it differently on the wire.

---

## Seeing HTTP in Action: Raw TCP Stream

### Testing with TCP Listener and curl

Let's see what HTTP actually looks like when it arrives over TCP. We'll use our TCP listener to capture the raw bytes.

**Command 1: Start TCP listener and capture output**

```bash
go run ./cmd/tcpListener | tee /tmp/rawget.http
```

**What this does:**

- `go run ./cmd/tcpListener` - Starts our TCP server listening on port 42069
- `|` - Pipes the output to the next command
- `tee /tmp/rawget.http` - Writes output to **both** the terminal (so we can see it) **and** the file `/tmp/rawget.http` (so we can inspect it later)

**Command 2: Send an HTTP request with curl**

```bash
curl http://localhost:42069/coffee
```

**What curl sends:**

- An HTTP GET request to our TCP listener
- The request is just plain text sent over TCP
- curl formats it according to HTTP/1.1 specification

**What we received (printed line by line):**

```
read: GET /coffee HTTP/1.1
read: Host: localhost:42069
read: User-Agent: curl/8.7.1
read: Accept: */*
```

**Breaking it down:**

1. **`GET /coffee HTTP/1.1`** - The request line
    - Method: `GET`
    - Path: `/coffee`
    - Version: `HTTP/1.1`

2. **`Host: localhost:42069`** - Required header specifying the server
3. **`User-Agent: curl/8.7.1`** - Identifies the client making the request
4. **`Accept: */*`** - Client accepts any content type

**Key insight:** HTTP is just text! Our TCP listener reads it line by line (each ending with `\r\n`), and we can see the entire HTTP request structure. This is exactly what web servers do - they read TCP streams, parse HTTP messages, and respond accordingly.

The raw file at `/tmp/rawget.http` contains these exact bytes that traveled over the network. HTTP isn't magic - it's a text format sent over TCP!

---

HTTP works because plain text is binary. Because HTTP uses TCP, if the HTTP request or response is too big to fit into a single TCP packet it can be broken up into many packets and reconstructed in the correct order on the other side. TCP guarantees that the data is in order and complete.

At the heart of HTTP is the HTTP-message: the format that the text in an HTTP request or response must use. From [RFC 9112 Section 2.1](https://datatracker.ietf.org/doc/html/rfc9112#name-message-format):

```
start-line CRLF
*( field-line CRLF )
CRLF
[ message-body ]
```

[CRLF](https://developer.mozilla.org/en-US/docs/Glossary/CRLF) (written in plain text as `\r\n`) is a carriage return followed by a line feed. It's a Microsoft Windows (and HTTP) style newline character.

Let's break down each part:

| Part                  | Example                    | Description                                                    |
| --------------------- | -------------------------- | -------------------------------------------------------------- |
| start-line CRLF       | `POST /users/adi HTTP/1.1` | The request (for a request) or status (for a response) line    |
| \*( field-line CRLF ) | `Host: google.com`         | Zero or more lines of HTTP headers. These are key-value pairs. |
| CRLF                  |                            | A blank line that separates the headers from the body.         |
| [ message-body ]      | `{"name": "TheHTTPagen"}`  | The body of the message. This is optional.                     |

Both HTTP requests and responses follow this same format, though the contents of each section will differ, we'll get to that!

---

To understand HTTP it may behoove you to become familiar with the RFCs ("requests for comment"). At the moment, there are several key RFCs for HTTP/1.1:

- [RFC 7231](https://datatracker.ietf.org/doc/html/rfc7231) – An active and widely referenced RFC.
- [RFC 9112](https://datatracker.ietf.org/doc/html/rfc9112) – Easier to read than RFC 7231, relies on understanding from RFC 9110.
- [RFC 9110](https://datatracker.ietf.org/doc/html/rfc9110) – Covers HTTP "semantics."
- [RFC 2616](https://datatracker.ietf.org/doc/html/rfc2616) – Deprecated by RFC 7231.
  We will be referring to 9110 and 9112 as they have better separation of information. 7231 can be a bit verbose whereas 9112 is much more to the point, but makes a lot of assumptions that you understand a decent amount of 9110.

---

**cURL**
You probably already figured this out (or knew), but curl is a command line tool for making HTTP requests. Take a good look at the raw HTTP requests that we just sent to our tcplistener program.

```
GET /goodies HTTP/1.1       # start-line CRLF
Host: localhost:42069       # *( field-line CRLF )
User-Agent: curl/7.81.0     # *( field-line CRLF )
Accept: */*                 # *( field-line CRLF )
                            # CRLF
                            # [ message-body ] (empty)
```

```
POST /coffee HTTP/1.1            # start-line CRLF
Host: localhost:42069            # *( field-line CRLF )
User-Agent: curl/8.6.0           # *( field-line CRLF )
Accept: */*                      # *( field-line CRLF )
Content-Type: application/json   # *( field-line CRLF )
Content-Length: 22               # *( field-line CRLF )
                                 # CRLF
{"flavor":"dark mode"}          # [ message-body ]
```

I've annotated the parts for you. Remember, there are really just four parts to an HTTP message:

```
start-line CRLF
\*( field-line CRLF )
CRLF
[ message-body ]
```

---

## Parsing a Stream

Unfortunately, parsing code tends to be just one edge case after another. Remember how I said TCP guarantees data to be in order? That’s true, but I never said it had to be complete. TCP (and by extension, HTTP) is a streaming protocol, which means we receive data in chunks and should be able to parse it as it comes in.

So, instead of a full HTTP request, we might just get the first few characters, like this:

```
GE
```

We need to manage the state of our parser to handle incomplete reads. For example, maybe in the first pass, our parser only gets:

```
GE
```

It needs to be smart enough to know that it’s not done yet and keep reading until it gets the full request line:

```
GET /coffee HTTP/1.1
```

---

## State Parser Implementation

### How the Streaming Parser Works

The HTTP request parser in [`internal/request/request.go`](internal/request/request.go) handles incomplete data arriving in chunks. Here's how it works:

**Core Concept: Accumulate Until Complete**

The parser uses a **state machine** with two states:

- `StateInit`: Waiting to parse the request line
- `StateDone`: Request line successfully parsed

**The Critical Flow:**

```go
for !request.done() {                    // Keep looping until parsing is done
    n, err := reader.Read(buf[bufLen:])  // Read NEW data into buffer AFTER existing data
    bufLen += n                          // Track total data in buffer

    readN, err := request.parse(buf[:bufLen])  // Try to parse all accumulated data

    copy(buf, buf[readN:bufLen])         // Shift unparsed data to start of buffer
    bufLen -= readN                      // Update length
}
```

**Example with Chunked Data:**

Imagine `"GET /path HTTP/1.1\r\n"` arrives in 3 chunks:

**Chunk 1: `"GE"`**

1. Buffer: `"GE"`, bufLen: 2
2. `parseRequestLine("GE")` looks for `\r\n` → **NOT FOUND**
3. Returns `n=0` (zero bytes consumed!)
4. `if n == 0 { break outer }` → exits parse loop early
5. **State stays `StateInit`** → `!request.done()` is true → keeps looping
6. Buffer shift: `copy(buf, buf[0:2])` → buffer still `"GE"`

**Chunk 2: `"T /path HTT"`**

1. Reads into `buf[2:]` → Buffer: `"GET /path HTT"`, bufLen: 13
2. `parseRequestLine("GET /path HTT")` → still no `\r\n` → returns `n=0`
3. **State still `StateInit`** → loop continues

**Chunk 3: `"P/1.1\r\n"`**

1. Reads into `buf[13:]` → Buffer: `"GET /path HTTP/1.1\r\n"`, bufLen: 20
2. `parseRequestLine("GET /path HTTP/1.1\r\n")` → **FOUND `\r\n`!**
3. Parses: `["GET", "/path", "HTTP/1.1"]` → returns `rl, 20, nil`
4. Saves RequestLine, sets **State = StateDone**, returns `read=20`
5. **`request.done()` returns true** → loop exits ✅

**Key Mechanism:**

```go
// In parseRequestLine()
idx := bytes.Index(b, SEPARATOR)  // Look for \r\n
if idx == -1 {
    return nil, 0, nil  // Incomplete data - return 0 bytes consumed
}
```

When `parseRequestLine` can't find the delimiter (`\r\n`), it returns **0 bytes consumed**. This signals "I need more data." The state stays in `StateInit`, and the buffer **accumulates data across multiple reads** until there's enough to parse.

**Why the Buffer Shift?**

```go
copy(buf, buf[readN:bufLen])  // Move unparsed data to start
bufLen -= readN               // Adjust length
```

This handles cases where you've parsed some data but there's leftover bytes. It ensures the next read appends to unparsed data, not random buffer positions.

**Summary:**

- Data accumulates in the buffer across multiple reads
- Parser returns 0 when it needs more data (incomplete)
- State machine stays in `StateInit` until complete line is found
- Once `\r\n` is found, parses the request line and transitions to `StateDone`
- Loop exits only when parsing is complete

This pattern works for any streaming protocol where messages have delimiters or known boundaries!

---
