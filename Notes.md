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

## HTTP Headers Parsing Implementation

### Understanding HTTP Headers Structure

HTTP headers (also called "field lines" in RFC terminology) come after the request line and before the body:

```
GET /coffee HTTP/1.1\r\n          ← Request line
Host: localhost:42069\r\n         ← Header 1
User-Agent: curl/8.7.1\r\n        ← Header 2
Accept: */*\r\n                   ← Header 3
\r\n                              ← Empty line (signals end of headers)
[optional body]
```

**Key Rules from RFC 9110:**

1. **Header format:** `field-name: field-value\r\n`
2. **Field names are case-insensitive:** `Host` and `host` are the same
3. **Field names must be tokens:** Only certain characters allowed (alphanumeric + `#$%&'*+-.^_`|~`)
4. **No whitespace before colon:** `Host : value` is invalid, `Host: value` is valid
5. **Multiple headers with same name:** Should be combined with commas

### State Machine Evolution: Adding Headers State

Our parser now has **3 states** instead of 2:

```go
type parserState string
const (
    StateInit    parserState = "init"     // Parsing request line
    StateHeaders parserState = "headers"  // Parsing headers
    StateDone    parserState = "done"     // Parsing complete
)
```

**State Transitions:**

```
StateInit → StateHeaders → StateDone
```

1. **StateInit:** Parse request line until we find `\r\n`
2. **StateHeaders:** Parse headers until we find empty line `\r\n\r\n`
3. **StateDone:** Parsing complete, ready to use request

### How the Headers Parser Works

The [`Headers.Parse`](internal/headers/headers.go) method is designed to work with **streaming data** (just like `parseRequestLine`):

```go
func (h *Headers) Parse(data []byte) (int, bool, error)
```

**Returns:**

- `int`: Number of bytes consumed from `data`
- `bool`: Whether we've finished parsing all headers (found empty line)
- `error`: Any parsing errors

**The Parsing Loop:**

```go
read := 0
done := false

for {
    idx := bytes.Index(data[read:], clrf)  // Look for \r\n
    if idx == -1 {
        break  // Incomplete data - need more bytes
    }

    // Empty line signals end of headers
    if idx == 0 {
        done = true
        read += len(clrf)
        break
    }

    // Parse this header line
    name, value, err := parseHeader(data[read:read+idx])
    if err != nil {
        return 0, false, err
    }

    read += idx + len(clrf)
    h.Set(name, value)
}

return read, done, nil
```

**Key Points:**

1. **Returns 0 bytes consumed when incomplete:** If we can't find `\r\n`, we return `(read, false, nil)` to signal "give me more data"
2. **Empty line detection:** When `idx == 0`, it means we found `\r\n` immediately, which is the empty line separating headers from body
3. **Accumulates bytes consumed:** `read` tracks total bytes processed across multiple header lines

### Handling Multiple Headers with Same Name

**RFC Requirement:** Multiple headers with the same field name should be combined with commas.

**Example from the wild:**

```
Set-Cookie: sessionId=abc123
Set-Cookie: userId=42
```

Should be treated as:

```
Set-Cookie: sessionId=abc123,userId=42
```

**Our Implementation:**

```go
func (h *Headers) Set(name, value string) {
    name = strings.ToLower(name)  // Case-insensitive storage

    if v, ok := h.headers[name]; ok {
        // Header already exists - append with comma
        h.headers[name] = fmt.Sprintf("%s,%s", v, value)
    } else {
        // New header - just set it
        h.headers[name] = value
    }
}
```

**Why lowercase storage?**

HTTP header names are **case-insensitive** per RFC. `Host`, `host`, and `HOST` all refer to the same header. By storing everything lowercase, we ensure:

```go
h.Get("Host")      // Returns "localhost:42069"
h.Get("host")      // Returns "localhost:42069"
h.Get("HOST")      // Returns "localhost:42069"
```

### Header Validation: The `isToken` Function

Not all strings are valid header names. RFC 9110 defines a "token" as:

```
token = 1*tchar
tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." /
        "0"-"9" / "A"-"Z" / "^" / "_" / "`" / "a"-"z" / "|" / "~"
```

**Our implementation:**

```go
func isToken(str []byte) bool {
    for _, ch := range str {
        found := false
        if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' {
            found = true
        }
        switch ch {
            case '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
                found = true
        }

        if !found {
            return false
        }
    }
    return true
}
```

**Invalid examples:**

- `H©st:` - Contains invalid character ©
- `Host :` - Has trailing space (space is not a token character)
- `Ho st:` - Has space in the middle

### Integration with the State Machine

The headers state fits into our existing streaming parser in [`internal/request/request.go`](internal/request/request.go):

```go
func (r *Request) parse(data []byte) (int, error) {
    read := 0

outer:
    for {
        currentData := data[read:]

        switch r.State {
        case StateInit:
            // Parse request line...
            rl, n, err := parseRequestLine(currentData)
            if n == 0 { break outer }  // Need more data
            r.RequestLine = *rl
            read += n
            r.State = StateHeaders  // ← Transition to headers state

        case StateHeaders:
            // Parse headers...
            n, done, err := r.Headers.Parse(currentData)
            if n == 0 { break outer }  // Need more data
            read += n
            if done {
                r.State = StateDone  // ← Found empty line, done parsing
            }

        case StateDone:
            break outer
        }
    }
    return read, nil
}
```

**Flow Example with Chunked Data:**

```
Chunk 1: "GET /coffee HTTP/1.1\r\nHo"
→ StateInit: Parse request line (20 bytes) → StateHeaders
→ StateHeaders: Parse headers - no \r\n found → return 0 bytes
→ Buffer accumulates: "Ho"

Chunk 2: "st: localhost:42069\r\nUser-Agent"
→ StateHeaders: Parse "Host: localhost:42069\r\n" (23 bytes)
→ Buffer accumulates: "User-Agent"

Chunk 3: ": curl/8.7.1\r\n\r\n"
→ StateHeaders: Parse "User-Agent: curl/8.7.1\r\n" (24 bytes)
→ StateHeaders: Found empty line "\r\n" → done=true → StateDone
→ Parsing complete!
```

### Why This Design Works for Streaming

**The beauty of returning consumed bytes:**

Each parsing function returns how many bytes it consumed. The main loop uses this to:

1. **Skip parsed data:** `currentData := data[read:]`
2. **Handle incomplete data:** When `n == 0`, break and wait for more
3. **Accumulate across reads:** Buffer management in `RequestFromReader`

**Buffer Management Pattern:**

```go
buf := make([]byte, 1024)
bufLen := 0

for !request.done() {
    n, _ := reader.Read(buf[bufLen:])      // Read NEW data AFTER existing
    bufLen += n

    readN, _ := request.parse(buf[:bufLen]) // Parse ALL accumulated data

    copy(buf, buf[readN:bufLen])           // Shift unparsed data to start
    bufLen -= readN
}
```

This pattern handles **any** message size and **any** chunk size, which is exactly what you need for real network protocols!

### Testing Multiple Headers

From [`internal/headers/headers_test.go`](internal/headers/headers_test.go):

```go
headers = NewHeaders()
data = []byte("Host: localhost:42069\r\nHost: localhost:42069\r\n")
n, done, err = headers.Parse(data)

assert.Equal(t, "localhost:42069,localhost:42069", headers.Get("Host"))
```

The same header appears twice, and our implementation correctly combines them with a comma, as required by the RFC.

---

## HTTP Body Parsing - The Final State

### Adding StateBody to the State Machine

Our parser now has **4 states** to handle complete HTTP requests:

```go
type parserState string
const (
    StateInit    parserState = "init"     // Parsing request line
    StateHeaders parserState = "headers"  // Parsing headers
    StateBody    parserState = "body"     // Parsing body (if present)
    StateDone    parserState = "done"     // Parsing complete
)
```

**State Transitions:**

```
StateInit → StateHeaders → hasBody() check
                              ↓           ↓
                          StateBody    StateDone
                              ↓
                          StateDone
```

### The Body Parsing Challenge

**Key difference from request line and headers:**

- Request line has delimiter: `\r\n`
- Headers have delimiter: `\r\n` (and empty line `\r\n\r\n` signals end)
- **Body has NO delimiter!**

**Question: How do we know when the body ends?**

**Answer: The `Content-Length` header tells us!**

```
POST /submit HTTP/1.1\r\n
Host: localhost\r\n
Content-Length: 13\r\n          ← This tells us body is 13 bytes
\r\n
hello world!\n                  ← Exactly 13 bytes
```

### Implementation: StateBody Case

From [`internal/request/request.go`](internal/request/request.go):

```go
case StateBody:
    if len(currentData) == 0 {
        break outer  // No data to process
    }

    length := getInt(r.Headers, "content-length", 0)
    if length == 0 {
        panic("Chunked not implemented")
    }

    // Calculate how much to read from current chunk
    remaining := min(length-len(r.Body), len(currentData))

    r.Body += string(currentData[:remaining])
    read += remaining

    // Are we done?
    if len(r.Body) == length {
        r.State = StateDone
    }
```

**Breaking it down:**

1. **Check if we have data:** `if len(currentData) == 0` → nothing to process
2. **Get expected body size:** `length := getInt(r.Headers, "content-length", 0)`
3. **Calculate bytes to read:** `remaining := min(length - len(r.Body), len(currentData))`
    - `length - len(r.Body)` = how many bytes we still need
    - `len(currentData)` = how many bytes available right now
    - Take the **minimum** (can't read more than we have!)
4. **Accumulate body:** `r.Body += string(currentData[:remaining])`
5. **Track consumption:** `read += remaining`
6. **Check completion:** `if len(r.Body) == length` → we're done!

### Example: Body Arriving in Chunks

Request with 13-byte body arriving in **3 chunks**:

```
POST /submit HTTP/1.1\r\n
Host: localhost\r\n
Content-Length: 13\r\n
\r\n
hello world!\n
```

#### **Chunk 1: Headers + partial body `"hello"`**

After parsing headers, we transition to StateBody:

```go
case StateHeaders:
    // ... parsed headers, found empty line ...
    if done {
        if r.hasBody() {  // Content-Length: 13 → TRUE
            r.State = StateBody  // ← Go to body state
        } else {
            r.State = StateDone
        }
    }

// Next iteration of parse loop:
case StateBody:
    currentData = "hello"  // 5 bytes available

    length = 13            // Need 13 total
    remaining = min(13 - 0, 5) = 5

    r.Body = "hello"       // Accumulated so far
    read += 5

    len(r.Body) = 5, need 13 → NOT done yet
```

**State stays `StateBody`**, parse returns, outer loop reads more data.

#### **Chunk 2: `" world"`**

```go
case StateBody:
    currentData = " world"  // 6 bytes available

    length = 13
    remaining = min(13 - 5, 6) = 6

    r.Body = "hello world"  // Now 11 bytes
    read += 6

    len(r.Body) = 11, need 13 → Still NOT done
```

**State stays `StateBody`**, parse returns, outer loop reads more.

#### **Chunk 3: `"!\n"`**

```go
case StateBody:
    currentData = "!\n"     // 2 bytes available

    length = 13
    remaining = min(13 - 11, 2) = 2

    r.Body = "hello world!\n"  // Now 13 bytes
    read += 2

    len(r.Body) = 13, need 13 → COMPLETE! ✅
    r.State = StateDone
```

**State transitions to `StateDone`**, outer loop exits!

### The `hasBody()` Helper Function

After parsing headers, we need to decide: transition to StateBody or StateDone?

```go
func (r *Request) hasBody() bool {
    length := getInt(r.Headers, "content-length", 0)
    return length > 0
}
```

**Used in StateHeaders:**

```go
case StateHeaders:
    n, done, err := r.Headers.Parse(currentData)
    // ...
    if done {
        if r.hasBody() {       // Check if body exists
            r.State = StateBody // ← Parse body next
        } else {
            r.State = StateDone // ← No body, we're done!
        }
    }
```

**Examples:**

**GET request (no body):**

```
GET /coffee HTTP/1.1\r\n
Host: localhost\r\n
\r\n
```

- No `Content-Length` header
- `hasBody()` → FALSE
- StateHeaders → **StateDone** (skip StateBody entirely!)

**POST request (has body):**

```
POST /submit HTTP/1.1\r\n
Content-Length: 5\r\n
\r\n
hello
```

- `Content-Length: 5` exists
- `hasBody()` → TRUE
- StateHeaders → **StateBody** → StateDone

### Why `min()` is Critical

```go
remaining := min(length - len(r.Body), len(currentData))
```

**Scenario 1: Need more than we have**

- Need: 13 bytes total, have: 5 bytes so far
- Current chunk: 3 bytes
- `remaining = min(13 - 5, 3) = min(8, 3) = 3` ✅
- Read 3 bytes (all available), wait for more

**Scenario 2: Have more than we need**

- Need: 13 bytes total, have: 10 bytes so far
- Current chunk: 20 bytes (maybe next request started!)
- `remaining = min(13 - 10, 20) = min(3, 20) = 3` ✅
- Read only 3 bytes (exactly what we need), leave the rest

Without `min()`, we'd read too much and consume bytes from the **next HTTP request**!

### Body Parsing vs Request Line/Headers

**Similarities:**

- All accumulate data across multiple reads
- All return bytes consumed
- All handle incomplete data gracefully

**Key Difference:**

| Part         | Delimiter  | How we know it's complete           |
| ------------ | ---------- | ----------------------------------- |
| Request Line | `\r\n`     | Found the delimiter                 |
| Headers      | `\r\n\r\n` | Found empty line                    |
| **Body**     | **None!**  | **Counted bytes == Content-Length** |

Body parsing is **count-based**, not **delimiter-based**!

### The Complete Parse Loop with Body

```go
func (r *Request) parse(data []byte) (int, error) {
    read := 0

outer:
    for {
        currentData := data[read:]
        if len(currentData) == 0 {
            break outer
        }

        switch r.State {
        case StateInit:
            rl, n, err := parseRequestLine(currentData)
            if err != nil { return 0, err }
            if n == 0 { break outer }
            r.RequestLine = *rl
            read += n
            r.State = StateHeaders

        case StateHeaders:
            n, done, err := r.Headers.Parse(currentData)
            if err != nil { return 0, err }
            if n == 0 { break outer }
            read += n
            if done {
                if r.hasBody() {
                    r.State = StateBody  // ← Transition to body
                } else {
                    r.State = StateDone
                }
            }

        case StateBody:
            length := getInt(r.Headers, "content-length", 0)
            if length == 0 {
                panic("Chunked not implemented")
            }
            remaining := min(length-len(r.Body), len(currentData))
            r.Body += string(currentData[:remaining])
            read += remaining

            if len(r.Body) == length {
                r.State = StateDone  // ← Transition to done
            }

        case StateDone:
            break outer
        }
    }
    return read, nil
}
```

**The flow:**

```
Data arrives → Parse in current state → Consume bytes → Update state → Repeat
                                    ↓
                         If incomplete, return 0 and wait for more data
```

### Testing Body Parsing

From [`internal/request/request_test.go`](internal/request/request_test.go):

**Test 1: Body arrives in tiny chunks (3 bytes at a time)**

```go
reader := &chunkReader{
    data: "POST /submit HTTP/1.1\r\n" +
          "Content-Length: 13\r\n" +
          "\r\n" +
          "hello world!\n",
    numBytesPerRead: 3,  // Simulate slow network!
}

r, err := RequestFromReader(reader)
require.NoError(t, err)
assert.Equal(t, "hello world!\n", r.Body)
assert.Equal(t, "POST", r.RequestLine.Method)
```

**How it works:**

- Reads 3 bytes at a time
- Accumulates across ~23 read operations
- Successfully parses the complete request
- Proves the streaming parser handles any chunk size!

**Test 2: Request without body (GET)**

```go
reader := &chunkReader{
    data: "GET /coffee HTTP/1.1\r\n\r\n",
    numBytesPerRead: 10,
}

r, err := RequestFromReader(reader)
require.NoError(t, err)
assert.Equal(t, "", r.Body)  // No body
assert.Equal(t, "GET", r.RequestLine.Method)
```

**How it works:**

- After headers, `hasBody()` returns false
- State transitions: StateInit → StateHeaders → **StateDone**
- StateBody is **never entered**!

### Visual: Complete Request Parsing

```
┌─────────────────────────────────────────────────┐
│         Request Arrives in Chunks               │
│   "POST /submit HTTP/1.1\r\nCo"                │
│   "ntent-Length: 13\r\n\r\n"                   │
│   "hello"                                       │
│   " world!\n"                                   │
└────────────────┬────────────────────────────────┘
                 ▼
      ┌──────────────────────┐
      │ Buffer Management    │
      │ Accumulate + Shift   │
      └──────────┬───────────┘
                 ▼
      ┌──────────────────────┐
      │ StateInit            │
      │ Parse request line   │
      │ Found: POST /submit  │
      └──────────┬───────────┘
                 ▼
      ┌──────────────────────┐
      │ StateHeaders         │
      │ Parse headers        │
      │ Found Content-Length │
      │ Found empty line     │
      └──────────┬───────────┘
                 ▼
         ┌──────────────┐
         │ hasBody()?   │
         └───┬──────┬───┘
             │      │
        Yes  │      │ No
             ▼      ▼
      ┌──────────┐ ┌──────────┐
      │StateBody │ │StateDone │
      │Accumulate│ └──────────┘
      │13 bytes  │
      └────┬─────┘
           │
      ┌────▼──────────────────┐
      │ len(Body) == 13?      │
      │ YES → StateDone       │
      └───────────────────────┘
```

### Summary: Why Body Parsing Fits Perfectly

The body state integrates seamlessly into the streaming parser because:

1. **Same byte-counting pattern:** Returns bytes consumed, accumulates across reads
2. **Clear completion condition:** `len(Body) == Content-Length`
3. **State machine clarity:** One state, one responsibility (accumulate body bytes)
4. **Memory efficient:** Can handle large bodies without buffering entire request
5. **Works with any chunk size:** From 1-byte reads to full-body reads

The body is just **counted accumulation** instead of **delimiter-based parsing**, but the fundamental streaming approach remains identical! 🎯

---

## Building an HTTP Server: From Request to Response

Now that we can **parse** HTTP requests, we need to **generate** HTTP responses and tie everything together into a working server!

### HTTP Response Structure

Just like requests, HTTP responses follow a specific format:

```
HTTP/1.1 200 OK\r\n                    ← Status line
Content-Length: 10\r\n                 ← Headers
Content-Type: text/plain\r\n
Connection: close\r\n
\r\n                                   ← Empty line
All good \n                            ← Body
```

**Three parts:**

1. **Status line:** `HTTP/1.1 [status code] [status text]\r\n`
2. **Headers:** Same format as request headers
3. **Body:** The response content (HTML, JSON, plain text, etc.)

### The Response Package

From [`internal/response/response.go`](internal/response/response.go):

#### **Status Codes**

```go
type StatusCode int

const (
    StatusOK                  StatusCode = 200
    StatusBadRequest          StatusCode = 400
    StatusInternalServerError StatusCode = 500
)
```

These represent common HTTP status codes. In the real world, there are many more (404 Not Found, 201 Created, 301 Redirect, etc.), but we start with the essentials.

#### **Writing the Status Line**

```go
func WriteStatusLine(w io.Writer, statusCode StatusCode) error {
    statusLine := []byte{}
    switch statusCode {
    case StatusOK:
        statusLine = []byte("HTTP/1.1 200 OK\r\n")
    case StatusBadRequest:
        statusLine = []byte("HTTP/1.1 400 Bad Request\r\n")
    case StatusInternalServerError:
        statusLine = []byte("HTTP/1.1 500 Internal Server Error\r\n")
    default:
        return fmt.Errorf("unrecognized error code")
    }

    _, err := w.Write(statusLine)
    return err
}
```

**What this does:**

- Takes a status code and converts it to the proper HTTP format
- Writes it to any `io.Writer` (could be TCP connection, buffer, file, etc.)
- Returns error if something goes wrong

**Example output:** `HTTP/1.1 200 OK\r\n`

#### **Default Headers Helper**

```go
func GetDefaultHeaders(contentLen int) *headers.Headers {
    h := headers.NewHeaders()
    h.Set("Content-Length", fmt.Sprintf("%d", contentLen))
    h.Set("Connection", "close")
    h.Set("Content-Type", "text/plain")

    return h
}
```

**Why default headers?**

- **Content-Length:** Tells the client how many bytes to expect in the body
- **Connection: close:** Tells the client we'll close the connection after this response
- **Content-Type:** Tells the client the body is plain text (not HTML, JSON, etc.)

These are the **minimum headers** needed for a valid HTTP response.

#### **Writing Headers**

```go
func WriteHeaders(w io.Writer, headers *headers.Headers) error {
    b := []byte{}
    headers.Iterate(func(key, value string) {
        b = fmt.Appendf(b, "%s: %s\r\n", key, value)
    })
    b = fmt.Append(b, "\r\n")  // ← Empty line signals end of headers!
    _, err := w.Write(b)

    return err
}
```

**What this does:**

1. Iterates over all headers using the `Iterate` method we added earlier
2. Formats each as `Key: Value\r\n`
3. Adds the crucial **empty line** (`\r\n`) at the end
4. Writes everything at once

**Example output:**

```
Content-Length: 10\r\n
Connection: close\r\n
Content-Type: text/plain\r\n
\r\n
```

### The Server Package

From [`internal/server/server.go`](internal/server/server.go):

#### **Handler Function Type**

```go
type HandlerError struct {
    StatusCode response.StatusCode
    Message    string
}

type Handler func(w io.Writer, req *request.Request) *HandlerError
```

**Key design decision:** Handlers are **functions** that:

- Take a writer (to write response body) and a request
- Return either `nil` (success) or a `HandlerError` (something went wrong)

**Why return error as a struct?**

- Handler can specify **what went wrong** (StatusCode + Message)
- Server can decide how to format the error response
- Separates business logic from HTTP protocol details

#### **Processing a Single Connection**

```go
func runConnection(s *Server, conn io.ReadWriteCloser) {
    defer conn.Close()  // Always close connection when done

    headers := response.GetDefaultHeaders(0)

    // Step 1: Parse the request
    r, err := request.RequestFromReader(conn)
    if err != nil {
        // Parsing failed - send 400 Bad Request
        response.WriteStatusLine(conn, response.StatusBadRequest)
        response.WriteHeaders(conn, headers)
        return
    }

    // Step 2: Run the user's handler (writes to buffer, not connection)
    writer := bytes.NewBuffer([]byte{})
    handlerError := s.handler(writer, r)

    // Step 3: Determine response status and body
    var body []byte = nil
    var status response.StatusCode = response.StatusOK
    if handlerError != nil {
        status = handlerError.StatusCode
        body = []byte(handlerError.Message)
    } else {
        body = writer.Bytes()
    }

    // Step 4: Update Content-Length (now we know body size!)
    headers.Replace("Content-Length", fmt.Sprintf("%d", len(body)))

    // Step 5: Write response to connection
    response.WriteStatusLine(conn, status)
    response.WriteHeaders(conn, headers)
    conn.Write(body)
}
```

**The Flow Explained:**

**1. Parse the request:**

```go
r, err := request.RequestFromReader(conn)
```

Uses our streaming parser! If parsing fails (malformed HTTP), return 400 Bad Request immediately.

**2. Run handler into a buffer:**

```go
writer := bytes.NewBuffer([]byte{})
handlerError := s.handler(writer, r)
```

**Why a buffer and not write directly to the connection?**

We need to know the body size **before** writing headers! Remember, `Content-Length` must be in the headers:

```
Content-Length: 10\r\n    ← Need this FIRST
\r\n
All good \n               ← Body comes AFTER
```

By writing to a buffer first, we can:

- Let the handler write whatever it wants
- Measure the size
- Set `Content-Length` correctly
- Then send everything in the right order

**3. Determine status and body:**

```go
if handlerError != nil {
    status = handlerError.StatusCode
    body = []byte(handlerError.Message)
} else {
    body = writer.Bytes()
}
```

If handler returned an error, use the error message as body. Otherwise, use what the handler wrote.

**4. Update Content-Length:**

```go
headers.Replace("Content-Length", fmt.Sprintf("%d", len(body)))
```

Now we know the actual body size, update the header!

**5. Write response:**

```go
response.WriteStatusLine(conn, status)    // HTTP/1.1 200 OK\r\n
response.WriteHeaders(conn, headers)       // Headers + \r\n
conn.Write(body)                           // Body
```

Everything written in the **correct order** to the TCP connection.

#### **Running the Server Loop**

```go
func runServer(s *Server, listener net.Listener) {
    for {
        conn, err := listener.Accept()  // Wait for connection
        if s.closed {
            return
        }

        if err != nil {
            return
        }
        go runConnection(s, conn)  // Handle in goroutine (concurrent!)
    }
}
```

**What this does:**

1. **Accept:** Blocks until a client connects
2. **Check if closed:** Allows graceful shutdown
3. **Spawn goroutine:** Each connection handled concurrently (multiple clients at once!)

**Why `go runConnection()`?**

Without the `go` keyword, the server would handle one request at a time:

```
Client 1 connects → Process → Close → Client 2 connects → Process → ...
```

With `go`, multiple clients can be served simultaneously:

```
Client 1 connects → go Process
Client 2 connects → go Process  (Client 1 still processing!)
Client 3 connects → go Process  (Clients 1 & 2 still processing!)
```

This is **concurrent request handling** - essential for real servers!

#### **Starting the Server**

```go
func Serve(port uint16, handler Handler) (*Server, error) {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        return nil, err
    }
    server := &Server{closed: false, handler: handler}
    go runServer(server, listener)  // Run in background

    return server, nil
}
```

**What this does:**

1. **Create TCP listener:** Binds to port (e.g., `:42069`)
2. **Create server struct:** Stores handler function and closed state
3. **Start server loop in goroutine:** Doesn't block, returns immediately
4. **Return server handle:** Allows caller to shut down later

**Why run in goroutine?**

So `Serve()` returns immediately and doesn't block. The main program can do other things (like wait for SIGINT):

```go
server, _ := server.Serve(42069, myHandler)
// Server is running in background now!
// Main program continues...
<-sigChan  // Wait for Ctrl+C
server.Close()  // Gracefully shut down
```

### Using the Server: Example Application

From [`cmd/httpServer/main.go`](cmd/httpServer/main.go):

```go
server, err := server.Serve(port, func(w io.Writer, req *request.Request) *server.HandlerError {
    if req.RequestLine.RequestTarget == "/problem" {
        return &server.HandlerError{
            StatusCode: response.StatusBadRequest,
            Message:    "Bad Request encountered!",
        }
    } else if req.RequestLine.RequestTarget == "/woopsie-daisy" {
        return &server.HandlerError{
            StatusCode: response.StatusInternalServerError,
            Message:    "Woopsie internal Server Error encountered!",
        }
    } else {
        w.Write([]byte("All good \n"))
    }

    return nil
})
```

**Handler logic:**

- **Route: `/problem`** → Return 400 Bad Request
- **Route: `/woopsie-daisy`** → Return 500 Internal Server Error
- **Any other route** → Write "All good \n" and return 200 OK

**Testing it:**

```bash
curl http://localhost:42069/hello
# Response:
# HTTP/1.1 200 OK
# Content-Length: 10
# Connection: close
# Content-Type: text/plain
#
# All good

curl http://localhost:42069/problem
# Response:
# HTTP/1.1 400 Bad Request
# Content-Length: 26
# Connection: close
# Content-Type: text/plain
#
# Bad Request encountered!
```

### Graceful Shutdown

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan  // Blocks until Ctrl+C or kill signal
log.Println("Server gracefully stopped")
```

**What this does:**

1. Creates a channel to receive OS signals
2. Registers for SIGINT (Ctrl+C) and SIGTERM (kill)
3. Blocks until signal received
4. Logs and exits gracefully

**Why this matters:**

Without signal handling:

- Server runs forever, can't be stopped cleanly
- Ctrl+C just kills the process abruptly

With signal handling:

- Ctrl+C triggers graceful shutdown
- Server can finish in-flight requests
- Clean exit with proper logging

### The Complete Request-Response Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Client sends HTTP request over TCP                  │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Server: listener.Accept() - Accept connection       │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 3. Spawn goroutine: go runConnection()                 │
│    (Server can now accept more connections!)           │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 4. Parse request: request.RequestFromReader()          │
│    - StateInit: Parse request line                     │
│    - StateHeaders: Parse headers                       │
│    - StateBody: Parse body (if Content-Length > 0)     │
│    - StateDone: Request complete!                      │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Run handler: handler(buffer, request)               │
│    - Check route (/problem, /woopsie-daisy, etc.)     │
│    - Write response body to buffer                     │
│    - Return HandlerError or nil                        │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 6. Determine response status and body                  │
│    - Error? Use error status + message                 │
│    - Success? Use 200 OK + buffer contents             │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 7. Update Content-Length header                        │
│    (Now we know body size!)                            │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 8. Write response to TCP connection                    │
│    a. WriteStatusLine: HTTP/1.1 200 OK\r\n            │
│    b. WriteHeaders: Content-Length: 10\r\n...\r\n     │
│    c. Write body: All good \n                          │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│ 9. Close connection (Connection: close)                │
└─────────────────────────────────────────────────────────┘
```

### Key Design Patterns Used

**1. io.Writer Interface for Flexibility**

```go
func WriteStatusLine(w io.Writer, statusCode StatusCode)
func WriteHeaders(w io.Writer, headers *headers.Headers)
```

These functions accept **any** `io.Writer`:

- TCP connection: Write directly to client
- Buffer: Collect data before sending
- File: Log responses to disk
- Test: Capture output for assertions

This is **interface-based design** - makes code reusable and testable!

**2. Buffering Strategy**

```go
writer := bytes.NewBuffer([]byte{})
handlerError := s.handler(writer, r)
body := writer.Bytes()
```

**Problem:** We need `Content-Length` header **before** body, but we don't know body size until handler finishes.

**Solution:** Write to buffer first, measure, then send with headers. This is a common pattern in HTTP servers!

**3. Goroutines for Concurrency**

```go
go runConnection(s, conn)
```

Each connection handled in a separate goroutine → **concurrent request handling** for free! Go's runtime manages thousands of goroutines efficiently.

**4. Error Handling via Return Values**

```go
type HandlerError struct {
    StatusCode response.StatusCode
    Message    string
}
```

Instead of panicking or returning generic errors, handlers return **structured errors** that the server can format as proper HTTP responses.

**5. Graceful Shutdown with Signals**

```go
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
```

Production servers need to shut down cleanly. Signal handling allows:

- Finishing in-flight requests
- Closing connections gracefully
- Cleaning up resources

### Testing Your Server

**Start the server:**

```bash
go run ./cmd/httpServer
```

**Test successful request:**

```bash
curl -v http://localhost:42069/hello
```

Output:

```
HTTP/1.1 200 OK
Content-Length: 10
Connection: close
Content-Type: text/plain

All good
```

**Test error handling:**

```bash
curl -v http://localhost:42069/problem
```

Output:

```
HTTP/1.1 400 Bad Request
Content-Length: 26
Connection: close
Content-Type: text/plain

Bad Request encountered!
```

**Test with POST body:**

```bash
curl -X POST http://localhost:42069/test \
  -H "Content-Type: application/json" \
  -d '{"hello":"world"}'
```

The server will:

1. Parse the request line
2. Parse headers (including Content-Type)
3. Parse body (12 bytes based on Content-Length)
4. Run handler
5. Send response

### Summary: Building on the Foundation

**What we built:**

1. **Request Parser** (previous work)
    - Streaming parser with state machine
    - Handles chunked data
    - Parses request line, headers, and body

2. **Response Generator** (new!)
    - Formats HTTP responses
    - Handles status codes and headers
    - Writes data in correct order

3. **Server** (new!)
    - Accepts TCP connections
    - Parses requests using our parser
    - Runs user-defined handlers
    - Generates and sends responses
    - Handles multiple clients concurrently

**Why this architecture works:**

- **Separation of concerns:** Request parsing, response generation, and server logic are separate
- **Reusable components:** Response writer can be used in any Go program
- **Testable:** Each component can be tested independently
- **Concurrent:** Goroutines handle multiple clients simultaneously
- **Standards-compliant:** Follows HTTP/1.1 specification

You've now built a **working HTTP server from scratch** using only TCP sockets and the HTTP specification! This is the same fundamental architecture used by production servers like nginx, Apache, and Go's `net/http` package. 🚀

---
