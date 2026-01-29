# HTTP Server from Scratch in Go

A production-grade HTTP/1.1 server implementation built from the ground up using only TCP sockets and the HTTP specification (RFC 9110 & RFC 9112). This project demonstrates how HTTP works under the hood by implementing a streaming parser, request/response handling, and concurrent connection management.

## 📁 Project Structure

```
.
├── cmd/
│   ├── httpServer/          # Main HTTP server application
│   │   └── main.go         # Routes: /, /video, /httpbin/*
│   ├── tcpListener/         # Raw TCP listener (debugging)
│   │   └── main.go
│   └── udpSender/           # UDP client (experiments)
│       └── main.go
├── internal/
│   ├── headers/             # HTTP header parsing
│   │   ├── headers.go      # Set, Get, Parse, Iterate
│   │   └── headers_test.go
│   ├── request/             # HTTP request parsing
│   │   ├── request.go      # State machine parser
│   │   └── request_test.go # Chunked reader tests
│   ├── response/            # HTTP response generation
│   │   └── response.go     # Writer, status codes, headers
│   └── server/              # HTTP server implementation
│       └── server.go       # Accept, handle, respond
├── assets/
│   └── demo.mp4            # Test video file
├── Notes.md                # Implementation notes (detailed)
├── README.md               # This file
└── go.mod
```

### Testing Endpoints

**1. Basic HTML Response:**

```bash
curl http://localhost:42069/
# Returns: 200 OK with HTML page
```

**2. Error Handling:**

```bash
curl http://localhost:42069/problem
# Returns: 400 Bad Request with error HTML

curl http://localhost:42069/woopsie-daisy
# Returns: 500 Internal Server Error
```

**3. Binary Data (Video):**

```bash
curl http://localhost:42069/video --output test.mp4
# Downloads demo.mp4

# Or open in browser:
open http://localhost:42069/video
```

**4. Chunked Transfer Encoding (Streaming):**

```bash
curl http://localhost:42069/httpbin/stream/10
# Streams 10 chunks from httpbin.org
# Uses Transfer-Encoding: chunked
# Includes trailers: X-Content-SHA256, X-Content-Length
```

**5. POST with Body:**

```bash
curl -X POST http://localhost:42069/test \
  -H "Content-Type: application/json" \
  -d '{"hello":"world"}'
# Returns: 200 OK (parses body correctly)
```

## 🎓 How It Works

### 1. Streaming Parser (State Machine)

HTTP requests arrive over TCP in arbitrary chunks. The parser handles incomplete data:

```go
// Data might arrive like this:
Chunk 1: "GET /path HT"           // Incomplete request line
Chunk 2: "TP/1.1\r\nHost: loc"   // Complete line + partial header
Chunk 3: "alhost\r\n\r\n"        // Complete header + empty line
```

**State transitions:**

```
StateInit (parse request line)
   ↓
StateHeaders (parse headers until empty line)
   ↓
hasBody() check → Yes: StateBody | No: StateDone
   ↓
StateBody (accumulate bytes until Content-Length reached)
   ↓
StateDone (parsing complete)
```

### 2. Key Parsing Functions

**`parseRequestLine(data []byte) (*RequestLine, int, error)`**

- Looks for `\r\n` delimiter
- Returns 0 bytes consumed if incomplete
- Parses: `GET /path HTTP/1.1`

**`Headers.Parse(data []byte) (int, bool, error)`**

- Parses multiple headers line by line
- Detects empty line (`\r\n\r\n`) signaling end
- Combines duplicate headers with commas

**`Request.parse(data []byte) (int, error)`**

- Main state machine loop
- Calls appropriate parser based on current state
- Returns bytes consumed

### 3. Buffer Management

```go
buf := make([]byte, 1024)
bufLen := 0

for !request.done() {
    n, _ := reader.Read(buf[bufLen:])     // Read into buffer
    bufLen += n

    readN, _ := request.parse(buf[:bufLen]) // Parse accumulated data

    copy(buf, buf[readN:bufLen])          // Shift unparsed data
    bufLen -= readN
}
```

**Why this works:**

- Accumulates data across multiple reads
- Parses as much as possible each iteration
- Keeps unparsed data for next read
- Handles any chunk size (1 byte to 1MB)

### 4. Body Parsing (Content-Length)

Unlike request line and headers (delimiter-based), body uses **byte counting**:

```go
length := getInt(headers, "content-length", 0)  // Get expected size
remaining := min(length - len(body), len(chunk)) // How much to read
body = append(body, chunk[:remaining]...)        // Accumulate

if len(body) == length {
    // Body complete!
}
```

### 5. Chunked Transfer Encoding

For streaming responses where size is unknown:

```
Transfer-Encoding: chunked\r\n
\r\n
20\r\n                 ← Hex size (32 bytes)
{32 bytes of data}\r\n
1a\r\n                 ← Hex size (26 bytes)
{26 bytes of data}\r\n
0\r\n                  ← Zero-length chunk
X-Trailer: value\r\n   ← Optional trailers
\r\n
```

## 📚 References

- **[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)** - HTTP Semantics (applies to all HTTP versions)
- **[RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html)** - HTTP/1.1 Message Syntax and Routing
- **[RFC 7231](https://datatracker.ietf.org/doc/html/rfc7231)** - HTTP/1.1 Semantics and Content (older)

## 🚧 Limitations & Future Work

### Current Limitations

- ❌ HTTP/2 and HTTP/3 not supported
- ❌ HTTPS/TLS not implemented
- ❌ Chunked request body parsing (only responses)
- ❌ Request body larger than buffer size
- ❌ Keep-alive connections (closes after each request)
- ❌ Compression (gzip, brotli)
- ❌ Multipart form data

---
