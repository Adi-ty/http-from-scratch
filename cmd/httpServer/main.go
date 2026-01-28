package main

import (
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Adi-ty/http-from-scratch/internal/request"
	"github.com/Adi-ty/http-from-scratch/internal/response"
	"github.com/Adi-ty/http-from-scratch/internal/server"
)

const port = 42069

func main() {
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
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer server.Close()
	log.Println("Server started on port", port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Server gracefully stopped")
}

