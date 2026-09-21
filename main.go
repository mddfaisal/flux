package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/mddfaisal/flux/server"
)

func main() {
	go server.Serve()
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	quit := make(chan os.Signal, 1)
	select {
	case sig := <-shutdown:
		println("Received signal:", sig)
		println("Shutting down gracefully...")
		server.Shutdown()
		println("Server stopped.")
	case sig := <-quit:
		println("Received quit signal:", sig)
		println("Exiting immediately...")
		os.Exit(0)
	}
}
