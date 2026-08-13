package operator

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// distTimeouts keep a stuck download from holding a connection forever.
const (
	distReadTimeout  = 30 * time.Second
	distWriteTimeout = 5 * time.Minute
)

// Distributor hands out the agent binary.
//
// The control plane serves it from its own image rather than pointing at a
// release page: then the agent's version always matches the system's, and
// there is one moving part instead of two. The binary travels inside the
// operator's image as ko data, so "the operator is deployed" and "agents of
// the right version can be installed" are the same fact.
type Distributor struct {
	// Root holds the binaries, laid out as agent/<os>/<arch>.
	Root string
	// Addr is where to listen.
	Addr string
}

// NewDistributor builds one over the image's own data directory.
func NewDistributor(addr string) *Distributor {
	return &Distributor{Root: os.Getenv("KO_DATA_PATH"), Addr: addr}
}

// Start serves until the context is done. It satisfies manager.Runnable, so
// the manager starts and stops it with everything else.
func (d *Distributor) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("GET /agent/", http.StripPrefix("/agent/",
		http.FileServer(http.Dir(filepath.Join(d.Root, "agent")))))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:              d.Addr,
		Handler:           mux,
		ReadHeaderTimeout: distReadTimeout,
		WriteTimeout:      distWriteTimeout,
	}

	go func() {
		<-ctx.Done()

		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), distReadTimeout)
		defer cancel()

		_ = server.Shutdown(shutdown)
	}()

	if err := server.ListenAndServe(); err != nil && !isClosed(err) {
		return fmt.Errorf("раздача агента остановилась: %w", err)
	}

	return nil
}

func isClosed(err error) bool {
	return err.Error() == http.ErrServerClosed.Error()
}
