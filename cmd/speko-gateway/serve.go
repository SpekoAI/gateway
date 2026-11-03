package main

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
)

// The command owns every background lifetime. Both signals and Serve errors
// stop/join periodic reporting before the final report and terminal DELETE.
type gatewayProcess interface {
	Serve(net.Listener) error
	BeginDrain()
	Drain(context.Context) error
}

func serveGateway(ctx context.Context, server gatewayProcess, listener net.Listener, reporter *instanceReporter, heartbeat func() controlplane.InstanceHeartbeat, interval time.Duration, warm func(context.Context)) error {
	background, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErrors, serveDone := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(serveDone)
		serveErrors <- server.Serve(listener)
	}()
	defer func() { _ = listener.Close(); <-serveDone }()
	warmDone := make(chan struct{})
	if warm == nil {
		close(warmDone)
	} else {
		go func() { defer close(warmDone); warm(background) }()
	}
	reportDone, reportErrors := make(chan struct{}), make(chan error, 1)
	if reporter == nil {
		close(reportDone)
	} else {
		go func() {
			defer close(reportDone)
			if err := reportInstanceLoop(background, reporter, heartbeat, interval); err != nil {
				reportErrors <- err
			}
		}()
	}
	var stopped error
	select {
	case <-ctx.Done():
		log.Printf("received shutdown signal; draining")
	case stopped = <-serveErrors:
	case stopped = <-reportErrors:
	}
	server.BeginDrain()
	cancel()
	<-reportDone
	<-warmDone
	if reporter != nil {
		finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := reporter.report(finalCtx, heartbeat()); err != nil {
			log.Printf("final draining heartbeat failed")
		}
		finalCancel()
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	drainErr := server.Drain(drainCtx)
	drainCancel()
	if reporter != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := reporter.close(closeCtx); err != nil {
			log.Printf("runtime instance deregistration failed")
		}
		closeCancel()
	}
	return errors.Join(stopped, drainErr)
}

func reportInstanceLoop(ctx context.Context, reporter *instanceReporter, heartbeat func() controlplane.InstanceHeartbeat, interval time.Duration) error {
	report := func() error {
		if ctx.Err() != nil {
			return nil
		}
		reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err := reporter.report(reportCtx, heartbeat())
		if errors.Is(err, errInstanceSequenceExhausted) {
			return err
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("runtime instance heartbeat failed")
		}
		return nil
	}
	if err := report(); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := report(); err != nil {
				return err
			}
		}
	}
}
