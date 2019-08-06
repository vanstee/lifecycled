package lifecycled

import (
	"context"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// New creates a new lifecycle Daemon.
func New(config *Config, sess *session.Session, logger *logrus.Logger) (*Daemon, error) {
	return NewDaemon(
		config,
		sqs.New(sess),
		autoscaling.New(sess),
		ec2metadata.New(sess),
		logger,
	)
}

// NewDaemon creates a new Daemon.
func NewDaemon(
	config *Config,
	sqsClient SQSClient,
	asgClient AutoscalingClient,
	metadata *ec2metadata.EC2Metadata,
	logger *logrus.Logger,
) (*Daemon, error) {
	daemon := &Daemon{
		instanceID: config.InstanceID,
		logger:     logger,
	}
	if config.SpotListener {
		daemon.AddListener(NewSpotListener(config.InstanceID, metadata, config.SpotListenerInterval))
	}
	if config.SQSQueue != "" {
		// TODO: Lookup queue name
		queue, err := NewQueue(config.SQSQueue, sqsClient)
		if err != nil {
			return nil, err
		}
		daemon.AddListener(NewAutoscalingListener(config.InstanceID, queue, asgClient))
	}
	return daemon, nil
}

// Config for the Lifecycled Daemon.
type Config struct {
	InstanceID           string
	SQSQueue             string
	SpotListener         bool
	SpotListenerInterval time.Duration
}

// Daemon is what orchestrates the listening and execution of the handler on a termination notice.
type Daemon struct {
	instanceID string
	listeners  []Listener
	logger     *logrus.Logger
	notices    chan Notice
	group      *errgroup.Group
	cancel     context.CancelFunc
}

// Start the Daemon.
func (d *Daemon) Start(ctx context.Context) chan Notice {
	log := d.logger.WithField("instanceId", d.instanceID)

	// Use a buffered channel to avoid deadlocking a goroutine when we stop listening
	d.notices = make(chan Notice, len(d.listeners))

	// Allow all listeners to be canceled
	ctx, d.cancel = context.WithCancel(ctx)
	d.group, ctx = errgroup.WithContext(ctx)

	for _, listener := range d.listeners {
		listener := listener
		d.group.Go(func() error {
			l := log.WithField("listener", listener.Type())
			l.Info("Starting listener")

			if err := listener.Start(ctx, d.notices, l); err != nil {
				l.WithError(err).Error("Failed while listening")
				return err
			}

			l.Info("Stopped listener")
			return nil
		})
	}

	log.Info("Waiting for lifecycle notices")
	return d.notices
}

// Stop the Daemon. Cancel and wait for all listeners to return.
func (d *Daemon) Stop() error {
	d.cancel()
	err := d.group.Wait()
	close(d.notices)
	return err
}

// AddListener to the Daemon.
func (d *Daemon) AddListener(l Listener) {
	d.listeners = append(d.listeners, l)
}

// Listener ...
type Listener interface {
	Type() string
	Start(context.Context, chan<- Notice, *logrus.Entry) error
}

type Transition string

const (
	LaunchTransition      Transition = "launch"
	TerminationTransition Transition = "termination"
)

// Notice ...
type Notice interface {
	Type() string
	Transition() Transition
	Handle(context.Context, Handler, *logrus.Entry) error
}

// Handler ...
type Handler interface {
	Execute(ctx context.Context, args ...string) error
}

// NewFileHandler ...
func NewFileHandler(file *os.File) *FileHandler {
	return &FileHandler{file: file}
}

// FileHandler ...
type FileHandler struct {
	file *os.File
}

// Execute the file handler.
func (h *FileHandler) Execute(ctx context.Context, args ...string) error {
	log.Printf("Running handler %s", h.file.Name())
	cmd := exec.CommandContext(ctx, h.file.Name(), args...)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	log.Printf("Finished handler %s", h.file.Name())
	return err
}
