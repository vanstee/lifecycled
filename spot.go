package lifecycled

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/sirupsen/logrus"
)

// NewSpotListener ...
func NewSpotListener(instanceID string, metadata *ec2metadata.EC2Metadata, interval time.Duration) *SpotListener {
	return &SpotListener{
		listenerType: "spot",
		instanceID:   instanceID,
		metadata:     metadata,
		interval:     interval,
	}
}

// SpotListener ...
type SpotListener struct {
	listenerType string
	instanceID   string
	metadata     *ec2metadata.EC2Metadata
	interval     time.Duration
}

// Type returns a string describing the listener type.
func (l *SpotListener) Type() string {
	return l.listenerType
}

// Start the spot termination notice listener.
func (l *SpotListener) Start(ctx context.Context, notices chan<- Notice, log *logrus.Entry) error {
	if !l.metadata.Available() {
		return errors.New("ec2 metadata is not available")
	}

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	var pending []Notice
	for {
		// Only send pending notices if available. The following select wont try to
		// send to a nil channel.
		var pendingNotices chan<- Notice
		var notice Notice
		if len(pending) > 0 {
			pendingNotices = notices
			notice = pending[0]
		}

		select {
		case <-ctx.Done():
			return nil
		case pendingNotices <- notice:
			pending = pending[1:]
		case <-ticker.C:
			log.Debug("Polling ec2 metadata for spot termination notices")

			out, err := l.metadata.GetMetadata("spot/termination-time")
			if err != nil {
				if e, ok := err.(awserr.Error); ok && strings.Contains(e.OrigErr().Error(), "404") {
					// Metadata returns 404 when there is no termination notice available
					continue
				} else {
					log.WithError(err).Warn("Failed to get spot termination")
					continue
				}
			}
			if out == "" {
				log.Error("Empty response from metadata")
				continue
			}
			t, err := time.Parse(time.RFC3339, out)
			if err != nil {
				log.WithError(err).Error("Failed to parse termination time")
				continue
			}
			notice := &spotTerminationNotice{
				noticeType:      l.Type(),
				instanceID:      l.instanceID,
				transition:      "ec2:SPOT_INSTANCE_TERMINATION",
				terminationTime: t,
			}
			pending = append(pending, notice)
		}
	}
}

type spotTerminationNotice struct {
	noticeType      string
	instanceID      string
	transition      string
	terminationTime time.Time
}

func (n *spotTerminationNotice) Type() string {
	return n.noticeType
}

func (n *spotTerminationNotice) Transition() Transition {
	return TerminationTransition
}

func (n *spotTerminationNotice) Handle(ctx context.Context, handler Handler, log *logrus.Entry) error {
	return handler.Execute(ctx, n.transition, n.instanceID, n.terminationTime.Format(time.RFC3339))
}
