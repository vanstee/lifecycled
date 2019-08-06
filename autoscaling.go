package lifecycled

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
	"github.com/sirupsen/logrus"
)

// AutoscalingClient for testing purposes
//go:generate mockgen -destination=mocks/mock_autoscaling_client.go -package=mocks github.com/buildkite/lifecycled AutoscalingClient
type AutoscalingClient autoscalingiface.AutoScalingAPI

// Message ...
type Message struct {
	Time        time.Time `json:"Time"`
	GroupName   string    `json:"AutoScalingGroupName"`
	InstanceID  string    `json:"EC2InstanceId"`
	ActionToken string    `json:"LifecycleActionToken"`
	Transition  string    `json:"LifecycleTransition"`
	HookName    string    `json:"LifecycleHookName"`
}

// NewAutoscalingListener ...
func NewAutoscalingListener(instanceID string, queue *Queue, autoscaling AutoscalingClient) *AutoscalingListener {
	return &AutoscalingListener{
		listenerType: "autoscaling",
		instanceID:   instanceID,
		queue:        queue,
		autoscaling:  autoscaling,
	}
}

// AutoscalingListener ...
type AutoscalingListener struct {
	listenerType string
	instanceID   string
	queue        *Queue
	autoscaling  AutoscalingClient
}

// Type returns a string describing the listener type.
func (l *AutoscalingListener) Type() string {
	return l.listenerType
}

// Start the autoscaling lifecycle hook listener.
func (l *AutoscalingListener) Start(ctx context.Context, notices chan<- Notice, log *logrus.Entry) error {
	var pending []Notice
	var lastActionToken string
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
		default:
			log.WithField("queueURL", l.queue.url).Debug("Polling sqs for messages")
			messages, err := l.queue.GetMessages(ctx)
			if err != nil {
				log.WithError(err).Warn("Failed to get messages from sqs")
			}
			for _, m := range messages {
				var msg Message

				log.Debug("Received an sqs message")

				if err := json.Unmarshal([]byte(*m.Body), &msg); err != nil {
					log.WithError(err).Error("Failed to unmarshal autoscaling message")
					continue
				}

				if msg.InstanceID != l.instanceID {
					log.WithField("target", msg.InstanceID).Debug("Skipping autoscaling event, doesn't match instance id")
					continue
				}

				if err := l.queue.DeleteMessage(ctx, aws.StringValue(m.ReceiptHandle)); err != nil {
					log.WithError(err).Warn("Failed to delete message")
				}

				// Delete messages can be returned multiple times; only respond to the
				// same action once.
				if msg.ActionToken == lastActionToken {
					continue
				}
				lastActionToken = msg.ActionToken

				switch msg.Transition {
				case "autoscaling:EC2_INSTANCE_LAUNCHING":
					notice := &autoscalingLaunchNotice{
						&autoscalingNotice{
							noticeType:  l.Type(),
							message:     &msg,
							autoscaling: l.autoscaling,
						},
					}
					pending = append(pending, notice)
				case "autoscaling:EC2_INSTANCE_TERMINATING":
					notice := &autoscalingTerminationNotice{
						&autoscalingNotice{
							noticeType:  l.Type(),
							message:     &msg,
							autoscaling: l.autoscaling,
						},
					}
					pending = append(pending, notice)
				default:
					log.WithField("transition", msg.Transition).Debug("Skipping autoscaling event, not a lifecycle notice")
					continue
				}
			}
		}
	}
}

type autoscalingNotice struct {
	noticeType  string
	message     *Message
	autoscaling AutoscalingClient
}

func (n *autoscalingNotice) Type() string {
	return n.noticeType
}

func (n *autoscalingNotice) Handle(ctx context.Context, handler Handler, l *logrus.Entry) error {
	defer func() {
		log.Printf("Running lifecylce action continue")
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := n.autoscaling.CompleteLifecycleActionWithContext(ctx, &autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String(n.message.GroupName),
			LifecycleHookName:     aws.String(n.message.HookName),
			InstanceId:            aws.String(n.message.InstanceID),
			LifecycleActionToken:  aws.String(n.message.ActionToken),
			LifecycleActionResult: aws.String("CONTINUE"),
		})
		log.Printf("Ran lifecylce action continue. ERROR: %v", err)
		cancel()
		if err != nil {
			l.WithError(err).Error("Failed to complete lifecycle action")
		} else {
			l.Info("Lifecycle action completed successfully")
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		log.Printf("ticker defer")
		ticker.Stop()
	}()

	go func() {
		for range ticker.C {
			l.Debug("Sending heartbeat")
			log.Printf("Sending heartbeat")
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := n.autoscaling.RecordLifecycleActionHeartbeatWithContext(
				ctx,
				&autoscaling.RecordLifecycleActionHeartbeatInput{
					AutoScalingGroupName: aws.String(n.message.GroupName),
					LifecycleHookName:    aws.String(n.message.HookName),
					InstanceId:           aws.String(n.message.InstanceID),
					LifecycleActionToken: aws.String(n.message.ActionToken),
				},
			)
			log.Printf("Done sending heartbeat. ERROR: %v", err)
			cancel()
			if err != nil {
				l.WithError(err).Warn("Failed to send heartbeat")
			}
		}
	}()

	return handler.Execute(ctx, n.message.Transition, n.message.InstanceID)
}

type autoscalingLaunchNotice struct {
	*autoscalingNotice
}

func (n *autoscalingLaunchNotice) Transition() Transition {
	return LaunchTransition
}

type autoscalingTerminationNotice struct {
	*autoscalingNotice
}

func (n *autoscalingTerminationNotice) Transition() Transition {
	return TerminationTransition
}
