package lifecycled

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
	"github.com/sirupsen/logrus"
)

// AutoscalingClient for testing purposes
//go:generate mockgen -destination=mocks/mock_autoscaling_client.go -package=mocks github.com/buildkite/lifecycled AutoscalingClient
type AutoscalingClient autoscalingiface.AutoScalingAPI

// Envelope ...
type Envelope struct {
	Type    string    `json:"Type"`
	Subject string    `json:"Subject"`
	Time    time.Time `json:"Time"`
	Message string    `json:"Message"`
}

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
	log.WithField("queue", l.queue.name).Debug("Creating sqs queue")
	if err := l.queue.Create(); err != nil {
		return err
	}
	defer func() {
		log.WithField("queue", l.queue.name).Debug("Deleting sqs queue")
		if err := l.queue.Delete(); err != nil {
			log.WithError(err).Error("Failed to delete queue")
		}
	}()

	log.WithField("topic", l.queue.topicArn).Debug("Subscribing queue to sns topic")
	if err := l.queue.Subscribe(); err != nil {
		return err
	}
	defer func() {
		log.WithField("arn", l.queue.subscriptionArn).Debug("Deleting sns subscription")
		if err := l.queue.Unsubscribe(); err != nil {
			log.WithError(err).Error("Failed to unsubscribe from sns topic")
		}
	}()

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
		default:
			log.WithField("queueURL", l.queue.url).Debug("Polling sqs for messages")
			messages, err := l.queue.GetMessages(ctx)
			if err != nil {
				log.WithError(err).Warn("Failed to get messages from SQS")
			}
			for _, m := range messages {
				var env Envelope
				var msg Message

				if err := l.queue.DeleteMessage(ctx, aws.StringValue(m.ReceiptHandle)); err != nil {
					log.WithError(err).Warn("Failed to delete message")
				}

				// unmarshal outer layer
				if err := json.Unmarshal([]byte(*m.Body), &env); err != nil {
					log.WithError(err).Error("Failed to unmarshal envelope")
					continue
				}

				log.WithFields(logrus.Fields{
					"type":    env.Type,
					"subject": env.Subject,
				}).Debug("Received an SQS message")

				// unmarshal inner layer
				if err := json.Unmarshal([]byte(env.Message), &msg); err != nil {
					log.WithError(err).Error("Failed to unmarshal autoscaling message")
					continue
				}

				if msg.InstanceID != l.instanceID {
					log.WithField("target", msg.InstanceID).Debug("Skipping autoscaling event, doesn't match instance id")
					continue
				}

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
					log.WithField("transition", msg.Transition).Debug("Skipping autoscaling event, not a termination notice")
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

func (n *autoscalingNotice) Handle(ctx context.Context, handler Handler, log *logrus.Entry) error {
	defer func() {
		_, err := n.autoscaling.CompleteLifecycleAction(&autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String(n.message.GroupName),
			LifecycleHookName:     aws.String(n.message.HookName),
			InstanceId:            aws.String(n.message.InstanceID),
			LifecycleActionToken:  aws.String(n.message.ActionToken),
			LifecycleActionResult: aws.String("CONTINUE"),
		})
		if err != nil {
			log.WithError(err).Error("Failed to complete lifecycle action")
		} else {
			log.Info("Lifecycle action completed successfully")
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Debug("Sending heartbeat")
			_, err := n.autoscaling.RecordLifecycleActionHeartbeat(
				&autoscaling.RecordLifecycleActionHeartbeatInput{
					AutoScalingGroupName: aws.String(n.message.GroupName),
					LifecycleHookName:    aws.String(n.message.HookName),
					InstanceId:           aws.String(n.message.InstanceID),
					LifecycleActionToken: aws.String(n.message.ActionToken),
				},
			)
			if err != nil {
				log.WithError(err).Warn("Failed to send heartbeat")
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
