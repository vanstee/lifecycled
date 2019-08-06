package lifecycled

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	longPollingWaitTimeSeconds = 20
)

// SQSClient for testing purposes
//go:generate mockgen -destination=mocks/mock_sqs_client.go -package=mocks github.com/buildkite/lifecycled SQSClient
type SQSClient sqsiface.SQSAPI

// Queue manages an SQS queue.
type Queue struct {
	url       string
	sqsClient SQSClient
}

// NewQueue returns a new... Queue.
func NewQueue(queueArn string, sqsClient SQSClient) (*Queue, error) {
	parsed, err := arn.Parse(queueArn)
	if err != nil {
		return nil, err
	}

	out, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              aws.String(parsed.Resource),
		QueueOwnerAWSAccountId: aws.String(parsed.AccountID),
	})
	if err != nil {
		return nil, err
	}

	return &Queue{
		url:       *out.QueueUrl,
		sqsClient: sqsClient,
	}, nil
}

// GetMessages long polls for messages from the SQS queue.
func (q *Queue) GetMessages(ctx context.Context) ([]*sqs.Message, error) {
	out, err := q.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(longPollingWaitTimeSeconds),
		VisibilityTimeout:   aws.Int64(0),
	})
	if err != nil {
		// Ignore error if the context was cancelled (i.e. we are shutting down)
		if e, ok := err.(awserr.Error); ok && e.Code() == request.CanceledErrorCode {
			return nil, nil
		}
		return nil, err
	}
	return out.Messages, nil
}

// DeleteMessage from the queue.
func (q *Queue) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := q.sqsClient.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		if e, ok := err.(awserr.Error); ok && e.Code() == request.CanceledErrorCode {
			return nil
		}
		return err
	}
	return nil
}
