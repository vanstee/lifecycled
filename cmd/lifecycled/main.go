package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/buildkite/lifecycled"

	logrus_cloudwatchlogs "github.com/kdar/logrus-cloudwatchlogs"
	"github.com/sirupsen/logrus"
)

var (
	Version string
)

func main() {
	app := kingpin.New("lifecycled",
		"Handle AWS autoscaling lifecycle events gracefully")

	app.Version(Version)
	app.Writer(os.Stdout)
	app.DefaultEnvars()

	var (
		instanceID          string
		snsTopic            string
		disableSpotListener bool
		launchHandler       *os.File
		terminationHandler  *os.File
		jsonLogging         bool
		debugLogging        bool
		cloudwatchGroup     string
		cloudwatchStream    string
	)

	app.Flag("instance-id", "The instance id to listen for events for").
		StringVar(&instanceID)

	app.Flag("sns-topic", "The SNS topic that receives events").
		StringVar(&snsTopic)

	app.Flag("no-spot", "Disable the spot termination listener").
		BoolVar(&disableSpotListener)

	app.Flag("handler", "The script to invoke to handle termination events").
		FileVar(&terminationHandler)

	app.Flag("launch-handler", "The script to invoke to handle launch events").
		FileVar(&launchHandler)

	app.Flag("termination-handler", "The script to invoke to handle termination events").
		FileVar(&terminationHandler)

	app.Flag("json", "Enable JSON logging").
		BoolVar(&jsonLogging)

	app.Flag("cloudwatch-group", "Write logs to a specific Cloudwatch Logs group").
		StringVar(&cloudwatchGroup)

	app.Flag("cloudwatch-stream", "Write logs to a specific Cloudwatch Logs stream, defaults to instance-id").
		StringVar(&cloudwatchStream)

	app.Flag("debug", "Show debugging info").
		BoolVar(&debugLogging)

	app.Action(func(c *kingpin.ParseContext) error {
		logger := logrus.New()
		if jsonLogging {
			logger.SetFormatter(&logrus.JSONFormatter{})
		} else {
			logger.SetFormatter(&logrus.TextFormatter{})
		}

		if debugLogging {
			logger.SetLevel(logrus.DebugLevel)
		}

		if launchHandler == nil && terminationHandler == nil {
			return errors.New("one of required flags --handler, --launch-handler, or --termination-handler not provided")
		}

		sess, err := session.NewSession()
		if err != nil {
			logger.WithError(err).Fatal("Failed to create new aws session")
		}

		if instanceID == "" {
			logger.Info("Looking up instance id from metadata service")
			instanceID, err = ec2metadata.New(sess).GetMetadata("instance-id")
			if err != nil {
				logger.WithError(err).Fatal("Failed to lookup instance id")
			}
		}

		if cloudwatchStream == "" {
			cloudwatchStream = instanceID
		}

		if cloudwatchGroup != "" {
			hook, err := logrus_cloudwatchlogs.NewHook(cloudwatchGroup, cloudwatchStream, aws.NewConfig())
			if err != nil {
				logger.Fatal(err)
			}

			logger.WithFields(logrus.Fields{
				"group":  cloudwatchGroup,
				"stream": cloudwatchStream,
			}).Info("Writing logs to CloudWatch")

			logger.AddHook(hook)
			if !jsonLogging {
				logger.SetFormatter(&logrus.TextFormatter{
					DisableColors:    true,
					DisableTimestamp: true,
				})
			}
		}

		sigs := make(chan os.Signal)
		defer close(sigs)

		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigs)

		// Create an execution context for the daemon that can be cancelled on OS signal
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			for signal := range sigs {
				logger.WithField("signal", signal.String()).Info("Received signal: shutting down...")
				cancel()
				break
			}
		}()

		daemon := lifecycled.New(&lifecycled.Config{
			InstanceID:           instanceID,
			SNSTopic:             snsTopic,
			SpotListener:         !disableSpotListener,
			SpotListenerInterval: 5 * time.Second,
		}, sess, logger)

		notices := daemon.Start(ctx)
		for notice := range notices {
			log := logger.WithFields(logrus.Fields{"instanceId": instanceID, "notice": notice.Type()})
			log.Info("Executing handler")

			var handler *lifecycled.FileHandler
			if notice.Transition() == lifecycled.LaunchTransition {
				handler = lifecycled.NewFileHandler(launchHandler)
			} else {
				handler = lifecycled.NewFileHandler(terminationHandler)
			}

			start, err := time.Now(), notice.Handle(ctx, handler, log)
			log = log.WithField("duration", time.Since(start).String())
			if err != nil {
				log.WithError(err).Error("Failed to execute handler")
			}
			log.Info("Handler finished succesfully")
		}
		return daemon.Stop()
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
