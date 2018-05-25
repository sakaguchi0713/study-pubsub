package main

import (
	"context"
	"fmt"
	"cloud.google.com/go/pubsub"
	"log"
	"google.golang.org/api/iterator"
	"time"
	"cloud.google.com/go/iam"
	"sync"
	"os"
)

func main() {
	ctx := context.Background()
	projectID := "project-id"

	//proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	//if proj == "" {
	//	fmt.Fprint(os.Stderr, "GOOGLE_CLOUD_PROJECT environment variable must be set.\n")
	//	os.Exit(1)
	//}
	//create new pub/sub client
	client, err := pubsub.NewClient(ctx, projectID)
	fmt.Printf("debug: %v", os.Getenv("PUBSUB_EMULATOR_HOST"))
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	defer client.Close()

	fmt.Println("Listing all subscriptions from the project:" )
	subs , err := list(client)
	if err != nil {
		log.Fatal(err)
	}
	for _, sub := range subs {
		fmt.Println(sub)
	}
	t := createTopicIfNotExits(client)

	const sub = "example-subscription"
	if err := create(client, sub, t); err != nil {
		panic(err)
	}

	if err := pullMsgs(client, sub, t); err != nil {
		panic(err)
	}

	if err := delete(client,sub); err != nil {
		panic(err)
	}
}

func list(client *pubsub.Client) ([]*pubsub.Subscription, error) {
	ctx := context.Background()
	var subs []*pubsub.Subscription
	it := client.Subscriptions(ctx)
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		subs = append(subs, s)
	}
	return subs, nil
}

func pullMsgs(client *pubsub.Client, name string, topic *pubsub.Topic) error {
	ctx := context.Background()

	//10個のメッセージをtopicに送る
	var results []*pubsub.PublishResult
	for i := 0; i < 1; i++ {
		res := topic.Publish(ctx, &pubsub.Message {
			Data: []byte(fmt.Sprintf("hello world #%d", i)),
		})
		results = append(results, res)
	}

	//全てのメッセージが送られたかの確認
	for _, r := range results {
		i, err := r.Get(ctx)
		fmt.Print(i)
		if err != nil {
			return err
		}
	}

	var mu sync.Mutex
	received := 0
	sub := client.Subscription(name)
	cctx, cancel := context.WithCancel(ctx)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message){
		msg.Ack()
		fmt.Printf("Got message: %q\n", string(msg.Data))
		mu.Lock()
		defer mu.Unlock()
		received++
		if received == 1 {
			cancel()
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func pullMsgsSettings(client *pubsub.Client, name string) error {
	ctx := context.Background()

	sub := client.Subscription(name)
	sub.ReceiveSettings.MaxOutstandingMessages = 10
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Printf("Got message: %q\n,", string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		return err
	}
	return nil
}

func create(client *pubsub.Client, name string, topic *pubsub.Topic) error {
	ctx := context.Background()

	sub, err := client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic: topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created subscription: %v\n", sub)
	return nil
}

func createWithEndpoint(client *pubsub.Client, name string, topic *pubsub.Topic, endpoint string) error {
	ctx := context.Background()

	sub, err := client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic: topic,
		AckDeadline: 10 * time.Second,
		PushConfig: pubsub.PushConfig{Endpoint: endpoint},
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created subscription: %v\n", sub)

	return nil
}

func updateEndpoint(client *pubsub.Client, name string, endpoint string) error {
	ctx := context.Background()

	subconfig, err := client.Subscription(name).Update(ctx, pubsub.SubscriptionConfigToUpdate{
		PushConfig: &pubsub.PushConfig{Endpoint: endpoint},
	})
	if err != nil {
		return err
	}
	fmt.Printf("Updated subscription config: %#v", subconfig)
	return nil
}

func delete(client *pubsub.Client, name string) error {
	ctx := context.Background()

	sub := client.Subscription(name)
	if err := sub.Delete(ctx); err != nil {
		return err
	}
	fmt.Println("Subscription deleted.")

	return nil
}

func createTopicIfNotExits(c *pubsub.Client) *pubsub.Topic {
	ctx := context.Background()

	const topic = "example-topic"
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		return t
	}

	t, err = c.CreateTopic(ctx, topic)
	if err != nil {
		log.Fatalf("Failed to create the topic: %v", err)
	}
	return t
}

func getPolicy(c *pubsub.Client, subName string) (*iam.Policy, error) {
	ctx := context.Background()

	policy , err := c.Subscription(subName).IAM().Policy(ctx)
	if err != nil {
		return nil, err
	}
	for _, role := range policy.Roles() {
		log.Printf("%q: %q", role, policy.Members(role))
	}

	return policy, err
}

func addUsers(c *pubsub.Client, subName string) error {
	ctx := context.Background()

	sub := c.Subscription(subName)
	policy, err := sub.IAM().Policy(ctx)
	if err != nil {
		return err
	}

	policy.Add(iam.AllUsers, iam.Viewer)
	policy.Add("group:cloud-logs@google.com", iam.Editor)
	if err := sub.IAM().SetPolicy(ctx, policy); err != nil {
		return err
	}
	return nil


}

func testPermissions(c *pubsub.Client, subName string) ([]string, error) {
	ctx := context.Background()

	sub := c.Subscription(subName)
	perms, err := sub.IAM().TestPermissions(ctx, []string{
		"pubsub.subscriptions.consume",
		"pubsub.subscriptions.update",
	})
	if err != nil {
		return nil, err
	}
	for _, perm := range perms {
		log.Printf("Allowed: %v", perm)
	}

	return perms, nil
}