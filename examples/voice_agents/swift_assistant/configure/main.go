// Command configure stores the assistant this directory describes on the router.
//
// It is the server-side half of the assistant: writing an agent config is not marked
// x-client-accessible, so the router refuses it from a phone, and the app only ever names
// the agent this leaves behind. agent.yaml says what runs it and instructions.md what it is
// told; the Go SDK reads both and syncs them in one request.
//
// Run it once, and again whenever the directory changes:
//
//	STREAM_ACCELERATION_URL=http://localhost:8080 \
//	STREAM_ACCELERATION_CUSTOMER_ID=examples \
//	go run ./configure
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"

	"github.com/GetStream/Vision-Agents/sdks/go/acceleration"
	"github.com/GetStream/Vision-Agents/sdks/go/agents"
)

func main() {
	dir := flag.String("dir", ".", "the agent directory to read")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, *dir); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dir string) error {
	agent, err := agents.New(agents.Options{Dir: dir})
	if err != nil {
		return err
	}
	config, err := agent.Sync(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("agent      %s (config %s)\n", config.Name, config.Id)

	// agent.yaml cannot say which tools' steps a reply shows, or which skills the agent has,
	// so both are patched on after the sync, which leaves a field it does not mention as it
	// is. Naming any tools replaces the default of search and web_search, so those are named
	// again beside the phone's own; naming skills replaces the built-in think, recall and
	// explain, so those stay beside vision, which looks through the phone's camera on a call.
	client, err := agent.Client()
	if err != nil {
		return err
	}
	visible := []string{"search", "web_search", "get_location", "list_events", "add_reminder", "send_email", "gmail__*", "linear__*"}
	skills := []string{"think", "recall", "explain", "vision"}
	patched, err := client.PatchAgentConfigWithResponse(ctx, config.Id,
		acceleration.PatchAgentConfigJSONRequestBody{VisibleTools: &visible, Skills: &skills})
	if err != nil {
		return err
	}
	if patched.JSON200 == nil {
		return fmt.Errorf("the router refused the visible tools: %s", patched.Status())
	}
	// A phone names its app's own scheme as its sessions' return_url, and react_assistant its
	// page's origin, which the router takes from a device only when the app's policy lists it.
	schemes := []string{
		"io.getstream.visionagents.swiftassistant",
		"io.getstream.visionagents.androidassistant",
		"io.getstream.visionagents.flutterassistant",
		"io.getstream.expochatgptsample",
		"http://localhost:5173",
	}
	if err := allowReturnURLs(ctx, client, schemes); err != nil {
		return err
	}
	fmt.Printf("visible    %v\n", visible)
	fmt.Printf("skills     %v\n", skills)
	fmt.Printf("return to  %v\n", schemes)
	fmt.Printf("\nnext       STREAM_API_SECRET=<the router's> go run ./backend\n")
	fmt.Printf("           open examples/voice_agents/swift_assistant/app/SwiftAssistant.xcodeproj and run it\n")
	return nil
}

// allowReturnURLs adds schemes to the return_urls the app's policy lets its phones name,
// keeping the entries already there, which another demo or a person may have set, and the
// rest of the policy as it is.
func allowReturnURLs(ctx context.Context, client *acceleration.ClientWithResponses, schemes []string) error {
	read, err := client.GetAppPolicyWithResponse(ctx)
	if err != nil {
		return err
	}
	if read.JSON200 == nil {
		return fmt.Errorf("reading the app's policy: %s %s", read.Status(), read.Body)
	}
	policy := *read.JSON200
	listed := []string{}
	if policy.DeviceReturnUrls != nil {
		listed = *policy.DeviceReturnUrls
	}
	for _, scheme := range schemes {
		if !slices.Contains(listed, scheme) {
			listed = append(listed, scheme)
		}
	}
	policy.DeviceReturnUrls = &listed
	stored, err := client.UpdateAppPolicyWithResponse(ctx, policy)
	if err != nil {
		return err
	}
	if stored.JSON200 == nil {
		return fmt.Errorf("the router refused the app's policy: %s %s", stored.Status(), stored.Body)
	}
	return nil
}
