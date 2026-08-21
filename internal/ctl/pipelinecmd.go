package ctl

import (
	"context"
	"flag"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"
	workerplanev1 "github.com/graphene-ci/pipeline/pkg/proto/workerplane/v1"
)

// cmdPipeline reads pipeline records.
func cmdPipeline(ctx context.Context, args []string) error {
	word, rest, err := need(args, "show")
	if err != nil {
		return err
	}
	if word != "show" {
		return fmt.Errorf("pipeline %q: want show", word)
	}
	fs := flag.NewFlagSet("pipeline show", flag.ExitOnError)
	ctxName, output := commonFlags(fs)
	pos, err := parseMixed(fs, rest)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: pipeline show <pipeline-id>")
	}
	cc, _, err := cliconfig.Resolve(*ctxName)
	if err != nil {
		return err
	}
	resp, err := getPipeline(ctx, cc, pos[0])
	if err != nil {
		return err
	}
	if *output == "json" {
		return printJSON(resp)
	}
	fmt.Fprintf(out, "pipeline %s\nimage    %s\ndigest   %s\n", pos[0], resp.GetImage(), resp.GetDigest())
	if len(resp.GetManifest()) > 0 {
		fmt.Fprintf(out, "manifest %s\n", compactJSON(resp.GetManifest()))
	}
	return nil
}

// getPipeline reads the record over the gRPC half of the same door.
func getPipeline(ctx context.Context, cc cliconfig.Context, pipelineId string) (*workerplanev1.GetPipelineResponse, error) {
	creds := credentials.NewTLS(nil)
	if cc.Insecure {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(cc.Server,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(ctlBearer{cc: cc}),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return workerplanev1.NewManifestAPIClient(conn).GetPipeline(ctx,
		&workerplanev1.GetPipelineRequest{PipelineId: pipelineId})
}

type ctlBearer struct{ cc cliconfig.Context }

func (b ctlBearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	md := map[string]string{"authorization": "Bearer " + b.cc.Token}
	if b.cc.Namespace != "" {
		md["x-graphene-namespace"] = b.cc.Namespace
	}
	return md, nil
}

func (b ctlBearer) RequireTransportSecurity() bool { return !b.cc.Insecure }
