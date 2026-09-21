package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mddfaisal/flux/proto"
	quash_proto "github.com/mddfaisal/flux/proto"
)

func Test_SetKV(t *testing.T) {

	for i := 0; i <= 10; i++ {
		t.Run("Test: "+fmt.Sprintf("Add %v", i), func(t *testing.T) {
			resp, err := SetKV(context.Background(), &proto.SetKVRequest{
				Key:             fmt.Sprintf("%v", i),
				Value:           fmt.Sprintf("%v", i),
				TimeOutDuration: 300,
			})
			if err != nil {
				t.Error(err)
			} else {
				t.Log(resp)
			}
		})
		time.Sleep(10 * time.Millisecond)
	}
}

func Test_GetKV(t *testing.T) {
	tests := []struct {
		name string
	}{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {})
	}
}

func Test_CreateQueue(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.CreateTopicRequest
		resp *quash_proto.CreateTopicResponse
		err  error
	}{
		{
			name: "Create Topic",
			ctx:  context.Background(),
			req: &quash_proto.CreateTopicRequest{
				TopicName: "test_topic",
			},
			resp: &quash_proto.CreateTopicResponse{
				Response: "Topic created",
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := CreateTopic(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if resp.Response != tt.resp.Response {
				t.Errorf("Expected response: %v, got: %v", tt.resp.Response, resp.Response)
			} else {
				t.Logf("Test passed with response: %v", resp.Response)
			}
		})
	}
}

func Test_DeleteQueue(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.RemoveTopicRequest
		resp *quash_proto.RemoveTopicResponse
		err  error
	}{
		{
			name: "Delete Topic",
			ctx:  context.Background(),
			req: &quash_proto.RemoveTopicRequest{
				TopicName: "test_topic",
			},
			resp: &quash_proto.RemoveTopicResponse{
				Response: "Queue deleted",
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateTopic(context.Background(), &quash_proto.CreateTopicRequest{
				TopicName: tt.req.TopicName,
			})
			resp, err := DeleteTopic(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if resp.Response != tt.resp.Response {
				t.Errorf("Expected response: %v, got: %v", tt.resp.Response, resp.Response)
			} else {
				t.Logf("Test passed with response: %v", resp.Response)
			}
		})
	}
}
