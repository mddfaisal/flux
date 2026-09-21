package quashserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "github.com/mddfaisal/quash/proto"
	"github.com/mddfaisal/quash/server/queue"
	"github.com/mddfaisal/quash/structs"
	"github.com/mddfaisal/quash/utils"
	grpc "google.golang.org/grpc"
)

var (
	kvLock      = &sync.Mutex{}
	brokersLock = &sync.Mutex{}
	kvMap       = map[string]structs.KVValue{}
	brokers     = map[structs.Topic]map[structs.SubscriptionID]*queue.Queue{}
)

func Snapshot() structs.SrvData {
	kvLock.Lock()
	kvLen := int64(len(kvMap))
	kvLock.Unlock()

	brokersLock.Lock()
	topics := make(map[string]map[string]int64, len(brokers))
	for name, subs := range brokers {
		topics[string(name)] = make(map[string]int64)
		for subscriber, queue := range subs {
			topics[string(name)][string(subscriber)] = queue.Count()
		}
	}
	brokersLock.Unlock()

	return structs.SrvData{
		KVMapLength: kvLen,
		Topics:      topics,
	}
}

type Server struct {
	pb.QuashServiceServer
}

func (s *Server) Init() {
	go GarbageCollection()
}

func (s *Server) SetKV(ctx context.Context, kv *pb.SetKVRequest) (*pb.SetKVResponse, error) {
	log.Printf("SetKV, key=%v, value=%v,\n", kv.Key, kv.Value)
	kvLock.Lock()
	kvMap[kv.Key] = structs.KVValue{
		Value:    kv.Value,
		InitTime: time.Now(),
		TimeOut:  time.Duration(kv.TimeOutDuration) * time.Second,
	}
	kvLock.Unlock()
	return &pb.SetKVResponse{Response: "Added"}, nil
}

func (s *Server) GetKV(ctx context.Context, kv *pb.GetKVRequest) (*pb.GetKVResponse, error) {
	log.Printf("GetKV, key=%v\n", kv.Key)
	now := time.Now()
	kvLock.Lock()
	defer kvLock.Unlock()
	val, ok := kvMap[kv.Key]
	if !ok {
		return nil, errors.New("key doesn't exist")
	}
	if now.After(val.InitTime.Add(val.TimeOut)) {
		delete(kvMap, kv.Key)
		return nil, errors.New("key doesn't exist")
	}
	return &pb.GetKVResponse{Value: val.Value}, nil
}

func (s *Server) DeleteKV(ctx context.Context, req *pb.DeleteKVRequest) (*pb.DeleteKVResponse, error) {
	kvLock.Lock()
	defer kvLock.Unlock()
	delete(kvMap, req.Key)
	return &pb.DeleteKVResponse{Value: "OK"}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	if _, ok := brokers[structs.Topic(req.TopicName)]; ok {
		return nil, errors.New("topic already exists")
	}
	brokers[structs.Topic(req.TopicName)] = map[structs.SubscriptionID]*queue.Queue{}
	log.Printf("CreateTopic, Topic Name=%v\n", req.TopicName)
	return &pb.CreateTopicResponse{Response: "Topic created: " + req.TopicName}, nil
}

func (s *Server) RemoveTopic(ctx context.Context, req *pb.RemoveTopicRequest) (*pb.RemoveTopicResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	delete(brokers, structs.Topic(req.TopicName))
	log.Printf("RemoveTopic, Topic Name=%v\n", req.TopicName)
	return &pb.RemoveTopicResponse{Response: "Topic deleted: " + req.TopicName}, nil
}

func (s *Server) AddSubscriber(ctx context.Context, req *pb.AddSubscriberRequest) (*pb.AddSubscriberResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	subs, ok := brokers[structs.Topic(req.TopicName)]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	id := utils.SubscriptionID()
	subs[structs.SubscriptionID(id)] = queue.NewQueue()
	log.Printf("AddSubscriber, Topic Name=%v, Subscriber ID=%v\n", req.TopicName, id)
	return &pb.AddSubscriberResponse{
		Response:     "Subscriber added",
		SubscriberId: id,
	}, nil
}

func (s *Server) RemoveSubscriber(ctx context.Context, req *pb.RemoveSubscriberRequest) (*pb.RemoveSubscriberResponse, error) {
	brokersLock.Lock()
	defer brokersLock.Unlock()
	delete(brokers[structs.Topic(req.TopicName)], structs.SubscriptionID(req.SubscriberId))
	log.Printf("RemovedSubscriber, Topic Name=%v, Subscriber ID=%v\n", req.TopicName, req.SubscriberId)
	return &pb.RemoveSubscriberResponse{
		Response: "Removed: " + fmt.Sprintf("%v", req.SubscriberId),
	}, nil
}

func (s *Server) Publish(stream grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		brokersLock.Lock()
		subs := brokers[structs.Topic(req.TopicName)]
		for id, _ := range subs {
			brokers[structs.Topic(req.TopicName)][structs.SubscriptionID(id)].Push(req.Value)
		}
		brokersLock.Unlock()
		log.Printf("Publish, Topic Name=%v, Value=%v\n", req.TopicName, req.Value)
		if err := stream.Send(&pb.PublishResponse{
			Response: fmt.Sprintf("Published: %v to Topic: %v", req.Value, req.TopicName),
		}); err != nil {
			return err
		}
	}
}

func (s *Server) Consume(stream grpc.BidiStreamingServer[pb.ConsumeRequest, pb.ConsumeResponse]) error {
	req, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	brokersLock.Lock()
	q := brokers[structs.Topic(req.TopicName)][structs.SubscriptionID(req.SubscriberId)]
	count := int(q.Count())
	for i := 0; i < count; i++ {
		val := q.Pop()
		if err := stream.Send(&pb.ConsumeResponse{Response: val}); err == nil {
			log.Printf("Message: %v, send to Subscriber: %v", val, req.SubscriberId)
		} else {
			return err
		}
	}
	brokersLock.Unlock()
	log.Printf("Subscribe, Topic Name=%v, Subscriber=%v\n", req.TopicName, req.SubscriberId)
	return nil
}

func (s *Server) memoryDump() {

}

func GarbageCollection() {
	for {
		kvLock.Lock()
		now := time.Now()
		for k, v := range kvMap {
			if now.After(v.InitTime.Add(v.TimeOut)) {
				delete(kvMap, k)
			}
		}
		kvLock.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (s *Server) Shutdown() {
	s.memoryDump()
	log.Printf("gRPC server stopped gracefully\n")
}
