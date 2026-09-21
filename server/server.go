package server

import (
	"net"

	"github.com/mddfaisal/flux/admin"
	pb "github.com/mddfaisal/flux/proto"
	"github.com/mddfaisal/flux/server/quashserver"
	"github.com/mddfaisal/flux/utils"
	"google.golang.org/grpc"
)

var (
	srv      = grpc.NewServer()
	quashSrv = &quashserver.Server{}
)

func Serve() {
	go admin.AdminServer()
	lis, err := net.Listen("tcp", utils.QuashDb)
	if err != nil {
		panic(err)
	}
	quashSrv.Init()
	pb.RegisterQuashServiceServer(srv, quashSrv)
	if err := srv.Serve(lis); err != nil {
		panic(err)
	}
}

func Shutdown() {
	quashSrv.Shutdown()
	srv.GracefulStop()
}
