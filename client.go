package client

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/comfforts/comff-delivery/api/v1"
)

const DEFAULT_SERVICE_PORT = "56051"
const DEFAULT_SERVICE_HOST = "127.0.0.1"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

const DeliveryClientContextKey = ContextKey("delivery-client")
const DefaultClientName = "comfforts-delivery-client"

type ClientOption struct {
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
	Caller           string
}

type Client interface {
	GetOrderStatuses(ctx context.Context, req *api.OrderStatusesRequest, opts ...grpc.CallOption) (*api.OrderStatusesResponse, error)
	GetDeliveryStatuses(ctx context.Context, req *api.DeliveryStatusesRequest, opts ...grpc.CallOption) (*api.DeliveryStatusesResponse, error)
	GetScheduleStatuses(ctx context.Context, req *api.ScheduleStatusesRequest, opts ...grpc.CallOption) (*api.ScheduleStatusesResponse, error)
	CreateOrder(ctx context.Context, req *api.CreateOrderRequest, opts ...grpc.CallOption) (*api.OrderResponse, error)
	UpdateOrder(ctx context.Context, req *api.UpdateOrderRequest, opts ...grpc.CallOption) (*api.OrderResponse, error)
	GetOrder(ctx context.Context, req *api.GetOrderRequest, opts ...grpc.CallOption) (*api.OrderResponse, error)
	GetOrders(ctx context.Context, req *api.GetOrdersRequest, opts ...grpc.CallOption) (*api.OrdersResponse, error)
	DeleteOrder(ctx context.Context, req *api.DeleteOrderRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	CreateDelivery(ctx context.Context, req *api.CreateDeliveryRequest, opts ...grpc.CallOption) (*api.DeliveryResponse, error)
	UpdateDelivery(ctx context.Context, req *api.UpdateDeliveryRequest, opts ...grpc.CallOption) (*api.DeliveryResponse, error)
	GetDelivery(ctx context.Context, req *api.GetDeliveryRequest, opts ...grpc.CallOption) (*api.DeliveryResponse, error)
	GetDeliveries(ctx context.Context, req *api.GetDeliveriesRequest, opts ...grpc.CallOption) (*api.DeliveriesResponse, error)
	DeleteDelivery(ctx context.Context, req *api.DeleteDeliveryRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	CreateSchedule(ctx context.Context, req *api.CreateScheduleRequest, opts ...grpc.CallOption) (*api.ScheduleResponse, error)
	UpdateSchedule(ctx context.Context, req *api.UpdateScheduleRequest, opts ...grpc.CallOption) (*api.ScheduleResponse, error)
	UpdateScheduleDelivery(ctx context.Context, req *api.UpdateScheduleRequest, opts ...grpc.CallOption) (*api.ScheduleResponse, error)
	GetSchedule(ctx context.Context, req *api.GetScheduleRequest, opts ...grpc.CallOption) (*api.ScheduleResponse, error)
	SearchSchedules(ctx context.Context, req *api.GetSchedulesRequest, opts ...grpc.CallOption) (*api.SchedulesResponse, error)
	DeleteSchedule(ctx context.Context, req *api.DeleteScheduleRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	Close() error
}

func NewDefaultClientOption() *ClientOption {
	return &ClientOption{
		DialTimeout:      defaultDialTimeout,
		KeepAlive:        defaultKeepAlive,
		KeepAliveTimeout: defaultKeepAliveTimeout,
	}
}

type deliveriesClient struct {
	logger logger.AppLogger
	client api.DeliveriesClient
	conn   *grpc.ClientConn
	opts   *ClientOption
}

func NewClient(
	logger logger.AppLogger,
	clientOpts *ClientOption,
) (*deliveriesClient, error) {
	if clientOpts.Caller == "" {
		clientOpts.Caller = DefaultClientName
	}

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.DELIVERY_CLIENT,
	})
	if err != nil {
		logger.Error("error setting delivery service client TLS", zap.Error(err))
		return nil, err
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	servicePort := os.Getenv("DELIVERY_SERVICE_PORT")
	if servicePort == "" {
		servicePort = DEFAULT_SERVICE_PORT
	}
	serviceHost := os.Getenv("DELIVERY_SERVICE_HOST")
	if serviceHost == "" {
		serviceHost = DEFAULT_SERVICE_HOST
	}
	serviceAddr := fmt.Sprintf("%s:%s", serviceHost, servicePort)
	// with load balancer
	// serviceAddr = fmt.Sprintf("%s:///%s", loadbalance.ShopResolverName, serviceAddr)
	// serviceAddr = fmt.Sprintf("%s:///%s", "shops", serviceAddr)

	conn, err := grpc.Dial(serviceAddr, opts...)
	if err != nil {
		logger.Error("delivery client failed to connect", zap.Error(err))
		return nil, err
	}

	client := api.NewDeliveriesClient(conn)
	logger.Info("delivery client connected", zap.String("host", serviceHost), zap.String("port", servicePort))
	return &deliveriesClient{
		client: client,
		logger: logger,
		conn:   conn,
		opts:   clientOpts,
	}, nil
}

func (dc *deliveriesClient) CreateSchedule(
	ctx context.Context,
	req *api.CreateScheduleRequest,
	opts ...grpc.CallOption,
) (*api.ScheduleResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.CreateSchedule(ctx, req)
}

func (dc *deliveriesClient) UpdateSchedule(
	ctx context.Context,
	req *api.UpdateScheduleRequest,
	opts ...grpc.CallOption,
) (*api.ScheduleResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.UpdateSchedule(ctx, req)
}

func (dc *deliveriesClient) UpdateScheduleDelivery(
	ctx context.Context,
	req *api.UpdateScheduleRequest,
	opts ...grpc.CallOption,
) (*api.ScheduleResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.UpdateScheduleDelivery(ctx, req)
}

func (dc *deliveriesClient) GetSchedule(
	ctx context.Context,
	req *api.GetScheduleRequest,
	opts ...grpc.CallOption,
) (*api.ScheduleResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetSchedule(ctx, req)
}

func (dc *deliveriesClient) SearchSchedules(
	ctx context.Context,
	req *api.GetSchedulesRequest,
	opts ...grpc.CallOption,
) (*api.SchedulesResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.SearchSchedules(ctx, req)
}

func (dc *deliveriesClient) DeleteSchedule(
	ctx context.Context,
	req *api.DeleteScheduleRequest,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.DeleteSchedule(ctx, req)
}

func (dc *deliveriesClient) GetOrderStatuses(
	ctx context.Context,
	req *api.OrderStatusesRequest,
	opts ...grpc.CallOption,
) (*api.OrderStatusesResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetOrderStatuses(ctx, req)
}

func (dc *deliveriesClient) GetDeliveryStatuses(
	ctx context.Context,
	req *api.DeliveryStatusesRequest,
	opts ...grpc.CallOption,
) (*api.DeliveryStatusesResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetDeliveryStatuses(ctx, req)
}

func (dc *deliveriesClient) GetScheduleStatuses(
	ctx context.Context,
	req *api.ScheduleStatusesRequest,
	opts ...grpc.CallOption,
) (*api.ScheduleStatusesResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetScheduleStatuses(ctx, req)
}

func (dc *deliveriesClient) CreateOrder(
	ctx context.Context,
	req *api.CreateOrderRequest,
	opts ...grpc.CallOption,
) (*api.OrderResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.CreateOrder(ctx, req)
}

func (dc *deliveriesClient) UpdateOrder(
	ctx context.Context,
	req *api.UpdateOrderRequest,
	opts ...grpc.CallOption,
) (*api.OrderResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.UpdateOrder(ctx, req)
}

func (dc *deliveriesClient) GetOrder(
	ctx context.Context,
	req *api.GetOrderRequest,
	opts ...grpc.CallOption,
) (*api.OrderResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetOrder(ctx, req)
}

func (dc *deliveriesClient) GetOrders(
	ctx context.Context,
	req *api.GetOrdersRequest,
	opts ...grpc.CallOption,
) (*api.OrdersResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetOrders(ctx, req)
}

func (dc *deliveriesClient) DeleteOrder(
	ctx context.Context,
	req *api.DeleteOrderRequest,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.DeleteOrder(ctx, req)
}

func (dc *deliveriesClient) CreateDelivery(
	ctx context.Context,
	req *api.CreateDeliveryRequest,
	opts ...grpc.CallOption,
) (*api.DeliveryResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.CreateDelivery(ctx, req)
}

func (dc *deliveriesClient) UpdateDelivery(
	ctx context.Context,
	req *api.UpdateDeliveryRequest,
	opts ...grpc.CallOption,
) (*api.DeliveryResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.UpdateDelivery(ctx, req)
}

func (dc *deliveriesClient) GetDelivery(
	ctx context.Context,
	req *api.GetDeliveryRequest,
	opts ...grpc.CallOption,
) (*api.DeliveryResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.GetDelivery(ctx, req)
}

func (dc *deliveriesClient) GetDeliveries(
	ctx context.Context,
	req *api.GetDeliveriesRequest,
	opts ...grpc.CallOption,
) (*api.DeliveriesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return dc.client.GetDeliveries(ctx, req)
}

func (dc *deliveriesClient) DeleteDelivery(
	ctx context.Context,
	req *api.DeleteDeliveryRequest,
	opts ...grpc.CallOption,
) (*api.DeleteResponse, error) {
	ctx, cancel := dc.contextWithOptions(ctx, dc.opts)
	defer cancel()

	return dc.client.DeleteDelivery(ctx, req)
}

func (dc *deliveriesClient) Close() error {
	if err := dc.conn.Close(); err != nil {
		dc.logger.Error("error closing deliveries client connection", zap.Error(err))
		return err
	}
	return nil
}

func (dc *deliveriesClient) contextWithOptions(ctx context.Context, opts *ClientOption) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, dc.opts.DialTimeout)
	if dc.opts.Caller != "" {
		md := metadata.New(map[string]string{"service-client": dc.opts.Caller})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return ctx, cancel
}
