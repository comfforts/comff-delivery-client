package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	comffC "github.com/comfforts/comff-constants"
	delclient "github.com/comfforts/comff-delivery-client"
	api "github.com/comfforts/comff-delivery/api/v1"
	"github.com/comfforts/logger"
)

const TEST_DIR = "data"

func TestDeliveriesClient(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		dc delclient.Client,
	){
		"test database setup check, succeeds": testDatabaseSetup,
		"test order CRUD, succeeds":           testOrderCRUD,
		"test delivery CRUD, succeeds":        testDeliveryCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			dc, teardown := setup(t, logger)
			defer teardown()
			fn(t, dc)
		})
	}

}

func setup(t *testing.T, logger logger.AppLogger) (
	dc delclient.Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := delclient.NewDefaultClientOption()
	clientOpts.Caller = "delivery-client-test"

	dc, err := delclient.NewClient(logger, clientOpts)
	require.NoError(t, err)

	return dc, func() {
		t.Logf(" TestDeliveriesClient ended, will close deliveries client")
		err := dc.Close()
		require.NoError(t, err)
	}
}

func testDatabaseSetup(t *testing.T, dc delclient.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	osResp, err := dc.GetOrderStatuses(ctx, &api.OrderStatusesRequest{})
	require.NoError(t, err)
	require.Equal(t, len(osResp.Statuses), 2)

	dsResp, err := dc.GetDeliveryStatuses(ctx, &api.DeliveryStatusesRequest{})
	require.NoError(t, err)
	require.Equal(t, len(dsResp.Statuses), 7)

	ssResp, err := dc.GetScheduleStatuses(ctx, &api.ScheduleStatusesRequest{})
	require.NoError(t, err)
	require.Equal(t, len(ssResp.Statuses), 4)
}

func testOrderCRUD(t *testing.T, dc delclient.Client) {
	t.Helper()

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	reqtr, shopId, txnId := "test-client-create-order@gmail.com", "test-client-order-create", "Cr341e0r62"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqtr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       "s742t",
		DestinationId: "63s1",
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	})
	or = getOrderTester(t, dc, &api.GetOrderRequest{
		Id: or.Order.Id,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deliveryId := "D3l1v3ry"
	resp, err := dc.UpdateOrder(ctx, &api.UpdateOrderRequest{
		Id:          or.Order.Id,
		DeliveryId:  deliveryId,
		Status:      api.OrderStatus_READY_FOR_PICKUP,
		RequestedBy: reqtr,
	})
	require.NoError(t, err)
	assert.Equal(t, resp.Order.DeliveryId, deliveryId, "order delivery id should match input delivery id")
	assert.Equal(t, resp.Order.Status, api.OrderStatus_READY_FOR_PICKUP, "order status should be READY FOR PICKUP")

	deleteOrderTester(t, dc, &api.DeleteOrderRequest{
		Id: or.Order.Id,
	})
}

func testDeliveryCRUD(t *testing.T, dc delclient.Client) {
	t.Helper()

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	reqtr, shopId, txnId := "test-client-create-delivery@gmail.com", "test-client-delivery-create", "Cr341D3lv2y"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqtr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       "s742t",
		DestinationId: "63s1",
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	})
	or = getOrderTester(t, dc, &api.GetOrderRequest{
		Id: or.Order.Id,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	del := createDeliveryTester(t, dc, &api.CreateDeliveryRequest{
		OrderId:       or.Order.Id,
		RequestedBy:   reqtr,
		SourceId:      shopId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       "s742t",
		DestinationId: "63s1",
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	})
	del = getDeliveryTester(t, dc, &api.GetDeliveryRequest{
		Id: del.Delivery.Id,
	})

	resp, err := dc.UpdateOrder(ctx, &api.UpdateOrderRequest{
		Id:          or.Order.Id,
		DeliveryId:  del.Delivery.Id,
		Status:      api.OrderStatus_READY_FOR_PICKUP,
		RequestedBy: reqtr,
	})
	require.NoError(t, err)
	assert.Equal(t, resp.Order.DeliveryId, del.Delivery.Id, "order delivery id should match input delivery id")
	assert.Equal(t, resp.Order.Status, api.OrderStatus_READY_FOR_PICKUP, "order status should be READY FOR PICKUP")

	courierId := "C0213r"
	delResp, err := dc.UpdateDelivery(ctx, &api.UpdateDeliveryRequest{
		Id:          del.Delivery.Id,
		CourierId:   courierId,
		Status:      api.DeliveryStatus_CONFIGURED,
		RequestedBy: reqtr,
	})
	require.NoError(t, err)
	assert.Equal(t, delResp.Delivery.CourierId, courierId, "delivery courier id should match input courier id")
	assert.Equal(t, delResp.Delivery.Status, api.DeliveryStatus_CONFIGURED, "delivery status should be CONFIGURED")

	deleteOrderTester(t, dc, &api.DeleteOrderRequest{
		Id: or.Order.Id,
	})

	deleteDeliveryTester(t, dc, &api.DeleteDeliveryRequest{
		Id: delResp.Delivery.Id,
	})
}

func createOrderTester(t *testing.T, client delclient.Client, cor *api.CreateOrderRequest) *api.OrderResponse {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := client.CreateOrder(ctx, cor)
	require.NoError(t, err)
	assert.Equal(t, resp.Order.ShopId, cor.ShopId, "order shop id should match input shop id")
	assert.Equal(t, resp.Order.Status, api.OrderStatus_ORDER_CREATED, "order status should be CREATED")

	assert.Equal(t, resp.Order.DeliveryDetails.Height, cor.PkgHeight, "order height should match input height")
	assert.Equal(t, resp.Order.DeliveryDetails.Width, cor.PkgWidth, "order width should match input width")
	assert.Equal(t, resp.Order.DeliveryDetails.Depth, cor.PkgDepth, "order depth should match input depth")

	assert.Equal(t, resp.Order.Source.AddressId, cor.StartId, "order start should match input start")
	assert.Equal(t, resp.Order.Destination.AddressId, cor.DestinationId, "order destination should match input destination")

	assert.Equal(t, resp.Order.Offer.Range.Max, cor.OfferMax, "order offer max should match input max")
	assert.Equal(t, resp.Order.Offer.Range.Min, cor.OfferMin, "order offer min should match input min")

	assert.Equal(t, resp.Order.DeliveryTimeRange.Start, cor.StartTime, "order start time should match input start time")
	assert.Equal(t, resp.Order.DeliveryTimeRange.End, cor.EndTime, "order end time should match input end time")

	return resp
}

func getOrderTester(t *testing.T, client delclient.Client, gor *api.GetOrderRequest) *api.OrderResponse {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.GetOrder(ctx, gor)
	require.NoError(t, err)
	require.Equal(t, resp.Order.Id, gor.Id)
	return resp
}

func deleteOrderTester(t *testing.T, client delclient.Client, dor *api.DeleteOrderRequest) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.DeleteOrder(ctx, dor)
	require.NoError(t, err)
	require.Equal(t, true, resp.Ok)
}

func createDeliveryTester(t *testing.T, client delclient.Client, cdr *api.CreateDeliveryRequest) *api.DeliveryResponse {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := client.CreateDelivery(ctx, cdr)
	require.NoError(t, err)
	assert.Equal(t, resp.Delivery.SourceId, cdr.SourceId, "delivery source id should match input source id")
	assert.Equal(t, resp.Delivery.OrderId, cdr.OrderId, "delivery order id should match input order id")
	assert.Equal(t, resp.Delivery.Status, api.DeliveryStatus_DELIVERY_CREATED, "delivery status should be CREATED")

	assert.Equal(t, resp.Delivery.DeliveryDetails.Height, cdr.PkgHeight, "order height should match input height")
	assert.Equal(t, resp.Delivery.DeliveryDetails.Width, cdr.PkgWidth, "order width should match input width")
	assert.Equal(t, resp.Delivery.DeliveryDetails.Depth, cdr.PkgDepth, "order depth should match input depth")

	assert.Equal(t, resp.Delivery.Source.AddressId, cdr.StartId, "order start should match input start")
	assert.Equal(t, resp.Delivery.Destination.AddressId, cdr.DestinationId, "order destination should match input destination")

	assert.Equal(t, resp.Delivery.Offer.Range.Max, cdr.OfferMax, "order offer max should match input max")
	assert.Equal(t, resp.Delivery.Offer.Range.Min, cdr.OfferMin, "order offer min should match input min")

	assert.Equal(t, resp.Delivery.DeliveryTimeRange.Start, cdr.StartTime, "order start time should match input start time")
	assert.Equal(t, resp.Delivery.DeliveryTimeRange.End, cdr.EndTime, "order end time should match input end time")

	return resp
}

func getDeliveryTester(t *testing.T, client delclient.Client, gd *api.GetDeliveryRequest) *api.DeliveryResponse {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.GetDelivery(ctx, gd)
	require.NoError(t, err)
	require.Equal(t, resp.Delivery.Id, gd.Id)
	return resp
}

func deleteDeliveryTester(t *testing.T, client delclient.Client, dd *api.DeleteDeliveryRequest) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.DeleteDelivery(ctx, dd)
	require.NoError(t, err)
	require.Equal(t, true, resp.Ok)
}
