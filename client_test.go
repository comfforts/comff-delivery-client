package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	comffC "github.com/comfforts/comff-constants"
	geoclient "github.com/comfforts/comff-geo-client"
	geoapi "github.com/comfforts/comff-geo/api/v1"
	"github.com/comfforts/logger"

	delclient "github.com/comfforts/comff-delivery-client"
	api "github.com/comfforts/comff-delivery/api/v1"
)

const TEST_DIR = "data"

func TestDeliveriesClient(t *testing.T) {
	testLogger := logger.NewTestAppLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		dc delclient.Client,
		testLogger logger.AppLogger,
	){
		"test database setup check, succeeds":       testDatabaseSetup,
		"test order CRUD, succeeds":                 testOrderCRUD,
		"test delivery CRUD, succeeds":              testDeliveryCRUD,
		"invalid delivery create check, succeeds":   testInvalidDeliveryCreate,
		"duplicate delivery create check, succeeds": testDuplicateDeliveryCreate,
		"test schedule CRUD, succeeds":              testScheduleCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			dc, teardown := setup(t, testLogger)
			defer teardown()
			fn(t, dc, testLogger)
		})
	}
}

func setup(t *testing.T, testLogger logger.AppLogger) (
	dc delclient.Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := delclient.NewDefaultClientOption()
	clientOpts.Caller = "TestDeliveriesClient"

	dc, err := delclient.NewClient(testLogger, clientOpts)
	require.NoError(t, err)

	return dc, func() {
		t.Logf(" TestDeliveriesClient ended, will close deliveries client")
		err := dc.Close()
		require.NoError(t, err)
	}
}

func testDatabaseSetup(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
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

func testOrderCRUD(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
	t.Helper()

	gOpts := geoclient.NewDefaultClientOption()
	gOpts.Caller = "TestDeliveriesClient"
	geoClient, err := geoclient.NewClient(testLogger, gOpts)
	require.NoError(t, err)

	reqstr := "test-client-create-order@gmail.com"
	startAddr := createStartAddressTester(t, geoClient, reqstr)
	destAddr := createDestAddressTester(t, geoClient, reqstr)

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	shopId, txnId := "test-client-order-create", "Cr341e0r62"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqstr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
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
		RequestedBy: reqstr,
	})
	require.NoError(t, err)
	assert.Equal(t, resp.Order.DeliveryId, deliveryId, "order delivery id should match input delivery id")
	assert.Equal(t, resp.Order.Status, api.OrderStatus_READY_FOR_PICKUP, "order status should be READY FOR PICKUP")

	deleteOrderTester(t, dc, &api.DeleteOrderRequest{
		Id: or.Order.Id,
	})

	deleteAddressTester(t, geoClient, startAddr.Id, startAddr.RefId)
	deleteAddressTester(t, geoClient, destAddr.Id, destAddr.RefId)
}

func testDeliveryCRUD(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
	t.Helper()

	reqstr := "test-client-create-delivery@gmail.com"
	gOpts := geoclient.NewDefaultClientOption()
	gOpts.Caller = "TestDeliveriesClient"
	geoClient, err := geoclient.NewClient(testLogger, gOpts)
	require.NoError(t, err)

	startAddr := createStartAddressTester(t, geoClient, reqstr)
	destAddr := createDestAddressTester(t, geoClient, reqstr)

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	shopId, txnId := "test-client-delivery-create", "Cr341D3lv2y"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqstr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
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
		RequestedBy:   reqstr,
		SourceId:      or.Order.ShopId,
		OfferMin:      or.Order.Offer.Range.Min,
		OfferMax:      or.Order.Offer.Range.Max,
		StartId:       or.Order.Source.AddressId,
		DestinationId: or.Order.Destination.AddressId,
		PkgHeight:     or.Order.DeliveryDetails.Height,
		PkgWidth:      or.Order.DeliveryDetails.Width,
		PkgDepth:      or.Order.DeliveryDetails.Depth,
		StartTime:     or.Order.DeliveryTimeRange.Start,
		EndTime:       or.Order.DeliveryTimeRange.End,
	})
	del = getDeliveryTester(t, dc, &api.GetDeliveryRequest{
		Id: del.Delivery.Id,
	})

	resp, err := dc.UpdateOrder(ctx, &api.UpdateOrderRequest{
		Id:          or.Order.Id,
		DeliveryId:  del.Delivery.Id,
		Status:      api.OrderStatus_READY_FOR_PICKUP,
		RequestedBy: reqstr,
	})
	require.NoError(t, err)
	assert.Equal(t, resp.Order.DeliveryId, del.Delivery.Id, "order delivery id should match input delivery id")
	assert.Equal(t, resp.Order.Status, api.OrderStatus_READY_FOR_PICKUP, "order status should be READY FOR PICKUP")

	courierId := "C0213r"
	delResp, err := dc.UpdateDelivery(ctx, &api.UpdateDeliveryRequest{
		Id:          del.Delivery.Id,
		CourierId:   courierId,
		Status:      api.DeliveryStatus_CONFIGURED,
		RequestedBy: reqstr,
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

	deleteAddressTester(t, geoClient, startAddr.Id, startAddr.RefId)
	deleteAddressTester(t, geoClient, destAddr.Id, destAddr.RefId)
}

func testDuplicateDeliveryCreate(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
	t.Helper()

	reqstr := "test-client-dup-delivery@gmail.com"
	gOpts := geoclient.NewDefaultClientOption()
	gOpts.Caller = "TestDeliveriesClient"
	geoClient, err := geoclient.NewClient(testLogger, gOpts)
	require.NoError(t, err)

	startAddr := createStartAddressTester(t, geoClient, reqstr)
	destAddr := createDestAddressTester(t, geoClient, reqstr)

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	shopId, txnId := "test-client-dup-delivery", "Cr341D3lv2y"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqstr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	})
	or = getOrderTester(t, dc, &api.GetOrderRequest{
		Id: or.Order.Id,
	})

	cdr := &api.CreateDeliveryRequest{
		OrderId:       or.Order.Id,
		RequestedBy:   reqstr,
		SourceId:      shopId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	}

	delResp := createDeliveryTester(t, dc, cdr)
	delResp = getDeliveryTester(t, dc, &api.GetDeliveryRequest{
		Id: delResp.Delivery.Id,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = dc.CreateDelivery(ctx, cdr)
	require.Error(t, err)
	e, ok := status.FromError(err)
	require.Equal(t, ok, true)
	require.Equal(t, e.Code(), codes.AlreadyExists)

	deleteOrderTester(t, dc, &api.DeleteOrderRequest{
		Id: or.Order.Id,
	})

	deleteDeliveryTester(t, dc, &api.DeleteDeliveryRequest{
		Id: delResp.Delivery.Id,
	})

	deleteAddressTester(t, geoClient, startAddr.Id, startAddr.RefId)
	deleteAddressTester(t, geoClient, destAddr.Id, destAddr.RefId)
}

func testInvalidDeliveryCreate(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
	t.Helper()

	gOpts := geoclient.NewDefaultClientOption()
	gOpts.Caller = "TestDeliveriesClient"
	geoClient, err := geoclient.NewClient(testLogger, gOpts)
	require.NoError(t, err)

	reqstr := "test-client-invalid-delivery@gmail.com"
	startAddr := createStartAddressTester(t, geoClient, reqstr)
	destAddr := createDestAddressTester(t, geoClient, reqstr)

	now := time.Now()
	start := now.Add(48 * time.Hour).Unix()
	end := now.Add(53 * time.Hour).Unix()

	shopId, txnId := "test-client-invalid-delivery", "Cr341D3lv2y"
	or := createOrderTester(t, dc, &api.CreateOrderRequest{
		ShopId:        shopId,
		RequestedBy:   reqstr,
		TransactionId: txnId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	})
	or = getOrderTester(t, dc, &api.GetOrderRequest{
		Id: or.Order.Id,
	})

	cdr := &api.CreateDeliveryRequest{
		OrderId:       or.Order.Id,
		RequestedBy:   "",
		SourceId:      shopId,
		OfferMin:      comffC.F12,
		OfferMax:      comffC.F15,
		StartId:       startAddr.Id,
		DestinationId: destAddr.Id,
		PkgHeight:     comffC.F10,
		PkgWidth:      comffC.F20,
		PkgDepth:      comffC.F12,
		StartTime:     start,
		EndTime:       end,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = dc.CreateDelivery(ctx, cdr)
	require.Error(t, err)
	e, ok := status.FromError(err)
	require.Equal(t, ok, true)
	require.Equal(t, e.Code(), codes.InvalidArgument)

	deleteOrderTester(t, dc, &api.DeleteOrderRequest{
		Id: or.Order.Id,
	})

	deleteAddressTester(t, geoClient, startAddr.Id, startAddr.RefId)
	deleteAddressTester(t, geoClient, destAddr.Id, destAddr.RefId)
}

func testScheduleCRUD(t *testing.T, dc delclient.Client, testLogger logger.AppLogger) {
	t.Helper()

	reqstr := "test-client-create-schedule@comfforts.com"
	courierId, initialOfferId := "test-client-schedule-create", "Cr341S63d2l"

	now := time.Now()
	sch := createScheduleTester(t, dc, &api.CreateScheduleRequest{
		CourierId:     courierId,
		OfferId:       initialOfferId,
		DayIdentifier: now.Unix(),
		RequestedBy:   reqstr,
	}, now)

	sch = getScheduleTester(t, dc, &api.GetScheduleRequest{
		Id: sch.Id,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upResp, err := dc.UpdateSchedule(ctx, &api.UpdateScheduleRequest{
		Id:          sch.Id,
		Status:      api.ScheduleStatus_SCHEDULED,
		RequestedBy: reqstr,
	})
	require.NoError(t, err)
	require.Equal(t, api.ScheduleStatus_SCHEDULED, upResp.Schedule.Status)

	searchTime := now.Add(45 * time.Minute)
	resp, err := dc.SearchSchedules(ctx, &api.GetSchedulesRequest{
		CourierId:     courierId,
		DayIdentifier: searchTime.Unix(),
	})
	require.NoError(t, err)
	require.Equal(t, true, len(resp.Schedules) > 0)
	require.Equal(t, upResp.Schedule.Id, resp.Schedules[0].Id)

	deleteScheduleTester(t, dc, &api.DeleteScheduleRequest{
		Id: sch.Id,
	})
}

func createStartAddressTester(t *testing.T, gCl geoclient.Client, rqstr string) *geoapi.Address {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := gCl.AddAddress(ctx, &geoapi.AddressRequest{
		Type:        geoapi.AddressType_GEO,
		Street:      "5101 S Pecos Rd",
		City:        "Las Vegas",
		State:       "NV",
		PostalCode:  "89120",
		Country:     comffC.US,
		RequestedBy: rqstr,
	})
	require.NoError(t, err)
	return resp.Address
}

func createDestAddressTester(t *testing.T, gCl geoclient.Client, rqstr string) *geoapi.Address {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := gCl.AddAddress(ctx, &geoapi.AddressRequest{
		Type:        geoapi.AddressType_GEO,
		Street:      "4001 S Maryland Pkwy",
		City:        "Las Vegas",
		State:       "NV",
		PostalCode:  "89119",
		Country:     comffC.US,
		RequestedBy: rqstr,
	})
	require.NoError(t, err)
	return resp.Address
}

func deleteAddressTester(t *testing.T, gCl geoclient.Client, id, refId string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := gCl.DeleteAddress(ctx, &geoapi.DeleteAddressRequest{
		Id:    id,
		RefId: refId,
	})
	require.NoError(t, err)
	require.Equal(t, true, resp.Ok)
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

func createScheduleTester(t *testing.T, client delclient.Client, req *api.CreateScheduleRequest, timeVal time.Time) *api.DeliverySchedule {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := client.CreateSchedule(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, req.CourierId, resp.Schedule.CourierId, "schedule courier id should match input courier id")
	assert.Equal(t, req.OfferId, resp.Schedule.InitialOfferId, "schedule initial offer id should match input offer id")
	assert.Equal(t, api.ScheduleStatus_CREATED, resp.Schedule.Status, "schedule status should be CREATED")

	year, month, day := timeVal.Date()
	dayStr := fmt.Sprintf("%d/%s/%d", year, month, day)
	assert.Equal(t, dayStr, resp.Schedule.DayIdentifier, "schedule day identifier should be valid for input timestamp ")
	return resp.Schedule
}

func getScheduleTester(t *testing.T, client delclient.Client, req *api.GetScheduleRequest) *api.DeliverySchedule {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.GetSchedule(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp.Schedule.Id, req.Id)
	return resp.Schedule
}

func deleteScheduleTester(t *testing.T, client delclient.Client, req *api.DeleteScheduleRequest) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.DeleteSchedule(ctx, req)
	require.NoError(t, err)
	require.Equal(t, true, resp.Ok)
}
