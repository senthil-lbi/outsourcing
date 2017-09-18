package com.cb.ss;

import java.util.Base64;
import java.util.HashMap;

import com.chargebee.models.Order.Status;

public class CBSSIConstants
{

	/////////////// CBSSI Modes /////////////////
	
	final static int INIT_FETCH = 1;
	final static int FAILED_INV_PROCESS = 2;
	final static int SYNC = 3;
	
	/////////////// ChargeBee Api_End_Points And Constants /////////////////
	
	final static String cbName = "raster-test";
	final static String cbKey = "test_1LY2Lcd7R2BwwG2PmulgVBu325B87LYDi";
	
	final static String cbPending = "PENDING";
	final static String cbVoided = "VOIDED";
	final static String cbNotPaid = "NOT_PAID";
	final static String cbPaid = "PAID";
	final static String cbPaymentDue = "PAYMENT_DUE";
	final static String cbPosted = "POSTED";

	/////////////// ShipStation Api_End_Points And Constants /////////////////

	final static String orderKey = "orderKey";
	final static String orderNumber = "orderNumber";
	
	final static String ssAwaitingPay = "awaiting_payment";
	final static String ssAwaitingShip = "awaiting_shipment";
	final static String ssOnHold = "on_hold";
	final static String ssShipped = "shipped";
	final static String ssCancelled = "cancelled";
	
	final static String ssKey = "Basic " + Base64.getEncoder().encodeToString(
			("59ccbfb6d5c448c8807a8de1b0a0f7c9:120a343873364944afe1b3351090f768").getBytes());
	final static String ordersUrl = "https://ssapi.shipstation.com/orders";
	final static String shipmentUrl = "https://ssapi.shipstation.com/shipments";
	final static String listOrdersAfter = "https://ssapi.shipstation.com/orders?modifyDateStart=";
	final static String createOrdersUrl = "https://ssapi.shipstation.com/orders/createorders";
	final static String ssWareHousesUrl = "https://ssapi.shipstation.com/warehouses";
	
	final static HashMap<String, String> SS_CURR_CODE = new HashMap<String, String>()
	{
		{
			put("US", "USD");
			put("AU", "AUD");
			put("CA", "CAD");
			put("GB", "GBP");
		}
	};
	/////////////// Thirdparty_Mapping Fields /////////////////
	
	final static String CBInvIdVsCBCusId = "cbinvid-cbcusid";
	final static String CBInvIdVsCBSubId = "cbinvid-cbsubid";
	final static String CBCusIdVsCBInvId = "cbcusid-cbinvid";
	final static String CBSubIdVsCBInvId = "cbsubid-cbinvid";
	final static String FailedInvDets = "failed-inv-dets";
	final static String CBInvIdVSSOrdNo = "ssordno-vs-cbinvid";
	final static String SSOrdNoVsSSOrderKey = "ssordno-vs-ssordkey";
	final static String SSOrdKeyVsCBInvId = "ssordkey-vs-cbinvid";
	final static String SSOrdVsCBOrd = "ssord-vs-cbord";
	final static String CBOrdVsSSOrd = "cbord-vs-ssord";
	final static String CBLastSyncTime = "cb-last-sync-time";
	final static String SSLastSyncTime = "ss-last-sync-time";
	
	final static HashMap<String, Status> cbOrdStatusForSSOrdStatus = new HashMap<String, Status>()
	{
		{
			put("awaiting_payment", Status.VOIDED);
			put("awaiting_shipment", Status.PROCESSING);
			put("on_hold", Status.VOIDED);
			put("shipped", Status.COMPLETE);
			put("cancelled", Status.CANCELLED);
		}
	};

	final static HashMap<String, String> ssOrdStatusOfCBInvStatus = new HashMap<String, String>()
	{
		{
			put("PAYMENT_DUE", "awaiting_payment");
			put("PAID", "awaiting_shipment");
			put("PENDING", "on_hold");
			put("POSTED", "shipped");
			put("VOIDED", "cancelled");
		}
	};
}
