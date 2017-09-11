package com.cb.ss;

import java.util.Base64;
import java.util.HashMap;

public class CBSSConstants
{
	final static String createOrdersUrl = "https://ssapi.shipstation.com/orders/createorders";
	final static String ordersUrl = "https://ssapi.shipstation.com/orders/";
	final static String listUpUrlWithdate = "https://ssapi.shipstation.com/orders?createDateStart=";
	final static String listUpUrl = "https://ssapi.shipstation.com/orders";
	final static String ssKey = "Basic " + Base64.getEncoder().encodeToString(("59ccbfb6d5c448c8807a8de1b0a0f7c9:120a343873364944afe1b3351090f768").getBytes());
	final static String cbName = "raster-test";
	final static String cbKey = "test_1LY2Lcd7R2BwwG2PmulgVBu325B87LYDi";
	final static String shipmentUrl = "https://ssapi.shipstation.com/shipments";
	final static String ssWareHousesUrl = "https://ssapi.shipstation.com/warehouses";
	
	final static String FailedInvDets = "failed-inv-dets";
	final static String CBInvVsSSOrdNo = "cbinv-vs-ssordno";
	final static String SSOrdVsCBInv = "ssordno-vs-cbinv";
	final static String SSOrdVsCBOrd = "ssord-vs-cbord";
	final static String CBOrdVsSSOrd = "cbord-vs-ssord";

	final static String orderKey = "orderKey";
	
	final static int INIT_FETCH = 1;
	final static int FAILED_INV_PROCESS = 2;
	final static int SYNC = 3;
	
	/////////////// ShipStation CountryCode VS CurrencyCode /////////////////

	static final HashMap<String, String> SS_CURR_CODE = new HashMap<String, String>()
	{
		{ put("US", "USD"); put("AU", "AUD"); put("CA", "CAD"); put("GB", "GBP");}
	};
	
}
