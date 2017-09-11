package com.cb.ss;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;

import com.chargebee.Environment;
import com.chargebee.ListResult;
import com.chargebee.Result;
import com.chargebee.exceptions.InvalidRequestException;
import com.chargebee.models.Customer;
import com.chargebee.models.Invoice;
import com.chargebee.models.Order;
import com.chargebee.models.Subscription;
import com.chargebee.models.Invoice.BillingAddress;
import com.chargebee.models.Invoice.Discount;
import com.chargebee.models.Invoice.InvoiceListRequest;
import com.chargebee.models.Invoice.LineItem;
import com.chargebee.models.Invoice.ShippingAddress;
import com.chargebee.models.enums.BillingPeriodUnit;

public class CBSSIntegration
{

	private static final Logger logger = Logger.getLogger(CBSSIntegration.class.getName());

	String cbApiKey;
	String ssApiKey;
	String cbSite;
	String ssCurrCode;
	JSONObject currencies;
	JSONObject thirdPartyMapping;
	BufferedWriter out = new BufferedWriter(new FileWriter("/home/aravind/Dinesh/CBSSI.txt"));
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void main(String[] args) throws Exception
	{
		CBSSIntegration integ = new CBSSIntegration(CBSSConstants.cbKey, CBSSConstants.cbName, CBSSConstants.ssKey);
		integ.initSync();
	}

	private String getCBTimeZone(Date date)
	{
		formatter.setTimeZone(TimeZone.getTimeZone("Europe/London"));
		return formatter.format(date);
	}

	private String getSSTimeZone(Date date)
	{
		formatter.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		return formatter.format(date);
	}
	
	public CBSSIntegration(String cbApiKey, String cbSite, String ssApiKey) throws Exception
	{
		this.cbApiKey = cbApiKey;
		this.cbSite = cbSite;
		this.ssApiKey = ssApiKey;
		this.ssCurrCode = getSSCurrencyCode(ssApiKey);
		logger.log(Level.INFO ,"\n\tbase currency:" + ssCurrCode);
		out.write("\n\tbase currency:" + ssCurrCode);
		Client client = ClientBuilder.newClient();
		Response response = client.target("https://api.fixer.io/latest?base="+ssCurrCode).request(MediaType.TEXT_PLAIN_TYPE).get();
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			currencies = new JSONObject(response.readEntity(String.class)).getJSONObject("rates");
			logger.log(Level.INFO ,"\n\tcurrencies:" + currencies.toString());
			out.write("\n\tcurrencies:" + currencies.toString());
			if(currencies.length() == 0)
			{
				throw new RuntimeException("No Default Currency Values Found");
			}
		}
		else
		{
			throw new RuntimeException(response.getStatus() + ": " + response.getStatusInfo().getReasonPhrase());
		}
		thirdPartyMapping = getThirdPartyMapping();
	}

	private String getSSCurrencyCode(String ssApiKey) throws JSONException
	{

		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSConstants.ssWareHousesUrl)
		  .request(MediaType.TEXT_PLAIN_TYPE)
		  .header("Authorization", ssApiKey)
//		  .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("bc9e26b4606546b3a49da7e52547e542:2e96d8639728454dbda1f74971dc0d4e").getBytes()))
		  .get();
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			JSONArray warhouses = new JSONArray(response.readEntity(String.class));
			if(warhouses.length() != 0)
			{
				return CBSSConstants.SS_CURR_CODE.get(warhouses.getJSONObject(0).getJSONObject("originAddress").getString("country"));
			}
			throw new RuntimeException("ShipStation Native Country Not Defined, No Warhouses has been Configuered yet");
		}
		else
		{
			throw new RuntimeException(response.getStatus() + ": " + response.getStatusInfo().getReasonPhrase());
		}

	}

	public void initSync() throws Exception
	{
		logger.log(Level.INFO ,"\n\tStarting Initial Sync.");
		out.write("\n\tStarting Initial Sync.");
		processSync(CBSSConstants.INIT_FETCH);
	}

	private void processSync(int mode) throws Exception
	{
		JSONObject ssOrder;
		HashMap<String, String> invoiceVsOrder = new HashMap<String, String>();
		JSONArray ssAllOrders = new JSONArray();
		String nextOffSet = null;
		Response response;
		long lastSyncTime = System.currentTimeMillis();
		String invoicesAsCSV = null;
		int billingPeriod;
		Date orderDate;
		boolean isInvFailed;
		ListResult.Entry entry;
		Invoice invoice;
		ListResult result = null;
		int counter = 1;
		Environment.configure(cbSite, cbApiKey);

		if (isFailedInvProcess(mode))
		{
			JSONArray failedInvs = thirdPartyMapping.getJSONArray(CBSSConstants.FailedInvDets);
			logger.log(Level.INFO ,"\n\tMethod: processSync thirdPartyMapping FailedInvoices: " + failedInvs);
			out.write("\n\tMethod: processSync thirdPartyMapping FailedInvoices: " + failedInvs);
			
			if (failedInvs.length() == 0) // If no failed invoices.
			{ return; }
			invoicesAsCSV = getInvIdAsCSV(failedInvs);
		}
		do
		{
			try
			{
				InvoiceListRequest req = Invoice.list().limit(100).includeDeleted(false).offset(nextOffSet).status().is(Invoice.Status.PAID);
				if (invoicesAsCSV != null) // previous failed invoice handling
				{
					req.id().in(invoicesAsCSV);
				}
				if (isSync(mode)) // sync handling
				{
					logger.log(Level.INFO ,"\n\tObtaining Invoices from the account!");
					out.write("\n\tObtaining Invoices from the account!");
					req.updatedAt().after(new Timestamp(getCBLastSyncTime()));
				}
				result = req.request();
				nextOffSet = result.nextOffset();
				for (int j = 0; j < result.size(); j++)
				{
					entry = result.get(j);
					invoice = entry.invoice();
					if (checkFailedInvoice(invoice.id()) && !isFailedInvProcess(mode))
					{
						continue;
					}
					Subscription sub = Subscription.retrieve(invoice.subscriptionId()).request().subscription();
					billingPeriod = sub.billingPeriod(); // handling of recurring Invoices
					for (int i = 0; i < billingPeriod; i++)
					{
						orderDate = getOrderDate(i, sub.billingPeriodUnit(), invoice);
						isInvFailed = false;
						ssOrder = new JSONObject();
						fillCustomerInfo(invoice, ssOrder);
						isInvFailed = fillBillingAddress(invoice, ssOrder) ? false : true; // method returns true if all fields are available
						if (isInvFailed)
						{
							logger.log(Level.INFO ,"\n\tInvoice " + invoice.id() + "is failed due to invalid billing address");
							out.write("\n\tInvoice " + invoice.id() + "is failed due to invalid billing address");
							updateFailedInvoice(invoice.id(), "Invalid Billing Address");
							continue;
						}
						isInvFailed = fillShippingAddress(invoice, ssOrder) ? false : true; // method returns true if all fields are available

						if (isInvFailed)
						{
							logger.log(Level.INFO ,"\n\tInvoice " + invoice.id() + "is failed due to invalid shipping address");
							out.write("\n\tInvoice " + invoice.id() + "is failed due to invalid shipping address");
							updateFailedInvoice(invoice.id(), "Invalid Shipping Address");
							continue;
						}
						fillOrders(invoice, ssOrder, orderDate, invoiceVsOrder, isSync(mode), billingPeriod);
						ssAllOrders.put(ssOrder);

						if (counter == 100)
						{
							response = createShipStationOrders(ssAllOrders);
							JSONObject respJSON = handleResponse(response, ssAllOrders, invoiceVsOrder);
							if (response.getStatus() == HttpStatus.SC_OK || response.getStatus() == HttpStatus.SC_ACCEPTED)
							{
								updCBLastSyncTime(lastSyncTime);
								createCBOrders(respJSON);
								updSSLastSyncTime(lastSyncTime);
							}
							ssAllOrders = new JSONArray();
							counter = 0;
						}
					}
					counter++;
				}
				if (nextOffSet == null && counter > 1)
				{
					response = createShipStationOrders(ssAllOrders);
					JSONObject respJSON = handleResponse(response, ssAllOrders, invoiceVsOrder);
					if (response.getStatus() == HttpStatus.SC_OK || response.getStatus() == HttpStatus.SC_ACCEPTED)
					{
						createCBOrders(respJSON);
					}
					ssAllOrders = new JSONArray();
					counter = 0;
				}
			}
			catch (InvalidRequestException e)
			{
				if (e.apiErrorCode.equals("site_not_found"))
				{
					logger.log(Level.INFO ,"\n\tChargeBee Site Not found");
					out.write("\n\tChargeBee Site Not found");
				}
				else if (e.apiErrorCode.equals("api_authentication_failed"))
				{
					logger.log(Level.INFO ,"\n\tChargeBee Key is Invalid");
					out.write("\n\tChargeBee Key is Invalid");
				}
				e.printStackTrace();
				throw e;

			}
		} 
		while (nextOffSet != null);
		updThirdPartyMapping("Third Party Mapping");
		logger.log(Level.INFO ,"\n\tSync operation finished!");
		out.write("\n\tSync operation finished!");
		logger.log(Level.INFO, "Sync operation finished!");
	}

	private void updThirdPartyMapping(String action) throws IOException
	{
		logger.log(Level.INFO ,"\n\tThird Party JSON : " + thirdPartyMapping.toString());
		out.write("\n\t" + action + " : " + thirdPartyMapping.toString());
	}

	private boolean checkFailedInvoice(String invoiceId) throws JSONException
	{
		JSONArray array = thirdPartyMapping.getJSONArray(CBSSConstants.FailedInvDets);
		JSONObject obj = new JSONObject();
		for (int i = 0; i < array.length(); i++)
		{
			obj = array.getJSONObject(i);
			if (invoiceId.equals(obj.getString("invoice-id"))) { return true; }
		}
		return false;
	}

	private Date getOrderDate(int index, BillingPeriodUnit billingPeriodUnit, Invoice invoice) throws IOException
	{
		logger.log(Level.INFO ,"\n\tindex : " + String.valueOf(index));
		out.write("\n\tindex : " + String.valueOf(index));
		Timestamp orgDate = invoice.date();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(orgDate.getTime());
		if (billingPeriodUnit.equals(BillingPeriodUnit.MONTH))
		{
			cal.add(Calendar.MONTH, index);
		}
		else if (billingPeriodUnit.equals(BillingPeriodUnit.WEEK))
		{
			cal.add(Calendar.WEEK_OF_MONTH, index);
		}
		else if (billingPeriodUnit.equals(BillingPeriodUnit.YEAR))
		{
			cal.add(Calendar.YEAR, index);
		}
		logger.log(Level.INFO ,"\n\tDate : " + cal.getTime());
		out.write("\n\tDate : " + cal.getTime());
		return cal.getTime();
	}

	private JSONObject handleResponse(Response response, JSONArray ssOrders, HashMap<String, String> invoiceVsOrder)
			throws JSONException, InterruptedException, IOException
	{
		JSONObject obj = null;
		int status = response.getStatus();
		if (status == HttpStatus.SC_OK || status == HttpStatus.SC_ACCEPTED)
		{
			obj = handleSuccessResponse(response, invoiceVsOrder);
		}
		else
		{
			handleErrorResponse(response, ssOrders, invoiceVsOrder);
		}
		return obj;
	}

	private void updOrdInThirdPartyMapping(JSONObject order, String invoiceId) throws JSONException, IOException
	{
		logger.log(Level.INFO ,"\n\tMethod: updOrdInThirdPartyMapping");
		out.write("\n\tMethod: updOrdInThirdPartyMapping");
		JSONArray failedInvs = thirdPartyMapping.getJSONArray(CBSSConstants.FailedInvDets);
		if (!order.getBoolean("success"))
		{
			JSONObject obj;

			for (int i = 0; i < failedInvs.length(); i++)
			{
				obj = failedInvs.getJSONObject(i);
				if (invoiceId.equals(obj.getString("invoice-id")))
				{
					obj.put("reason", order.getString("errorMessage"));
					failedInvs.put(obj);
					thirdPartyMapping.put(CBSSConstants.FailedInvDets, failedInvs);
					return;
				}
			}
			obj = new JSONObject();
			obj.put("invoice-id", invoiceId);
			obj.put("reason", order.getString("errorMessage"));
			failedInvs.put(obj);
			thirdPartyMapping.put(CBSSConstants.FailedInvDets, failedInvs);

			logger.log(Level.INFO ,"\n\tMethod: updOrdInThirdPartyMapping");
			out.write("\n\tMethod: updOrdInThirdPartyMapping");
			updThirdPartyMapping("failedinvoice inserted");
		}
		else
		{
			JSONArray array = thirdPartyMapping.getJSONArray(CBSSConstants.CBInvVsSSOrdNo);
			JSONArray ssOrdVsCBInv = thirdPartyMapping.getJSONArray(CBSSConstants.SSOrdVsCBInv);
			JSONObject obj;
			for (int i = 0; i < array.length(); i++)
			{
				obj = array.getJSONObject(i);
				if (obj.has(invoiceId)) { return; }
			}
			obj = new JSONObject();
			obj.put(invoiceId, order.getString(CBSSConstants.orderKey));
			array.put(obj);
			obj = new JSONObject();
			obj.put(order.getString(CBSSConstants.orderKey), invoiceId);
			ssOrdVsCBInv.put(obj);
			thirdPartyMapping.put(CBSSConstants.SSOrdVsCBInv, ssOrdVsCBInv);
			thirdPartyMapping.put(CBSSConstants.CBInvVsSSOrdNo, array);
			logger.log(Level.INFO ,"\n\tMethod: updOrdInThirdPartyMapping");
			out.write("\n\tMethod: updOrdInThirdPartyMapping");
			updThirdPartyMapping("SSOrdVsCBInv & CBInvVsSSOrdNo updated");

			for (int i = 0; i < failedInvs.length(); i++) //remove failed invoices from the thirdparty
			{
				obj = failedInvs.getJSONObject(i);
				if (invoiceId.equals(obj.getString("invoice-id")))
				{
					failedInvs.remove(i);
					thirdPartyMapping.put(CBSSConstants.FailedInvDets, failedInvs);
					updThirdPartyMapping("failedinvoice removed");
					return;
				}
			}
		}
	}

	private JSONObject handleSuccessResponse(Response response, HashMap<String, String> invoiceVsOrder)
			throws JSONException, IOException
	{
		logger.log(Level.INFO ,"\n\tMethod: handleSuccessResponse");
		out.write("\n\tMethod: handleSuccessResponse");
		JSONObject obj = new JSONObject(response.readEntity(String.class));
		JSONArray results = obj.getJSONArray("results");
		JSONObject order;
		for (int i = 0; i < results.length(); i++)
		{
			order = results.getJSONObject(i);
			updOrdInThirdPartyMapping(order, invoiceVsOrder.get(order.getString("orderNumber")));
			
		}
		return obj;
	}

	private void handleErrorResponse(Response response, JSONArray ssOrders, HashMap<String, String> invoiceVsOrder)
			throws JSONException, InterruptedException, IOException
	{
		int status = response.getStatus();
		int failedInvoiceCount = 0;
		JSONObject obj;
		logger.log(Level.INFO ,"\n\tstatus: " + status);
		out.write("\n\tstatus: " + status);
		if ((status == 429))
		{
			Thread.sleep(600000);
			createShipStationOrders(ssOrders);
		}
		else if (status == HttpStatus.SC_UNAUTHORIZED)
		{
			throw new RuntimeException("Invalid API Key");
		}
		else if ((status == HttpStatus.SC_BAD_REQUEST) || (status == HttpStatus.SC_FORBIDDEN)
				|| (status == HttpStatus.SC_NOT_FOUND) || status == HttpStatus.SC_INTERNAL_SERVER_ERROR)
		{
			while (failedInvoiceCount < ssOrders.length())
			{
				obj = ssOrders.getJSONObject(failedInvoiceCount);
				updateFailedInvoice(invoiceVsOrder.get(obj.getString(CBSSConstants.orderKey)), "Internal Error");
				failedInvoiceCount++;
			}
		}
	}

	private void updateFailedInvoice(String invoiceId, String reason) throws JSONException, IOException
	{
		JSONObject obj;
		JSONArray array = thirdPartyMapping.getJSONArray(CBSSConstants.FailedInvDets);
		out.write("thirdPartyMapping : " + thirdPartyMapping.names().toString());
		for (int i = 0; i < array.length(); i++)
		{
			obj = array.getJSONObject(i);
			if (invoiceId.equals(obj.getString("invoice-id")))
			{
				obj.put("reason", reason);
				array.put(obj);
				thirdPartyMapping.put(CBSSConstants.FailedInvDets, array);
				try {
					logger.log(Level.INFO ,"\n\tMethod: updateFailedInvoice");
					out.write("\n\tMethod: updateFailedInvoice");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				updThirdPartyMapping("failed invoices updated");
				return;
			}
		}
		obj = new JSONObject();
		obj.put("invoice-id", invoiceId);
		obj.put("reason", reason);
		array.put(obj);
		thirdPartyMapping.put(CBSSConstants.FailedInvDets, array);
		out.write("thirdPartyMapping : " + thirdPartyMapping.toString());
		logger.log(Level.INFO ,"\n\tMethod: updateFailedInvoice");
		out.write("\n\tMethod: updateFailedInvoice");
		updThirdPartyMapping("failed invoices inserted");
	}

	private void updSSLastSyncTime(long time) throws JSONException, IOException
	{
		thirdPartyMapping.put("ss-last-sync-time", time);
		logger.log(Level.INFO ,"\n\tMethod: updLastSyncTime");
		out.write("\n\tMethod: updLastSyncTime");
		updThirdPartyMapping("SS last sync time updated");
	}

	private void updCBLastSyncTime(long time) throws JSONException, IOException
	{
		thirdPartyMapping.put("cb-last-sync-time", time);
		logger.log(Level.INFO ,"\n\tMethod: updLastSyncTime");
		out.write("\n\tMethod: updLastSyncTime");
		updThirdPartyMapping("CB last sync time updated");
	}

	
	public void sync() throws Exception
	{
		handlePreviousFailedInvoice();
		processSync(CBSSConstants.SYNC);
	}

	private void handlePreviousFailedInvoice() throws Exception
	{
		processSync(CBSSConstants.FAILED_INV_PROCESS);
	}

	private boolean fillBillingAddress(Invoice invoice, JSONObject ssOrder) throws Exception
	{
		logger.log(Level.INFO ,"\n\tfilling the Billing address for invoice :: " + invoice.id());
		out.write("\n\tfilling the Billing address for invoice :: " + invoice.id());
		Result result = Customer.retrieve(invoice.customerId()).request();
		Customer customer = result.customer();
		if (invoice.billingAddress() == null && customer.billingAddress() == null)
		{
			logger.log(Level.INFO ,"\n\tbilling address is empty for invoice ::  " + invoice.id());
			out.write("\n\tbilling address is empty for invoice ::  " + invoice.id());
			return false;
		}
		JSONObject billTo = new JSONObject();
		BillingAddress address = invoice.billingAddress();
		boolean validAdd = fillBillingAddress(address, customer, billTo, true, invoice);
		if (!validAdd)
		{
			logger.log(Level.INFO ,"\n\tbilling address is not a valid for invoice :: " + invoice.id());
			out.write("\n\tbilling address is not a valid for invoice :: " + invoice.id());
			return false;
		}
		ssOrder.put("billTo", billTo);
		return true;
	}

	private boolean fillBillingAddress(BillingAddress address, Customer customer, JSONObject object, boolean isBilling,
			Invoice invoice) throws JSONException
	{

		if (address.firstName() == null && address.lastName() == null)
		{
			if (customer.firstName() == null && customer.lastName() == null)
			{
				return false;
			}
			else if (customer.firstName() == null && customer.lastName() != null)
			{
				object.put("name", customer.lastName());
			}
			else if (customer.firstName() != null && customer.lastName() == null)
			{
				object.put("name", customer.firstName());
			}
			else if (customer.firstName() != null && customer.lastName() != null)
			{
				object.put("name", customer.firstName() + customer.lastName());
			}
		}
		else if (address.firstName() == null && address.lastName() != null)
		{
			object.put("name", address.lastName());
		}
		else if (address.firstName() != null && address.lastName() == null)
		{
			object.put("name", address.firstName());
		}
		else
		{
			object.put("name", address.firstName() + address.lastName());
		}
		if (address.company() == null && customer.company() == null)
		{
			return false;
		}
		else if (address.company() == null && customer.company() != null)
		{
			object.put("company", customer.company());
		}
		else
		{
			object.put("company", address.company());
		}
		if (address.line1() == null)
		{
			return false;
		}
		else
		{
			object.put("street1", address.line1());
		}
		if (address.line2() == null)
		{
			return false;
		}
		else
		{
			object.put("street2", address.line2());
		}
		if (address.line3() != null)
		{
			object.put("street3", address.line3());
		}
		if (address.city() == null)
		{
			return false;
		}
		else
		{
			object.put("city", address.city());
		}
		if (address.state() == null)
		{
			return false;
		}
		else
		{
			object.put("state", address.state());
		}
		if (address.zip() == null)
		{
			return false;
		}
		else
		{
			object.put("postalCode", address.zip());
		}
		if (address.country() == null)
		{
			return false;
		}
		else
		{
			object.put("country", address.country());
		}
		if (address.phone() != null)
		{
			object.put("phone", address.phone());
		}
		return true;
	}

	private boolean fillShippingAddress(ShippingAddress address, Customer customer, JSONObject object, Invoice invoice)
			throws JSONException
	{
		if (address.firstName() == null && address.lastName() == null)
		{
			if (customer.firstName() == null && customer.lastName() == null)
			{
				return false;
			}
			else if (customer.firstName() == null && customer.lastName() != null)
			{
				object.put("name", customer.lastName());
			}
			else if (customer.firstName() != null && customer.lastName() == null)
			{
				object.put("name", customer.firstName());
			}
			else if (customer.firstName() != null && customer.lastName() != null)
			{
				object.put("name", customer.firstName() + customer.lastName());
			}
		}
		else if (address.firstName() == null && address.lastName() != null)
		{
			object.put("name", address.lastName());
		}
		else if (address.firstName() != null && address.lastName() == null)
		{
			object.put("name", address.firstName());
		}
		else
		{
			object.put("name", address.firstName() + address.lastName());
		}
		if (address.company() == null && customer.company() == null)
		{
			return false;
		}
		else if (address.company() == null && customer.company() != null)
		{
			object.put("company", customer.company());
		}
		else
		{
			object.put("company", address.company());
		}
		if (address.line1() == null)
		{
			return false;
		}
		else
		{
			object.put("street1", address.line1());
		}
		if (address.line2() == null)
		{
			return false;
		}
		else
		{
			object.put("street2", address.line2());
		}
		if (address.line3() != null)
		{
			object.put("street3", address.line3());
		}
		if (address.city() == null)
		{
			return false;
		}
		else
		{
			object.put("city", address.city());
		}
		if (address.state() == null)
		{
			return false;
		}
		else
		{
			object.put("state", address.state());
		}
		if (address.zip() == null)
		{
			return false;
		}
		else
		{
			object.put("postalCode", address.zip());
		}
		if (address.country() == null)
		{
			return false;
		}
		else
		{
			object.put("country", address.country());
		}
		if (address.phone() != null)
		{
			object.put("phone", address.phone());
		}
		return true;
	}

	public boolean fillShippingAddress(Invoice invoice, JSONObject ssOrder) throws Exception
	{
		Result result = Customer.retrieve(invoice.customerId()).request(); // retrieving customerId																 
		Customer customer = result.customer();
		if (invoice.shippingAddress() == null) { return false; }
		JSONObject shipTo = new JSONObject();
		ShippingAddress address = invoice.shippingAddress();
		boolean validAdd = fillShippingAddress(address, customer, shipTo, invoice); // checking valid address
		if (!validAdd) { return false; }
		if (address.validationStatus() != null) shipTo.put("addressVerified", address.validationStatus().toString());
		ssOrder.put("shipTo", shipTo);
		return true;
	}

	private void fillCustomerInfo(Invoice invoice, JSONObject order) throws IOException, JSONException
	{
		logger.log(Level.INFO ,"\n\tfilling Customer Info of invoice-id: " + invoice.id());
		out.write("\n\tfilling Customer Info of invoice-id: " + invoice.id());
		order.put("customerUsername", cbSite);
		order.put("customerEmail", getCBCustomerEmail(invoice.customerId()));
	}

	private String getCBCustomerEmail(String userId_cb) throws IOException
	{
		Result result = Customer.retrieve(userId_cb).request();
		return result.customer().email();
	}

	private String getOrderNoForInvoice(String invoiceId, String orderNo) throws JSONException, IOException
	{
		JSONArray array = thirdPartyMapping.getJSONArray(CBSSConstants.SSOrdVsCBInv);
		logger.log(Level.INFO ,"\n\tMethod: getOrderNoForInvoice");
		out.write("\n\tMethod: getOrderNoForInvoice");
		JSONObject obj;
		for (int i = 0; i < array.length(); i++)
		{
			obj = array.getJSONObject(i);
			if (obj.has(invoiceId)) { return obj.getString(invoiceId); }
		}
		return orderNo;
	}

	private void fillOrders(Invoice invoice, JSONObject ssOrder, Date orderDate,
			HashMap<String, String> invoiceVsOrder, boolean isSync, int billingPeriod) throws Exception
	{
		JSONObject order;
		JSONObject product;
		String orderNumber = invoice.id();
		JSONArray lineItems = new JSONArray();
		String cbCurrCode = invoice.currencyCode();
		invoiceVsOrder.put(orderNumber, invoice.id() + "_" + UUID.randomUUID());
		JSONArray array = thirdPartyMapping.getJSONArray(CBSSConstants.CBInvVsSSOrdNo);
		for(int i = 0; i < array.length(); i++)
		{
			order = array.getJSONObject(i);
			if(order.has("orderNumber"))
			{
				ssOrder.put("orderKey", order.getString("orderNumber"));
			}
		}
		
		logger.log(Level.INFO ,"\n\torderDate : " + getSSTimeZone(orderDate));
		out.write("\n\torderDate : " + getSSTimeZone(orderDate));
		
		if (isSync)
		{
			orderNumber = getOrderNoForInvoice(invoice.id(), orderNumber);
		}
		
		ssOrder.put("orderNumber", orderNumber);
		ssOrder.put("orderDate", getSSTimeZone(orderDate));
		ssOrder.put("paymentDate", getSSTimeZone(invoice.paidAt()));
		ssOrder.put("orderStatus", getOrderStatus(invoice.status().toString()));
		ssOrder.put("amountPaid", getSSCurrency(invoice.amountPaid(), cbCurrCode)/billingPeriod);
		out.write("\n\tinvoice : " + orderNumber);
		logger.log(Level.INFO ,"\n\tamountPaid : " + invoice.amountPaid());
		out.write("\n\tamountPaid : " + invoice.amountPaid());
		logger.log(Level.INFO ,"\n\tamountPaid/billingPeriod : " + invoice.amountPaid()/billingPeriod);
		out.write("\n\tamountPaid/billingPeriod : " + invoice.amountPaid()/billingPeriod);
		logger.log(Level.INFO ,"\n\tamountPaid after conversion : " + getSSCurrency(invoice.amountPaid(), cbCurrCode));
		out.write("\n\tamountPaid after conversion : " + getSSCurrency(invoice.amountPaid(), cbCurrCode));
		logger.log(Level.INFO ,"\n\tamountPaid/billingPeriod after conversion : " + getSSCurrency(invoice.amountPaid(), cbCurrCode)/billingPeriod);
		out.write("\n\tamountPaid/billingPeriod after conversion : " + getSSCurrency(invoice.amountPaid(), cbCurrCode)/billingPeriod);
		logger.log(Level.INFO ,"\n\tdiscount : " + invoice.discounts());
		out.write("\n\tdiscount : " + invoice.discounts());
		for (LineItem item : invoice.lineItems())
		{
			
			product = new JSONObject();
			product.put("lineItemKey", item.id());
			product.put("sku", item.entityId());
			product.put("name", item.description());
			product.put("quantity", item.quantity());
			product.put("unitPrice", getSSCurrency(item.unitAmount(), cbCurrCode)/billingPeriod);
			product.put("taxAmount", getSSCurrency(item.taxAmount(), cbCurrCode)/billingPeriod);
			product.put("adjustment", false);
			out.write("\n\tunitPrice after conversion : " + getSSCurrency(item.unitAmount(), cbCurrCode));
			out.write("\n\tunitPrice/billingPeriod after conversion : " + getSSCurrency(item.unitAmount(), cbCurrCode)/billingPeriod);
			out.write("\n\titemTaxAmount after conversion : " + getSSCurrency(item.taxAmount(), cbCurrCode));
			out.write("\n\titemTaxAmount/billingPeriod after conversion : " + getSSCurrency(item.taxAmount(), cbCurrCode)/billingPeriod);
			lineItems.put(product);
		}
		double disc = 0;
		for(Discount discount : invoice.discounts())
		{
			product = new JSONObject();
			product.put("lineItemKey", discount.entityId());
			product.put("sku", discount.entityType());
			product.put("name", discount.description());
			product.put("unitPrice", getSSCurrency(discount.amount(), cbCurrCode)/billingPeriod);
			product.put("quantity", 1);
			product.put("adjustment", true);
			lineItems.put(product);

			disc+= getSSCurrency(discount.amount(), cbCurrCode);
		}
		out.write("\n\tdiscountAmount after conversion : " + disc);
		out.write("\n\tdiscountAmount/billingPeriod after conversion : " + disc/billingPeriod);
		ssOrder.put("items", lineItems);

	}

	private double getSSCurrency(Integer amount, String cbCurrCode) throws JSONException
	{
		return ((double)amount/100) / currencies.getDouble(cbCurrCode);
		
	}

	private Response createShipStationOrders(JSONArray shipStationJson) throws IOException
	{
		logger.log(Level.INFO ,"\n\tcreating shipstation orders");
		out.write("\n\tcreating shipstation orders");
		Client client = ClientBuilder.newClient();
		Entity<String> payload = Entity.json(shipStationJson.toString());
		return client.target(CBSSConstants.createOrdersUrl).request(MediaType.APPLICATION_JSON_TYPE).header(
				"Authorization", ssApiKey).post(payload);
	}

	private void createCBOrders(JSONObject ssOrderResp) throws JSONException, InterruptedException, IOException
	{
		logger.log(Level.INFO ,"\n\tgetting shipstation orders");
		out.write("\n\tgetting shipstation orders");
		Client client = ClientBuilder.newClient();
		String orderNo;
		String orderStatus = null;
		String customerNote = null;
		String trackingId = null;
		String batchId = null;
		String referenceId;
		StringBuilder url = new StringBuilder(CBSSConstants.ordersUrl);
		if(getSSLastSyncTime() != null)
		{
			Date fromDate = new Date(getSSLastSyncTime());
			Date toDate = new Date(System.currentTimeMillis());
			url.append("&createDateStart=").append(getSSTimeZone(fromDate)).append("&createDateEnd=").append(getSSTimeZone(toDate)).
			append("&modifyDateStart=").append(getSSTimeZone(fromDate)).append("&modifyDateEnd=").append(getSSTimeZone(toDate));
		}
		Response response = client.target(url.toString()).request(MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey).get();
		
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			String orders = response.readEntity(String.class);
			JSONObject respJSON = new JSONObject(orders);
			JSONArray shipstationOrders = respJSON.getJSONArray("orders");
			JSONObject orderShipInfo;

			for (int i = 0; i < shipstationOrders.length(); i++)
			{
				JSONObject order = shipstationOrders.getJSONObject(i);
				orderNo = order.getString("orderNumber");
				if (isOrderFromCB(order, ssOrderResp))
				{
					referenceId = order.getString("orderId");
					if (order.has("orderStatus"))
					{
						orderStatus = order.getString("orderStatus");
					}
					if (order.has("customerNote"))
					{
						customerNote = order.getString("customerNote");
					}
					orderShipInfo = getShipInfo(orderNo);
					if (order.has("trackingNumber"))
					{
						trackingId = orderShipInfo.getString("trackingNumber");
					}
					if (order.has("batchNumber"))
					{
						batchId = orderShipInfo.getString("batchNumber");
					}
					logger.log(Level.INFO ,"\n\tCreating ChargeBee orders");
					out.write("\n\tCreating ChargeBee orders");
					create(orderNo, orderStatus, batchId, trackingId, customerNote, referenceId);
				}
			}
		}
	}

	private boolean isOrderFromCB(JSONObject order, JSONObject ssOrderResp) throws JSONException, IOException
	{
		logger.log(Level.INFO ,"\n\tSplitting Shipstation Orders");
		out.write("\n\tSplitting Shipstation Orders");
		JSONArray result = ssOrderResp.getJSONArray("results");
		JSONObject obj;
		for (int i = 0; i < result.length(); i++)
		{
			obj = result.getJSONObject(i);
			if (order.getString("orderId").equals(obj.getString("orderId"))) { return true; }
		}
		return false;
	}

	private JSONObject getShipInfo(String orderNo) throws JSONException
	{
		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSConstants.shipmentUrl + "?orderNumber=" + orderNo).request(
				MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey).get();
		return new JSONObject(response.readEntity(String.class));
	}

	private void create(String orderNumber, String orderStatus, String batchId, String trackingId, String customerNote, String referenceId) throws IOException, JSONException
	{
		String cbOrdId = getCBOrdId(referenceId);
		if(cbOrdId == null) // new order
		{
			
			Result result = Order.create().id(referenceId).invoiceId(orderNumber).fulfillmentStatus(orderStatus)
					.referenceId(referenceId).batchId(batchId).note(customerNote).request();
			logger.log(Level.INFO ,"\n\tChargeBee orders created!");
			out.write("\n\tChargeBee orders created!");
			Order order = result.order();
			updCBOrderVsSSOrder(order.id(), referenceId);
			logger.log(Level.INFO ,"\n\tChargeBee order response:" + order);
			out.write("\n\tChargeBee order response:" + order);
		}
		else
		{
			Order.update(getCBOrdId(referenceId)).fulfillmentStatus(orderStatus).referenceId(referenceId).batchId(batchId).note(customerNote).request();
		}
	}

	private String getCBOrdId(String ssOrdId) throws JSONException, IOException
	{
		logger.log(Level.INFO ,"\n\tgetting the ChargeBee orderId for Update");
		out.write("\n\tgetting the ChargeBee orderId for Update");
		JSONArray ssOrdVsCBOrd = thirdPartyMapping.getJSONArray(CBSSConstants.SSOrdVsCBOrd);
		JSONObject obj;
		for (int i = 0; i < ssOrdVsCBOrd.length(); i++)
		{
			obj = ssOrdVsCBOrd.getJSONObject(i);
			if (obj.has(ssOrdId)) { return obj.getString(ssOrdId); }
		}
		return null;
	}

	private void updCBOrderVsSSOrder(String cbOrderId, String ssOrderId) throws JSONException, IOException
	{
		JSONArray cbOrdVsSSOrd = thirdPartyMapping.getJSONArray(CBSSConstants.CBOrdVsSSOrd);
		JSONArray ssOrdVsCBOrd = thirdPartyMapping.getJSONArray(CBSSConstants.SSOrdVsCBOrd);
		JSONObject obj;

		for (int i = 0; i < cbOrdVsSSOrd.length(); i++)
		{
			obj = cbOrdVsSSOrd.getJSONObject(i);
			if (obj.has(cbOrderId)) { return; }
		}
		obj = new JSONObject();
		obj.put(cbOrderId, ssOrderId);
		cbOrdVsSSOrd.put(obj);
		thirdPartyMapping.put(CBSSConstants.CBOrdVsSSOrd, cbOrdVsSSOrd);
		obj = new JSONObject();
		obj.put(ssOrderId, cbOrderId);
		ssOrdVsCBOrd.put(obj);
		thirdPartyMapping.put(CBSSConstants.SSOrdVsCBOrd, ssOrdVsCBOrd);
		logger.log(Level.INFO ,"\n\tMethod: updCBOrderVsSSOrder");
		out.write("\n\tMethod: updCBOrderVsSSOrder");
		updThirdPartyMapping("CBOrdVsSSOrd & SSOrdVsCBOrd updated");
	}

	public String getTime()
	{
		long date = (new Date().getTime()) / 1000L;
		return String.valueOf(date);
	}

	public Long getCBLastSyncTime() throws JSONException
	{
		if(!thirdPartyMapping.has("cb-last-sync-time"))
		{
			return null;
		}
		return thirdPartyMapping.getLong("cb-last-sync-time");
	}

	public Long getSSLastSyncTime() throws JSONException
	{
		if(!thirdPartyMapping.has("ss-last-sync-time"))
		{
			return null;
		}
		return thirdPartyMapping.getLong("ss-last-sync-time");
	}

	
	private String getInvIdAsCSV(JSONArray array) throws JSONException
	{
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < array.length(); i++)
		{
			builder.append(array.getJSONObject(i).getString("invoice-id")).append(",");
		}
		return builder.deleteCharAt(builder.lastIndexOf(",")).toString();
	}

	private JSONObject getThirdPartyMapping() throws JSONException
	{
		JSONObject obj = null;

		if (obj == null)
		{
			obj = new JSONObject();
			obj.put(CBSSConstants.FailedInvDets, new JSONArray());
			obj.put(CBSSConstants.CBInvVsSSOrdNo, new JSONArray());
			obj.put(CBSSConstants.SSOrdVsCBInv, new JSONArray());
			obj.put(CBSSConstants.SSOrdVsCBOrd, new JSONArray());
			obj.put(CBSSConstants.CBOrdVsSSOrd, new JSONArray());
		}
		return obj;
	}

	private boolean isSync(int mode)
	{
		return mode == CBSSConstants.SYNC;
	}

	private boolean isFailedInvProcess(int mode)
	{
		return mode == CBSSConstants.FAILED_INV_PROCESS;
	}

	private String getOrderStatus(String invoiceStatus) throws IOException
	{
		out.write(invoiceStatus);
		String status = null;
		if ("PENDING".equals(invoiceStatus))
		{
			status = "on_hold";
		}
		else if ("VOIDED".equals(invoiceStatus))
		{
			status = "cancelled";
		}
		else if ("NOT_PAID".equals(invoiceStatus) || "payment_due".equals(invoiceStatus))
		{
			status = "awaiting_payment";
		}
		else if ("PAID".equals(invoiceStatus))
		{
			status = "awaiting_shipment";
		}
		else if ("POSTED".equals(invoiceStatus))
		{
			status = "shipped";
		}
		return status;
	}

	private boolean isInitSync(int mode)
	{
		return mode == CBSSConstants.INIT_FETCH;
	}

}
