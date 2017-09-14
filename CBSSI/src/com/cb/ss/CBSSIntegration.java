package com.cb.ss;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;

import com.chargebee.APIException;
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
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public static void main(String[] args) throws Exception
	{
		logger.log(Level.INFO, "\n\n\n\n\n");
		CBSSIntegration integ = new CBSSIntegration(CBSSIConstants.cbKey, CBSSIConstants.cbName,
				CBSSIConstants.ssKey);
		integ.initSync();
		logger.log(Level.INFO, "Initial Fetch Finished");
		Thread.sleep(300000);
		integ.sync();
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
		this.ssCurrCode = getSSCurrencyCode();
		logger.log(Level.INFO, "\n\tbase currency:" + ssCurrCode);

		Client client = ClientBuilder.newClient();
		Response response = client.target("https://api.fixer.io/latest?base=" + ssCurrCode)
				.request(MediaType.TEXT_PLAIN_TYPE).get();
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			currencies = new JSONObject(response.readEntity(String.class)).getJSONObject("rates");
			logger.log(Level.INFO, "\n\tcurrencies:" + currencies.toString());

			if (currencies.length() == 0)
			{
				throw new RuntimeException("No Default Currency Values Found");
			}
		}
		else
		{
			throw new RuntimeException(
					response.getStatus() + ": " + response.getStatusInfo().getReasonPhrase());
		}
		thirdPartyMapping = getThirdPartyMapping();
	}

	private String getSSCurrencyCode() throws Exception
	{

		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSIConstants.ssWareHousesUrl)
				.request(MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey)
				// .header("Authorization", "Basic " +
				// Base64.getEncoder().encodeToString(("bc9e26b4606546b3a49da7e52547e542:2e96d8639728454dbda1f74971dc0d4e").getBytes()))
				.get();
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			JSONArray warhouses = new JSONArray(response.readEntity(String.class));
			if (warhouses.length() != 0)
			{
				return CBSSIConstants.SS_CURR_CODE.get(warhouses.getJSONObject(0)
						.getJSONObject("originAddress").getString("country"));
			}
			throw new RuntimeException(
					"ShipStation Native Country Not Defined, No Warhouses has been Configuered yet");
		}
		else
		{
			throw new RuntimeException(
					response.getStatus() + ": " + response.getStatusInfo().getReasonPhrase());
		}

	}

	public void initSync() throws Exception
	{
		logger.log(Level.INFO, "\n\tStarting Initial Sync.");

		processSync(CBSSIConstants.INIT_FETCH);
	}

	private void processSync(int mode) throws Exception
	{
		JSONObject ssOrder;
		JSONObject orderVsInvoice = new JSONObject();
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
		int count = 0;
		Environment.configure(cbSite, cbApiKey);

		if (isFailedInvProcess(mode))
		{
			JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
			logger.log(Level.INFO,
					"\n\tMethod: FailedInvProcess : FailedInvoices :: " + failedInvoices);
			if (failedInvoices.length() == 0) // If no failed invoices.
			{
				return;
			}
			invoicesAsCSV = getInvIdAsCSV(failedInvoices.names());
		}
		do
		{
			try
			{
				InvoiceListRequest req = Invoice.list().limit(100).includeDeleted(false)
						.offset(nextOffSet).status().is(Invoice.Status.PAID);
				if (invoicesAsCSV != null) // previous failed invoice handling
				{
					logger.log(Level.INFO, "\n\tinvoicesAsCSV" + invoicesAsCSV);

					req.id().in(invoicesAsCSV);
				}
				if (isSync(mode)) // sync handling
				{
					logger.log(Level.INFO, "\n\tObtaining Invoices from the account!");

					req.updatedAt().after(new Timestamp(getCBLastSyncTime()));
				}
				result = req.request();
				nextOffSet = result.nextOffset();
				logger.log(Level.INFO, "\n\tnextOffSet : " + nextOffSet);
				for (int j = 0; j < result.size(); j++)
				{
					entry = result.get(j);
					invoice = entry.invoice();
					if (checkFailedInvoice(invoice.id()) && !isFailedInvProcess(mode))
					{
						continue;
					}
					Subscription sub = Subscription.retrieve(invoice.subscriptionId()).request()
							.subscription();
					billingPeriod = sub.billingPeriod(); // handling of recurring Invoices
					logger.log(Level.INFO, "\n\t invoice : " + invoice.id() + " billingPeriod : " + billingPeriod);
					for (int i = 0; i < billingPeriod; i++)
					{
						orderDate = getOrderDate(i, sub.billingPeriodUnit(), invoice);
						isInvFailed = false;
						ssOrder = new JSONObject();
						fillCustomerInfo(invoice, ssOrder);
						isInvFailed = fillBillingAddress(invoice, ssOrder) ? false : true; 
						// method returns true if all fields are available
						if (isInvFailed)
						{
							logger.log(Level.INFO, "\n\tfailed invoice : " + invoice.id()
									+ "is failed due to invalid billing address");

							i = billingPeriod;
							insertFailedInvoice(invoice.id(), "Invalid Billing Address");
							continue;
						}
						isInvFailed = fillShippingAddress(invoice, ssOrder) ? false : true; 
						// method returns true if all fields are available

						if (isInvFailed)
						{
							logger.log(Level.INFO, "\n\tInvoice " + invoice.id()
									+ "is failed due to invalid shipping address");

							i = billingPeriod;
							insertFailedInvoice(invoice.id(), "Invalid Shipping Address");
							continue;
						}
						fillOrders(i, invoice, ssOrder, orderDate, orderVsInvoice, mode,
								billingPeriod);
						ssAllOrders.put(ssOrder);
						if (count == 99)
						{
//							logger.log(Level.INFO, "ssAllOrders : " + ssAllOrders.toString());
							response = createShipStationOrders(ssAllOrders);
							JSONObject respJSON = handleResponse(response, ssAllOrders,
									orderVsInvoice);
							if (response.getStatus() == HttpStatus.SC_OK
									|| response.getStatus() == HttpStatus.SC_ACCEPTED)
							{

								if(nextOffSet == null)
								{
									if(isFailedInvProcess(mode))
									{
										updThirdPartyMapping("Third Party Mapping");
										return;
									}
									updCBLastSyncTime(lastSyncTime);
								}
								createCBOrders(respJSON, mode);
								updSSLastSyncTime(lastSyncTime);
							}
							ssAllOrders = new JSONArray();
							logger.log(Level.INFO, "count : " + count);
							count = 0;
						}
					}
					count++;
				}
//				logger.log(Level.INFO, "ssAllOrders : " + ssAllOrders.toString());
				if (count > 0)
				{
					response = createShipStationOrders(ssAllOrders);
					JSONObject respJSON = handleResponse(response, ssAllOrders, orderVsInvoice);
					if (response.getStatus() == HttpStatus.SC_OK
							|| response.getStatus() == HttpStatus.SC_ACCEPTED)
					{
						if(isFailedInvProcess(mode))
						{
							updThirdPartyMapping("Third Party Mapping");
							return;
						}
						updCBLastSyncTime(lastSyncTime);
						createCBOrders(respJSON, mode);
						updSSLastSyncTime(lastSyncTime);
					}
					ssAllOrders = new JSONArray();
					logger.log(Level.INFO, "count : " + count);
					count = 0;
				}
			}
			catch (InvalidRequestException e)
			{
				if (e.apiErrorCode.equals("site_not_found"))
				{
					logger.log(Level.INFO, "\n\tChargeBee Site Not found");

				}
				else if (e.apiErrorCode.equals("api_authentication_failed"))
				{
					logger.log(Level.INFO, "\n\tChargeBee Key is Invalid");

				}
				logger.log(Level.INFO, "APIException : " + e.getStackTrace().toString());
				throw e;

			}
			catch (APIException e)
			{
				if(e.apiErrorCode.equals("api_authorization_failed"))
				{
					logger.log(Level.INFO, "ChargeBee Key has no valid permission");
				}
				logger.log(Level.INFO, "APIException : " + e.getStackTrace().toString());
				throw e;
			}
			catch(Exception e)
			{
				logger.log(Level.INFO, "APIException : " + e.getStackTrace().toString());
				throw e;
			}
		}
		while (nextOffSet != null);
		updThirdPartyMapping("Third Party Mapping");
		logger.log(Level.INFO, "\n\tSync operation finished!");

	}

	private void updThirdPartyMapping(String action) throws Exception
	{
		logger.log(Level.INFO, "\n\t" + action +" : " + thirdPartyMapping.toString());
	}

	private boolean checkFailedInvoice(String invoiceId) throws Exception
	{
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		if(failedInvoices.has(invoiceId))
		{
			return true;
		}
		return false;
	}

	private Date getOrderDate(int index, BillingPeriodUnit billingPeriodUnit, Invoice invoice)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tindex : " + String.valueOf(index));

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
		return cal.getTime();
	}

	private JSONObject handleResponse(Response response, JSONArray ssAllOrders,
			JSONObject orderVsInvoice) throws Exception
	{
		JSONObject orders = null;
		int status = response.getStatus();
		if (status == HttpStatus.SC_OK || status == HttpStatus.SC_ACCEPTED)
		{
			orders = handleSuccessResponse(response, orderVsInvoice);
		}
		else
		{
			handleErrorResponse(response, ssAllOrders, orderVsInvoice);
		}
		return orders;
	}

	private void updOrdInThirdPartyMapping(JSONObject order, JSONObject orderVsInvoice)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updOrdInThirdPartyMapping");

		JSONObject cbInvIdVsSSOrdNo = thirdPartyMapping
				.getJSONObject(CBSSIConstants.CBInvIdVSSOrdNo);
		JSONObject ssOrdNoVsSSOrdKey = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdNoVsSSOrderKey);
		JSONObject ssOrdKeyVsCBInvId = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdKeyVsCBInvId);
		String orderNo = order.getString(CBSSIConstants.orderNumber);
		String orderKey = order.getString(CBSSIConstants.orderKey);
		String invoiceId = orderVsInvoice.getString(orderNo);
		
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		if (!order.getBoolean("success"))
		{
			insertFailedInvoice(invoiceId, order.getString("errorMessage"));
		}
		else
		{
			JSONArray orders;
			if (cbInvIdVsSSOrdNo.has(invoiceId))
			{
				orders = cbInvIdVsSSOrdNo.getJSONArray(invoiceId);
				for(int i = 0; i < orders.length(); i++)
				{
					if(!orders.getString(i).equals(orderNo))
					{
						orders.put(orderNo);
					}
				}
			}
			else
			{
				orders = new JSONArray();
				orders.put(orderNo);
				cbInvIdVsSSOrdNo.put(invoiceId, orders);
			}
			if (!ssOrdNoVsSSOrdKey.has(orderNo))
			{
				ssOrdNoVsSSOrdKey.put(orderNo, orderKey);
				ssOrdKeyVsCBInvId.put(orderKey, invoiceId);
			}
			thirdPartyMapping.put(CBSSIConstants.CBInvIdVSSOrdNo, cbInvIdVsSSOrdNo);
			thirdPartyMapping.put(CBSSIConstants.SSOrdNoVsSSOrderKey, ssOrdNoVsSSOrdKey);
			thirdPartyMapping.put(CBSSIConstants.SSOrdKeyVsCBInvId, ssOrdKeyVsCBInvId);
			logger.log(Level.INFO, "\n\tCBInvIdVSSOrdNo updated : " + invoiceId + " - " + orderNo);
			logger.log(Level.INFO, "\n\tSSOrdNoVsSSOrderKey updated : " + orderNo + " - " + orderKey);
			logger.log(Level.INFO, "\n\tSSOrdKeyVsCBInvId updated : " + orderKey + " - " + invoiceId);
			removeFailedInvoice(invoiceId);
		}
	}

	private JSONObject handleSuccessResponse(Response response, JSONObject orderVsInvoice)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: handleSuccessResponse");

		JSONObject orders = new JSONObject(response.readEntity(String.class));
		JSONArray results = orders.getJSONArray("results");
		JSONObject order;
		for (int i = 0; i < results.length(); i++)
		{
			order = results.getJSONObject(i);
			updOrdInThirdPartyMapping(order, orderVsInvoice);
			//logger.log(Level.INFO, "\n\torder : " + order.toString());
		}
		return orders;
	}

	private void handleErrorResponse(Response response, JSONArray ssAllOrders,
			JSONObject orderVsInvoice) throws Exception
	{
		int status = response.getStatus();
		String messege = response.getStatusInfo().getReasonPhrase();
		int failedInvoiceCount = 0;
		JSONObject order;
		String invoiceId;
		String orderNo;
		if ((status == 429))
		{
			logger.log(Level.INFO, "\n\t" + status + " : " + messege + "( Rate Limit Exceeded)");
			Thread.sleep(60000);
			createShipStationOrders(ssAllOrders);
		}
		else if ((status == HttpStatus.SC_UNAUTHORIZED) || (status == HttpStatus.SC_BAD_REQUEST) || (status == HttpStatus.SC_FORBIDDEN)
				|| (status == HttpStatus.SC_NOT_FOUND)
				|| status == HttpStatus.SC_INTERNAL_SERVER_ERROR)
		{
			while (failedInvoiceCount < ssAllOrders.length())
			{
				order = ssAllOrders.getJSONObject(failedInvoiceCount);
				orderNo = order.getString(CBSSIConstants.orderNumber);
				invoiceId = orderVsInvoice.getString(orderNo);
				insertFailedInvoice(invoiceId, "Internal Error");
				//logger.log(Level.INFO, "order : " + order.toString());
				failedInvoiceCount++;
			}
			logger.log(Level.INFO, "\n\t" + status + " : " + messege + "(Wrong user credentials, NO user permissions to this api, Invalid api resource, Internal Error)");
			throw new RuntimeException(messege);
		}
	}

	private void insertFailedInvoice(String invoiceId, String reason)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updateFailedInvoice");
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		failedInvoices.put(invoiceId, reason);
		thirdPartyMapping.put(CBSSIConstants.FailedInvDets, failedInvoices);
		return;
		//updThirdPartyMapping("failed invoice inserted");
	}

	private void removeFailedInvoice(String invoiceId)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updateFailedInvoice");
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		if(failedInvoices.has(invoiceId))
		{
			failedInvoices.remove(invoiceId);
			thirdPartyMapping.put(CBSSIConstants.FailedInvDets, failedInvoices);
		}
		return;
		//updThirdPartyMapping("failed invoice removed");
	}

	private void updSSLastSyncTime(long time) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updLastSyncTime");
		thirdPartyMapping.put("ss-last-sync-time", time);
		//updThirdPartyMapping("SS last sync time updated : " + getSSTimeZone(new Date(time)));
	}

	private void updCBLastSyncTime(long time) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updLastSyncTime");
		thirdPartyMapping.put("cb-last-sync-time", time);
		//updThirdPartyMapping("CB last sync time updated : " + getSSTimeZone(new Date(time)));
	}

	public void sync() throws Exception
	{
		handlePreviousFailedInvoice();
		processSync(CBSSIConstants.SYNC);
	}

	private void handlePreviousFailedInvoice() throws Exception
	{
		processSync(CBSSIConstants.FAILED_INV_PROCESS);
	}

	private boolean fillBillingAddress(Invoice invoice, JSONObject ssOrder) throws Exception
	{
		logger.log(Level.INFO, "\n\tfilling the Billing address for invoice :: " + invoice.id());
		Result result = Customer.retrieve(invoice.customerId()).request();
		Customer customer = result.customer();
		if (invoice.billingAddress() == null && customer.billingAddress() == null)
		{
			logger.log(Level.INFO, "\n\tbilling address is empty for invoice ::  " + invoice.id());
			return false;
		}
		JSONObject billTo = new JSONObject();
		BillingAddress address = invoice.billingAddress();
		boolean validAdd = fillBillingAddress(address, customer, billTo, true, invoice);
		if (!validAdd)
		{
			logger.log(Level.INFO,
					"\n\tbilling address is not a valid for invoice :: " + invoice.id());
			return false;
		}
		ssOrder.put("billTo", billTo);
		return true;
	}

	private boolean fillBillingAddress(BillingAddress address, Customer customer, JSONObject object,
			boolean isBilling, Invoice invoice) throws Exception
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

	private boolean fillShippingAddress(ShippingAddress address, Customer customer,
			JSONObject object, Invoice invoice) throws Exception
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
		if (invoice.shippingAddress() == null)
		{
			return false;
		}
		JSONObject shipTo = new JSONObject();
		ShippingAddress address = invoice.shippingAddress();
		boolean validAdd = fillShippingAddress(address, customer, shipTo, invoice); // checking
																					// valid address
		if (!validAdd)
		{
			return false;
		}
		if (address.validationStatus() != null)
			shipTo.put("addressVerified", address.validationStatus().toString());
		ssOrder.put("shipTo", shipTo);
		return true;
	}

	private void fillCustomerInfo(Invoice invoice, JSONObject order)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tfilling Customer Info of invoice-id: " + invoice.id());
		order.put("customerUsername", cbSite);
		order.put("customerEmail", getCBCustomerEmail(invoice.customerId()));
	}

	private String getCBCustomerEmail(String userId_cb) throws Exception
	{
		Result result = Customer.retrieve(userId_cb).request();
		return result.customer().email();
	}

	private String getSSOrderKey(String orderNo) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: getOrderNoForInvoice");
		JSONObject ordNoVsOrdkey = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdNoVsSSOrderKey);
		return 	ordNoVsOrdkey.getString(orderNo);
	}

	private void fillOrders(int i, Invoice invoice, JSONObject ssOrder, Date orderDate,
			JSONObject orderVsInvoice, int mode, int billingPeriod) throws Exception
	{
		logger.log(Level.INFO, "\n\tOrderDate : " + getSSTimeZone(orderDate));
		JSONObject cbInvIdVsSSOrdNo = thirdPartyMapping.getJSONObject(CBSSIConstants.CBInvIdVSSOrdNo);
		JSONObject product;
		double disc = 0;
		String orderKey;
		String invoiceId = invoice.id();
		String orderNo = invoiceId + "_" + UUID.randomUUID();
		JSONArray lineItems = new JSONArray();
		String cbCurrCode = invoice.currencyCode();

		if (isSync(mode) && cbInvIdVsSSOrdNo.has(invoiceId))
		{
			orderNo = (String) cbInvIdVsSSOrdNo.getJSONArray(invoiceId).get(i);
			orderKey = getSSOrderKey(orderNo);
			ssOrder.put("orderKey", orderKey);
		}
		orderVsInvoice.put(orderNo, invoiceId);
		ssOrder.put("orderNumber", orderNo);
		ssOrder.put("orderDate", getSSTimeZone(orderDate));
		ssOrder.put("paymentDate", getSSTimeZone(invoice.paidAt()));
		ssOrder.put("orderStatus", getOrderStatus(invoice.status().toString()));
		for(Discount discount : invoice.discounts())
		{
			disc+= getSSCurrency(discount.amount(), cbCurrCode);
		}
		ssOrder.put("amountPaid", (getSSCurrency(invoice.amountPaid(), cbCurrCode)+disc));
		logger.log(Level.INFO, "\n\tamountPaid after conversion : "
				+ getSSCurrency(invoice.amountPaid(), cbCurrCode));
		logger.log(Level.INFO, "\n\tdiscount : " + invoice.discounts());
		for (LineItem item : invoice.lineItems())
		{

			product = new JSONObject();
			product.put("adjustment", false);
			product.put("lineItemKey", item.id());
			product.put("sku", item.entityId());
			product.put("name", item.description());
			product.put("quantity", item.quantity());
			product.put("unitPrice", getSSCurrency(item.unitAmount(), cbCurrCode));
			product.put("taxAmount", getSSCurrency(item.taxAmount(), cbCurrCode));
			logger.log(Level.INFO, "\n\tunitPrice after conversion : "
					+ getSSCurrency(item.unitAmount(), cbCurrCode));
			logger.log(Level.INFO, "\n\titemTaxAmount after conversion : "
					+ getSSCurrency(item.taxAmount(), cbCurrCode));
			lineItems.put(product);
		}
		// for(Discount discount : invoice.discounts())
		// {
		// product = new JSONObject();
		// product.put("adjustment", true);
		// product.put("lineItemKey", discount.entityId());
		// product.put("sku", discount.entityType());
		// product.put("name", discount.description());
		// product.put("unitPrice", getSSCurrency(discount.amount(),
		// cbCurrCode)/billingPeriod);
		// product.put("quantity", 1);
		// lineItems.put(product);
		//
		// disc+= getSSCurrency(discount.amount(), cbCurrCode);
		// }
		// logger.log(Level.INFO ,"\n\tdiscount : " + invoice.discounts().toString());
		logger.log(Level.INFO, "\n\tdiscountAmount after conversion : " + disc);
		ssOrder.put("items", lineItems);

	}

	private double getSSCurrency(Integer amount, String cbCurrCode) throws Exception
	{
		if(cbCurrCode.equals(ssCurrCode))
		{
			return (double) amount / 100;
		}
		return ((double) amount / 100) / currencies.getDouble(cbCurrCode);

	}

	private Response createShipStationOrders(JSONArray shipStationJson)
	{
		logger.log(Level.INFO, "\n\tcreating shipstation orders");
		Client client = ClientBuilder.newClient();
		Entity<String> payload = Entity.json(shipStationJson.toString());
		return client.target(CBSSIConstants.createOrdersUrl).request(MediaType.APPLICATION_JSON_TYPE)
				.header("Authorization", ssApiKey).post(payload);
	}

	private void createCBOrders(JSONObject ordersFromCBInvoices, int mode)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tgetting shipstation orders");
		Client client = ClientBuilder.newClient();
		String orderNo;
		String orderKey;
		String invoiceId;
		String orderStatus = null;
		String customerNote = null;
		String trackingId = null;
		String batchId = null;
		String ssOrdId;
		StringBuilder url = new StringBuilder(CBSSIConstants.ordersUrl);
		JSONObject ssOrdNoVsSSOrdKey = thirdPartyMapping.getJSONObject(CBSSIConstants.SSOrdNoVsSSOrderKey);
		JSONObject ssOrdKeyVsCBInvId = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdKeyVsCBInvId);
		url.append("?orderStatus=awaiting_shipment");
		if(!isInitialFetch(mode))
		{
			Date fromDate = new Date(getSSLastSyncTime());
			url.append("&modifyDateStart=").append(getSSTimeZone(fromDate));
		}
		logger.log(Level.INFO, "\n\tlist orders in shipstation :: " + url.toString());
		Response response = client.target(url.toString()).request(MediaType.TEXT_PLAIN_TYPE)
				.header("Authorization", ssApiKey).get();
		int status = response.getStatus();
		String messege = response.getStatusInfo().getReasonPhrase();
		if (response.getStatus() == HttpStatus.SC_OK)
		{
			JSONObject ssOrders = new JSONObject(response.readEntity(String.class));
			JSONArray orders = ssOrders.getJSONArray("orders");
			logger.log(Level.INFO, "\n\tssOrders : " + orders.toString());
			JSONObject orderShipInfo;

			for (int i = 0; i < orders.length(); i++)
			{
				JSONObject order = orders.getJSONObject(i);
				orderNo = order.getString(CBSSIConstants.orderNumber);
				orderKey = ssOrdNoVsSSOrdKey.getString(orderNo);
				invoiceId = ssOrdKeyVsCBInvId.getString(orderKey);
				if (isOrderFromCB(order, ordersFromCBInvoices))
				{
					ssOrdId = order.getString("orderId");
					orderShipInfo = getShipInfo(orderNo);

					if (order.has("orderStatus"))
					{
						orderStatus = order.getString("orderStatus");
					}
					if (order.has("customerNote"))
					{
						customerNote = order.getString("customerNote");
					}
					if (order.has("trackingNumber"))
					{
						trackingId = orderShipInfo.getString("trackingNumber");
					}
					if (order.has("batchNumber"))
					{
						batchId = orderShipInfo.getString("batchNumber");
					}
					create(invoiceId, ssOrdId, orderStatus, batchId, trackingId, customerNote);
				}
			}
		}
		else if ((status == 429))
		{
			logger.log(Level.INFO, "\n\t" + status + " : " + messege + "( Rate Limit Exceeded)");
			Thread.sleep(60000);
			createCBOrders(ordersFromCBInvoices, mode);
		}
		else if ((status == HttpStatus.SC_UNAUTHORIZED) || (status == HttpStatus.SC_BAD_REQUEST) || (status == HttpStatus.SC_FORBIDDEN)
				|| (status == HttpStatus.SC_NOT_FOUND)
				|| status == HttpStatus.SC_INTERNAL_SERVER_ERROR)
		{
			logger.log(Level.INFO, "\n\t" + status + " : " + messege + "(Wrong user credentials, NO user permissions to this api, Invalid api resource, Internal Error)");
			throw new RuntimeException(messege);
		}
	}

	private boolean isOrderFromCB(JSONObject order, JSONObject ordersFromCBInvoices)
			throws JSONException, IOException
	{
		logger.log(Level.INFO, "\n\tSplitting Shipstation Orders");
		String orderId = order.getString("orderId");
		JSONArray ordersFromCB = ordersFromCBInvoices.getJSONArray("results");
		JSONObject ssOrdVsCBOrd = thirdPartyMapping.getJSONObject(CBSSIConstants.SSOrdVsCBOrd);
		JSONObject ordFromCB;
		for (int i = 0; i < ordersFromCB.length(); i++)
		{
			ordFromCB = ordersFromCB.getJSONObject(i);
			if (orderId.equals(ordFromCB.getString("orderId")))
			{
				return true;
			}
			else if(ssOrdVsCBOrd.has(orderId))
			{
				return true;
			}
		}
		return false;
	}

	private JSONObject getShipInfo(String orderNo) throws JSONException
	{
		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSIConstants.shipmentUrl + "?orderNumber=" + orderNo)
				.request(MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey).get();
		return new JSONObject(response.readEntity(String.class));
	}

	private void create(String invoiceId, String ssOrdId, String orderStatus, String batchId, String trackingId,
			String customerNote) throws Exception
	{
		logger.log(Level.INFO, "\n\tCreating ChargeBee orders");
		Order order;
		Result result;
		String cbOrdId = getCBOrdId(ssOrdId);
		if (cbOrdId == null) // new order
		{

			result = Order.create().id(ssOrdId).invoiceId(invoiceId)
					.fulfillmentStatus(orderStatus).referenceId(ssOrdId).batchId(batchId)
					.note(customerNote).request();
			order = result.order();
			updSSOrderVsCBOrder(ssOrdId, order.id());
			logger.log(Level.INFO, "\n\tCreated Chargebee Order : " + order);
		}
		else
		{
			logger.log(Level.INFO, "\n\tUpdating ChargeBee orders");
			result = Order.update(getCBOrdId(ssOrdId)).fulfillmentStatus(orderStatus)
					.referenceId(ssOrdId).batchId(batchId).note(customerNote).request();
			order = result.order();
			logger.log(Level.INFO, "\n\tUpdated Chargebee Order : " + order);
		}
	}

	private String getCBOrdId(String ssOrdId) throws Exception
	{
		logger.log(Level.INFO, "\n\tgetting the ChargeBee orderId for Update");
		JSONArray ssOrdVsCBOrd = thirdPartyMapping.getJSONArray(CBSSIConstants.SSOrdVsCBOrd);
		JSONObject obj;
		for (int i = 0; i < ssOrdVsCBOrd.length(); i++)
		{
			obj = ssOrdVsCBOrd.getJSONObject(i);
			if (obj.has(ssOrdId))
			{
				return obj.getString(ssOrdId);
			}
		}
		return null;
	}

	private void updSSOrderVsCBOrder(String ssOrderId, String cbOrderId)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : updCBOrderVsSSOrder");
		JSONObject ssOrdVsCBOrd = thirdPartyMapping.getJSONObject(CBSSIConstants.SSOrdVsCBOrd);
		JSONObject cbOrdVsSSOrd = thirdPartyMapping.getJSONObject(CBSSIConstants.CBOrdVsSSOrd);
		if (ssOrdVsCBOrd.has(ssOrderId))
		{
			return;
		}
		ssOrdVsCBOrd.put(ssOrderId, cbOrderId);
		thirdPartyMapping.put(CBSSIConstants.SSOrdVsCBOrd, ssOrdVsCBOrd);
		cbOrdVsSSOrd.put(cbOrderId, ssOrderId);
		thirdPartyMapping.put(CBSSIConstants.CBOrdVsSSOrd, cbOrdVsSSOrd);
		//updThirdPartyMapping("SSOrdVsCBOrd updated");
	}

	public String getTime()
	{
		long date = (new Date().getTime()) / 1000L;
		return String.valueOf(date);
	}

	public Long getCBLastSyncTime() throws JSONException
	{
		if (!thirdPartyMapping.has("cb-last-sync-time"))
		{
			return null;
		}
		return thirdPartyMapping.getLong("cb-last-sync-time");
	}

	public Long getSSLastSyncTime() throws JSONException
	{
		if (!thirdPartyMapping.has("ss-last-sync-time"))
		{
			return null;
		}
		return thirdPartyMapping.getLong("ss-last-sync-time");
	}

	private String getInvIdAsCSV(JSONArray failedInvoices) throws JSONException
	{
		StringBuilder builder = new StringBuilder();
		int length = failedInvoices.length();
		for (int i = 0; i < length; i++)
		{
			builder.append(failedInvoices.get(i));
			if(i != (length-1))
			{
				builder.append(",");
			}
		}
		return builder.toString();
	}

	private JSONObject getThirdPartyMapping() throws JSONException
	{
		JSONObject obj = null;

		if (obj == null)
		{
			obj = new JSONObject();
			obj.put(CBSSIConstants.FailedInvDets, new JSONObject());
			obj.put(CBSSIConstants.SSOrdVsCBOrd, new JSONObject());
			obj.put(CBSSIConstants.CBOrdVsSSOrd, new JSONObject());
			obj.put(CBSSIConstants.SSOrdNoVsSSOrderKey, new JSONObject());
			obj.put(CBSSIConstants.SSOrdKeyVsCBInvId, new JSONObject());
			obj.put(CBSSIConstants.CBInvIdVSSOrdNo, new JSONObject());
		}
		return obj;
	}

	private boolean isInitialFetch(int mode)
	{
		return mode == CBSSIConstants.INIT_FETCH;
	}

	private boolean isSync(int mode)
	{
		return mode == CBSSIConstants.SYNC;
	}

	private boolean isFailedInvProcess(int mode)
	{
		return mode == CBSSIConstants.FAILED_INV_PROCESS;
	}

	private String getOrderStatus(String invoiceStatus) throws IOException
	{
		logger.log(Level.INFO, "\n\tinvoiceStatus" + invoiceStatus);
		
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

}
