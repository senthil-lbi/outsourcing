package com.cb.ss;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import com.chargebee.models.Customer.CustomerListRequest;
import com.chargebee.models.Invoice;
import com.chargebee.models.Order;
import com.chargebee.models.Order.Status;
import com.chargebee.models.Subscription;
import com.chargebee.models.Subscription.SubscriptionListRequest;
import com.chargebee.models.Invoice.BillingAddress;
import com.chargebee.models.Invoice.Discount;
import com.chargebee.models.Invoice.InvoiceListRequest;
import com.chargebee.models.Invoice.IssuedCreditNote;
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
		logger.log(Level.INFO, "\n\tMethod : Main");
		CBSSIntegration integ = new CBSSIntegration(CBSSIConstants.cbKey, CBSSIConstants.cbName,
				CBSSIConstants.ssKey);
		integ.initSync();
		logger.log(Level.INFO, "\n\tInitial Fetch Finished");
		Thread.sleep(240000);
		integ.sync();
	}

	private String getCBTimeZone(Date date)
	{
		logger.log(Level.INFO, "\n\tMethod : getCBTimeZone");
		formatter.setTimeZone(TimeZone.getTimeZone("Europe/London"));
		return formatter.format(date);
	}

	private String getSSTimeZone(Date date)
	{
		logger.log(Level.INFO, "\n\tMethod : getSSTimeZone");
		formatter.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		return formatter.format(date);
	}

	public CBSSIntegration(String cbApiKey, String cbSite, String ssApiKey) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : CBSSIntegration Constructor");
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
		logger.log(Level.INFO, "\n\tMethod : getSSCurrencyCode");
		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSIConstants.ssWareHousesUrl).request(MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey).get();
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
		logger.log(Level.INFO, "\n\tMethod : initSync");
		logger.log(Level.INFO, "\n\tStarting Initial Sync.");
		processSync(CBSSIConstants.INIT_FETCH);
	}

	private void processSync(int mode) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : processSync");
		int count = 0;
		Response response;
		JSONObject ssOrder;
		int billingPeriod;
		Date orderDate;
		Invoice invoice;
		String invoiceId;
		String subscrpId;
		String customerId;
		InvoiceListRequest request;
		Environment.configure(cbSite, cbApiKey);
		JSONArray ssAllOrders = new JSONArray();
		JSONObject orderVsInvoice = new JSONObject();
		long lastSyncTime = System.currentTimeMillis();
		ArrayList<Invoice> invoices = new ArrayList<Invoice>();
		HashMap<String, Customer> customers = new HashMap<String, Customer>();
		HashMap<String, Subscription> subscriptions = new HashMap<String, Subscription>();

		try
		{
			if(isInitialFetch(mode))
			{
				logger.log(Level.INFO, "\n\tInitial Fetch");
				request = Invoice.list().limit(100).includeDeleted(false).status().is(Invoice.Status.PAID);
				invoices = getInvoices(request, customers, subscriptions);
			}
			else if (isSync(mode))
			{
				logger.log(Level.INFO, "\n\tSynchronization Process");
				request = Invoice.list().limit(100).includeDeleted(false).status().is(Invoice.Status.PAID).updatedAt().after(new Timestamp(getCBLastSyncTime()));
				invoices = getInvoices(request, customers, subscriptions);
				updateInvoices(invoices, customers, subscriptions);
			}
			else if (isFailedInvProcess(mode))
			{
				logger.log(Level.INFO, "\n\tFailed Invoice Process");
				JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
				if (failedInvoices.length() == 0) // If no failed invoices.
				{
					return;
				}
				request = Invoice.list().limit(100).includeDeleted(false).status().is(Invoice.Status.PAID).id().in(listAsArray(failedInvoices.names()));
				invoices = getInvoices(request, customers, subscriptions);
			}
			for (int i = 0; i < invoices.size(); i++)
			{
				invoice = invoices.get(i);
				invoiceId = invoice.id();
				subscrpId = invoice.subscriptionId();
				customerId = invoice.customerId();
				updCBInvIdVsCBCusId(invoiceId, customerId);
				updCBInvIdVsCBSubId(invoiceId, subscrpId);
				if (checkFailedInvoice(invoiceId) && !isFailedInvProcess(mode))
				{
					continue;
				}
				Customer customer = customers.get(customerId);
				Subscription subscription = subscriptions.get(subscrpId);
				billingPeriod = subscription.billingPeriod(); // handling of recurring Invoices
				logger.log(Level.INFO,
						"\n\t invoice : " + invoiceId + " billingPeriod : " + billingPeriod);
				for (int j = 0; j < billingPeriod; j++)
				{
					orderDate = getOrderDate(j, subscription.billingPeriodUnit(), invoice);
					ssOrder = new JSONObject();
					fillCustomerInfo(invoice, customer, ssOrder);
					
					if (!hasValidBillingAddress(invoice, customer, ssOrder, mode))
					{
						logger.log(Level.INFO, "\n\tfailed invoice : " + invoiceId
								+ "is failed due to invalid billing address");

						j = billingPeriod;
						updateFailedInvoice(invoiceId, "Invalid Billing Address");
						continue;
					}

					if (!hasValidShippingAddress(invoice, subscription, ssOrder, mode))
					{
						logger.log(Level.INFO, "\n\tInvoice " + invoiceId
								+ "is failed due to invalid shipping address");

						j = billingPeriod;
						updateFailedInvoice(invoiceId, "Invalid Shipping Address");
						continue;
					}
					fillOrders(j, invoice, ssOrder, orderDate, orderVsInvoice, mode, billingPeriod);
					ssAllOrders.put(ssOrder);
					if (count == 99)
					{
						// logger.log(Level.INFO, "ssAllOrders : " + ssAllOrders.toString());
						response = createShipStationOrders(ssAllOrders);
						JSONObject respJSON = handleResponse(response, ssAllOrders,
								orderVsInvoice);
						if (response.getStatus() == HttpStatus.SC_OK
								|| response.getStatus() == HttpStatus.SC_ACCEPTED)
						{

							if (i == invoices.size()-1)
							{
								if (isFailedInvProcess(mode))
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
			// logger.log(Level.INFO, "ssAllOrders : " + ssAllOrders.toString());
			if (count > 0)
			{
				response = createShipStationOrders(ssAllOrders);
				JSONObject respJSON = handleResponse(response, ssAllOrders, orderVsInvoice);
				if (response.getStatus() == HttpStatus.SC_OK
						|| response.getStatus() == HttpStatus.SC_ACCEPTED)
				{
					if (isFailedInvProcess(mode))
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
		} catch (InvalidRequestException e)
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

		} catch (APIException e)
		{
			if (e.apiErrorCode.equals("api_authorization_failed"))
			{
				logger.log(Level.INFO, "ChargeBee Key has no valid permission");
			}
			logger.log(Level.INFO, "APIException : " + e.getStackTrace().toString());
			throw e;
		} catch (Exception e)
		{
			logger.log(Level.INFO, "APIException : " + e.getStackTrace().toString());
			throw e;
		}
	updThirdPartyMapping("Third Party Mapping");
	}

	private ArrayList<Invoice> getInvoices(InvoiceListRequest request, HashMap<String, Customer> customers, HashMap<String, Subscription> subscriptions) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : getInvoices");
		int length;
		Invoice invoice;
		ListResult result;
		String nextOffset = null;
		String customer;
		String subscription;
		ArrayList<Invoice> invoiceList = new ArrayList<Invoice>();
		JSONObject cusList = new JSONObject();
		JSONObject subList = new JSONObject();
		do
		{
			InvoiceListRequest invoices = request;
			result = invoices.offset(nextOffset).request();
			nextOffset = result.nextOffset();
			length = result.size();
			logger.log(Level.INFO, "\n\tresult size : " + length);
			for (int i = 0; i < length; i++)
			{
				invoice = result.get(i).invoice();
				invoiceList.add(invoice);
				customer = invoice.customerId();
				if(!cusList.has(customer))
				{
					cusList.put(customer, i);
				}
				subscription = invoice.subscriptionId();
				if(!subList.has(subscription))
				{
					subList.put(subscription, i);
				}
			}
		}
		while(nextOffset != null);
		if(customers != null)
		{
			CustomerListRequest  cutomerRequest = Customer.list().limit(100).id().in(listAsArray(cusList.names()));
			customers.putAll(getCustomers(cutomerRequest));
		}
		if(subscriptions != null)
		{
			SubscriptionListRequest subscriptionRequest = Subscription.list().limit(100).id().in(listAsArray(subList.names()));
			subscriptions.putAll(getSubscriptions(subscriptionRequest));
		}
		
		return invoiceList;
	}

	private HashMap<String, Customer> getCustomers(CustomerListRequest request) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : getCustomers");
		Customer customer;
		ListResult result;
		String nextOffset = null;
		HashMap<String, Customer> updCustomers = new HashMap<String, Customer>();
		do
		{
			CustomerListRequest customers = request;
			result = customers.offset(nextOffset).request();
			nextOffset = result.nextOffset();
			logger.log(Level.INFO, "\n\tresult size : " + result.size());
			for (int i = 0; i < result.size(); i++)
			{
				customer = result.get(i).customer();
				updCustomers.put(customer.id(), customer);
			}
		}
		while(nextOffset != null);
		return updCustomers;
	}

	private HashMap<String, Subscription> getSubscriptions(SubscriptionListRequest request) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : getSubscriptions");
		ListResult result;
		String nextOffset = null;
		Subscription subscription;
		HashMap<String, Subscription> updSubscriptions = new HashMap<String, Subscription>();
		do
		{
			SubscriptionListRequest subscriptions = request;
			result = subscriptions.offset(nextOffset).request();
			nextOffset = result.nextOffset();
			logger.log(Level.INFO, "\n\tresult size : " + result.size());
			for (int i = 0; i < result.size(); i++)
			{
				subscription = result.get(i).subscription();
				updSubscriptions.put(subscription.id(), subscription);
			}
		}
		while(nextOffset != null);
		return updSubscriptions;
	}

	private void updateInvoices(ArrayList<Invoice> invoices, HashMap<String, Customer> customers,
			HashMap<String, Subscription> subscriptions) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : updateInvoices");
		Invoice invoice;
		CustomerListRequest customerRequest = Customer.list().limit(100).updatedAt().after(new Timestamp(getCBLastSyncTime()));
		SubscriptionListRequest subscriptionReqest = Subscription.list().limit(100).updatedAt().after(new Timestamp(getCBLastSyncTime()));
		HashMap<String, Customer> updCustomers = getCustomers(customerRequest);
		HashMap<String, Subscription> updSubscriptions = getSubscriptions(subscriptionReqest);
		ArrayList<Invoice> updCusInvoices = new ArrayList<Invoice>();
		ArrayList<Invoice> updSubInvoices = new ArrayList<Invoice>();
		
		//getting invoice list of updated customers and subscriptions
		if(updCustomers.size() != 0)
		{
			InvoiceListRequest invoiceRequest = Invoice.list().limit(100).includeDeleted(false).status().is(Invoice.Status.PAID).customerId().in(listAsArray(updCustomers.keySet()));
			updCusInvoices = getInvoices(invoiceRequest, null, null);
		}
		if(updSubscriptions.size() != 0)
		{
			InvoiceListRequest invoiceRequest = Invoice.list().limit(100).includeDeleted(false).status().is(Invoice.Status.PAID).subscriptionId().in(listAsArray(updSubscriptions.keySet()));
			updSubInvoices = getInvoices(invoiceRequest, null, null);
		}
		//removing already existing invoices and invoices need not to be updated
		for(int i = 0; i < updCusInvoices.size(); i++)
		{
			invoice = updCusInvoices.get(i);
			BillingAddress invBillAdd = invoice.billingAddress();
			Customer.BillingAddress cusBillAdd = updCustomers.get(invoice.customerId()).billingAddress();
			if(invBillAdd == null && cusBillAdd == null)
			{
				updCusInvoices.remove(invoice);
			}
			else if (invBillAdd != null && cusBillAdd != null && isSameAddress(invBillAdd, cusBillAdd))
			{
				updCusInvoices.remove(invoice);
			}
			if(invoices.contains(invoice));
			{
				updCusInvoices.remove(invoice);
			}
		}
		//removing already existing invoices and invoices need not to be updated
		for(int i = 0; i < updSubInvoices.size(); i++)
		{
			invoice = updSubInvoices.get(i);
			ShippingAddress invShippAdd = invoice.shippingAddress();
			Subscription.ShippingAddress subShippAdd = updSubscriptions.get(invoice.subscriptionId()).shippingAddress();
			if(invShippAdd == null && subShippAdd == null)
			{
				updSubInvoices.remove(invoice);
			}
			else if (invShippAdd != null && subShippAdd != null && isSameAddress(invShippAdd, subShippAdd))
			{
				updSubInvoices.remove(invoice);
			}
			else if(invoices.contains(invoice));
			{
				updSubInvoices.remove(invoice);
			}
		}
		//removing already existing invoices
		for(int i = 0; i < updSubInvoices.size(); i++)
		{
			invoice = updSubInvoices.get(i);
			if(updCusInvoices.contains(invoice));
			{
				updSubInvoices.remove(invoice);
			}
		}
		customers.putAll(updCustomers);
		subscriptions.putAll(updSubscriptions);
		invoices.addAll(updCusInvoices);
		invoices.addAll(updSubInvoices);
	}

	private String[] listAsArray(Set<String> keys)
	{
		logger.log(Level.INFO, "\n\tMethod : listAsArray");
		int length = keys.size();
		String[] array = new String[length];
		Iterator<String> it = keys.iterator();
		for(int i = 0; i < length; i++)
		{
			array[i] = it.next();
		}
		return array;
	}
	
	private void updCBInvIdVsCBCusId(String invoiceId, String customerId) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : updCBInvIdVsCBCusId");
		JSONObject invIdVsCusId = thirdPartyMapping.getJSONObject(CBSSIConstants.CBInvIdVsCBCusId);
		JSONObject cusIdVsInvId = thirdPartyMapping.getJSONObject(CBSSIConstants.CBCusIdVsCBInvId);
		if (!invIdVsCusId.has(invoiceId))
		{
			invIdVsCusId.put(invoiceId, customerId);
		}
		
		if(cusIdVsInvId.has(customerId))
		{
			JSONArray invIds = cusIdVsInvId.getJSONArray(customerId);
			for(int i = 0; i < invIds.length(); i++)
			{
				if(invIds.getString(i).equals(invoiceId))
				{
					break;
				}
				if(i == invIds.length()-1)
				{
					invIds.put(invoiceId);
				}

			}
			cusIdVsInvId.put(customerId, invIds);
		}
		else
		{
			JSONArray invIds = new JSONArray();
			invIds.put(invoiceId);
			cusIdVsInvId.put(customerId, invIds);
		}
		thirdPartyMapping.put(CBSSIConstants.CBInvIdVsCBSubId, invIdVsCusId);
		thirdPartyMapping.put(CBSSIConstants.CBCusIdVsCBInvId, cusIdVsInvId);
	}

	private void updCBInvIdVsCBSubId(String invoiceId, String subscrpId) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : updCBInvIdVsCBSubId");
		JSONObject invIdVsSubId = thirdPartyMapping.getJSONObject(CBSSIConstants.CBInvIdVsCBSubId);
		JSONObject subIdVsInvId = thirdPartyMapping.getJSONObject(CBSSIConstants.CBSubIdVsCBInvId);
		if (!invIdVsSubId.has(invoiceId))
		{
			invIdVsSubId.put(invoiceId, subscrpId);
		}
		
		if(subIdVsInvId.has(subscrpId))
		{
			JSONArray invIds = subIdVsInvId.getJSONArray(subscrpId);
			for(int i = 0; i < invIds.length(); i++)
			{
				if(invIds.getString(i).equals(invoiceId))
				{
					break;
				}
				if(i == invIds.length()-1)
				{
					invIds.put(invoiceId);
				}

			}
			subIdVsInvId.put(subscrpId, invIds);
		}
		else
		{
			JSONArray invIds = new JSONArray();
			invIds.put(invoiceId);
			subIdVsInvId.put(subscrpId, invIds);
		}
		thirdPartyMapping.put(CBSSIConstants.CBInvIdVsCBSubId, invIdVsSubId);
		thirdPartyMapping.put(CBSSIConstants.CBSubIdVsCBInvId, subIdVsInvId);
	}

	private void updThirdPartyMapping(String action) throws Exception
	{
		logger.log(Level.INFO, "\n\t" + action + " : " + thirdPartyMapping.toString());
	}

	private boolean checkFailedInvoice(String invoiceId) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : checkFailedInvoice");
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		if (failedInvoices.has(invoiceId))
		{
			return true;
		}
		return false;
	}

	private Date getOrderDate(int index, BillingPeriodUnit billingPeriodUnit, Invoice invoice)
			throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod : getOrderDate");

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

		if (!order.getBoolean("success"))
		{
			updateFailedInvoice(invoiceId, order.getString("errorMessage"));
		}
		else
		{
			JSONArray orders;
			if (cbInvIdVsSSOrdNo.has(invoiceId))
			{
				orders = cbInvIdVsSSOrdNo.getJSONArray(invoiceId);
				for (int i = 0; i < orders.length(); i++)
				{
					if (orders.getString(i).equals(orderNo))
					{
						break;
					}
					if(i == orders.length()-1)
					{
						orders.put(orderNo);
					}
				}
				cbInvIdVsSSOrdNo.put(invoiceId, orders);
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
			logger.log(Level.INFO,
					"\n\tSSOrdNoVsSSOrderKey updated : " + orderNo + " - " + orderKey);
			logger.log(Level.INFO,
					"\n\tSSOrdKeyVsCBInvId updated : " + orderKey + " - " + invoiceId);
			updateFailedInvoice(invoiceId);
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
			// logger.log(Level.INFO, "\n\torder : " + order.toString());
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
		else if ((status == HttpStatus.SC_UNAUTHORIZED) || (status == HttpStatus.SC_BAD_REQUEST)
				|| (status == HttpStatus.SC_FORBIDDEN) || (status == HttpStatus.SC_NOT_FOUND)
				|| status == HttpStatus.SC_INTERNAL_SERVER_ERROR)
		{
			while (failedInvoiceCount < ssAllOrders.length())
			{
				order = ssAllOrders.getJSONObject(failedInvoiceCount);
				orderNo = order.getString(CBSSIConstants.orderNumber);
				invoiceId = orderVsInvoice.getString(orderNo);
				updateFailedInvoice(invoiceId, "Internal Error");
				// logger.log(Level.INFO, "order : " + order.toString());
				failedInvoiceCount++;
			}
			logger.log(Level.INFO, "\n\t" + status + " : " + messege
					+ "(Wrong user credentials, NO user permissions to this api, Invalid api resource, Internal Error)");
			throw new RuntimeException(messege);
		}
	}

	private void updateFailedInvoice(String invoiceId, String reason) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updateFailedInvoice");
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		failedInvoices.put(invoiceId, reason);
		thirdPartyMapping.put(CBSSIConstants.FailedInvDets, failedInvoices);
		// updThirdPartyMapping("failed invoice inserted");
	}

	private void updateFailedInvoice(String invoiceId) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updateFailedInvoice");
		JSONObject failedInvoices = thirdPartyMapping.getJSONObject(CBSSIConstants.FailedInvDets);
		if (failedInvoices.has(invoiceId))
		{
			failedInvoices.remove(invoiceId);
			thirdPartyMapping.put(CBSSIConstants.FailedInvDets, failedInvoices);
		}
		// updThirdPartyMapping("failed invoice removed");
	}

	private void updSSLastSyncTime(long time) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updLastSyncTime");
		thirdPartyMapping.put(CBSSIConstants.SSLastSyncTime, time);
		// updThirdPartyMapping("SS last sync time updated : " + getSSTimeZone(new
		// Date(time)));
	}

	private void updCBLastSyncTime(long time) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: updLastSyncTime");
		thirdPartyMapping.put(CBSSIConstants.CBLastSyncTime, time);
		// updThirdPartyMapping("CB last sync time updated : " + getSSTimeZone(new
		// Date(time)));
	}

	public void sync() throws Exception
	{
		handlePreviousFailedInvoice();
		logger.log(Level.INFO, "\n\tFailed Invoice Process Finished");
		processSync(CBSSIConstants.SYNC);
		logger.log(Level.INFO, "\n\tSync Process Finished");
	}

	private void handlePreviousFailedInvoice() throws Exception
	{
		processSync(CBSSIConstants.FAILED_INV_PROCESS);
	}

	private boolean hasValidBillingAddress(Invoice invoice, Customer customer, JSONObject ssOrder, int mode) throws Exception
	{
		logger.log(Level.INFO, "\n\tFilling Billing address for invoice :: " + invoice.id());
		BillingAddress invBillAdd = invoice.billingAddress();
		logger.log(Level.INFO, "\n\tinvBillAdd : " + invBillAdd);
		Customer.BillingAddress cusBillAdd = customer.billingAddress();
		logger.log(Level.INFO, "\n\tcusBillAdd : " + cusBillAdd);
		if (invBillAdd == null && cusBillAdd == null)
		{
			logger.log(Level.INFO, "\n\tinvoice : " + invoice.id() + ", has no valid billing address");
			return false;
		}
		JSONObject object = new JSONObject();
		if(invBillAdd != null && cusBillAdd != null && hasValidInvBillAdd(invBillAdd) && hasValidCusBillAdd(cusBillAdd))
		{
			if(isSameAddress(invBillAdd, cusBillAdd))
			{
				fillInvBillAdd(invBillAdd, object);
			}
			else
			{
				fillCusBillAdd(cusBillAdd, object);
			}
			
		}
		else if(invBillAdd != null && hasValidInvBillAdd(invBillAdd))
		{	
			fillInvBillAdd(invBillAdd, object);
		}
		else if(cusBillAdd != null && hasValidCusBillAdd(cusBillAdd))
		{	
			fillCusBillAdd(cusBillAdd, object);
		}
		else
		{
			logger.log(Level.INFO, "\n\tinvoice : " + invoice.id() + ", has no valid billing address");
			return false;
		}
		ssOrder.put("billTo", object);
		return true;
	}
	
	private boolean isSameAddress(BillingAddress invBillAdd, Customer.BillingAddress cusBillAdd)
	{
		if((invBillAdd.firstName() == null && cusBillAdd.firstName() == null) || !(invBillAdd.firstName() != null && cusBillAdd.firstName() != null && invBillAdd.firstName().equals(cusBillAdd.firstName())))
		{
			return false;
		}
		else if((invBillAdd.lastName() == null && cusBillAdd.lastName() == null) || !(invBillAdd.lastName() != null && cusBillAdd.lastName() != null && invBillAdd.lastName().equals(cusBillAdd.lastName())))
		{
			return false;
		}
		else if((invBillAdd.line1() == null && cusBillAdd.line1() == null) || !(invBillAdd.line1() != null && cusBillAdd.line1() != null && invBillAdd.line1().equals(cusBillAdd.line1())))
		{
			return false;
		}
		else if((invBillAdd.line2() == null && cusBillAdd.line2() == null) || !(invBillAdd.line2() != null && cusBillAdd.line2() != null && invBillAdd.line2().equals(cusBillAdd.line2())))
		{
			return false;
		}
		else if((invBillAdd.line3() == null && cusBillAdd.line3() == null) || !(invBillAdd.line3() != null && cusBillAdd.line3() != null && invBillAdd.line3().equals(cusBillAdd.line3())))
		{
			return false;
		}
		else if(!(invBillAdd.city() != null && cusBillAdd.city() != null && invBillAdd.city().equals(cusBillAdd.city())))
		{
			return false;
		}
		else if(!(invBillAdd.state() != null && cusBillAdd.state() != null && invBillAdd.state().equals(cusBillAdd.state())))
		{
			return false;
		}
		else if(!(invBillAdd.zip() != null && cusBillAdd.zip() != null && invBillAdd.zip().equals(cusBillAdd.zip())))
		{
			return false;
		}
		else if(!(invBillAdd.country() != null && cusBillAdd.country() != null && invBillAdd.country().equals(cusBillAdd.country())))
		{
			return false;
		}
		else if((invBillAdd.phone() == null && cusBillAdd.phone() == null) || !(invBillAdd.phone() != null && cusBillAdd.phone() != null && invBillAdd.phone().equals(cusBillAdd.phone())))
		{
			return false;
		}
		return true;
	}

	private boolean hasValidInvBillAdd(BillingAddress invBillAdd) throws Exception
	{
		if ((invBillAdd.firstName() != null || invBillAdd.lastName() != null)
				&& (invBillAdd.line1() != null || invBillAdd.line2() != null
				|| invBillAdd.line3() != null) && invBillAdd.city() != null
				&& invBillAdd.state() != null && invBillAdd.zip() != null
				&& invBillAdd.country() != null)
		{	
			return true;
		}
		return false;
	}
	
	private void fillInvBillAdd(BillingAddress invBillAdd, JSONObject object) throws Exception
	{
		if (invBillAdd.firstName() != null && invBillAdd.lastName() != null)
		{
			object.put("name", invBillAdd.firstName() + " " + invBillAdd.lastName());
		}
		else if (invBillAdd.firstName() != null)
		{
			object.put("name", invBillAdd.firstName());
		}
		else if (invBillAdd.lastName() != null)
		{
			object.put("name", invBillAdd.lastName());
		}
		if (invBillAdd.line1() != null && invBillAdd.line2() != null && invBillAdd.line3() != null)
		{
			object.put("street1", invBillAdd.line1());
			object.put("street2", invBillAdd.line2());
			object.put("street3", invBillAdd.line3());
		}
		else if(invBillAdd.line1() != null && invBillAdd.line2() != null)
		{
			object.put("street1", invBillAdd.line1());
			object.put("street2", invBillAdd.line2());
		}
		else if(invBillAdd.line2() != null && invBillAdd.line3() != null)
		{
			object.put("street1", invBillAdd.line2());
			object.put("street2", invBillAdd.line3());
		}
		else if(invBillAdd.line1() != null && invBillAdd.line3() != null)
		{
			object.put("street1", invBillAdd.line1());
			object.put("street2", invBillAdd.line3());
		}
		else if(invBillAdd.line1() != null)
		{
			object.put("street1", invBillAdd.line1());
		}
		else if(invBillAdd.line2() != null)
		{
			object.put("street1", invBillAdd.line2());
		}
		else if(invBillAdd.line3() != null)
		{
			object.put("street1", invBillAdd.line3());
		}
		if (invBillAdd.validationStatus() != null)
		{
			object.put("addressVerified", invBillAdd.validationStatus().toString());
		}
		if (invBillAdd.phone() != null)
		{
			object.put("phone", invBillAdd.phone());
		}
		object.put("city", invBillAdd.city());
		object.put("state", invBillAdd.state());
		object.put("postalCode", invBillAdd.zip());
		object.put("country", invBillAdd.country());
	}

	private boolean hasValidCusBillAdd(Customer.BillingAddress cusBillAdd) throws Exception
	{
		if ((cusBillAdd.firstName() != null || cusBillAdd.lastName() != null)
				&& (cusBillAdd.line1() != null || cusBillAdd.line2() != null
				|| cusBillAdd.line3() != null) && cusBillAdd.city() != null
				&& cusBillAdd.state() != null && cusBillAdd.zip() != null
				&& cusBillAdd.country() != null)
		{	
			return true;
		}
		return false;
	}

	private void fillCusBillAdd(Customer.BillingAddress cusBillAdd, JSONObject object) throws Exception
	{
		if (cusBillAdd.firstName() != null && cusBillAdd.lastName() != null)
		{
			object.put("name", cusBillAdd.firstName() + " " + cusBillAdd.lastName());
		}
		else if (cusBillAdd.firstName() != null)
		{
			object.put("name", cusBillAdd.firstName());
		}
		else if (cusBillAdd.lastName() != null)
		{
			object.put("name", cusBillAdd.lastName());
		}
		if (cusBillAdd.line1() != null && cusBillAdd.line2() != null && cusBillAdd.line3() != null)
		{
			object.put("street1", cusBillAdd.line1());
			object.put("street2", cusBillAdd.line2());
			object.put("street3", cusBillAdd.line3());
		}
		else if(cusBillAdd.line1() != null && cusBillAdd.line2() != null)
		{
			object.put("street1", cusBillAdd.line1());
			object.put("street2", cusBillAdd.line2());
		}
		else if(cusBillAdd.line2() != null && cusBillAdd.line3() != null)
		{
			object.put("street1", cusBillAdd.line2());
			object.put("street2", cusBillAdd.line3());
		}
		else if(cusBillAdd.line1() != null && cusBillAdd.line3() != null)
		{
			object.put("street1", cusBillAdd.line1());
			object.put("street2", cusBillAdd.line3());
		}
		else if(cusBillAdd.line1() != null)
		{
			object.put("street1", cusBillAdd.line1());
		}
		else if(cusBillAdd.line2() != null)
		{
			object.put("street1", cusBillAdd.line2());
		}
		else if(cusBillAdd.line3() != null)
		{
			object.put("street1", cusBillAdd.line3());
		}
		if (cusBillAdd.validationStatus() != null)
		{
			object.put("addressVerified", cusBillAdd.validationStatus().toString());
		}
		if (cusBillAdd.phone() != null)
		{
			object.put("phone", cusBillAdd.phone());
		}
		
		object.put("city", cusBillAdd.city());
		object.put("state", cusBillAdd.state());
		object.put("postalCode", cusBillAdd.zip());
		object.put("country", cusBillAdd.country());
		
	}

	public Boolean hasValidShippingAddress(Invoice invoice, Subscription subscription, JSONObject ssOrder, int mode) throws Exception
	{
		ShippingAddress invShippAdd = invoice.shippingAddress();
		Subscription.ShippingAddress subShippAdd = subscription.shippingAddress();
		if (invShippAdd == null && subShippAdd == null)
		{
			ssOrder.put("shipTo", ssOrder.getJSONObject("billTo"));
			return true;
		}
		JSONObject object = new JSONObject();
		if(invShippAdd != null && subShippAdd != null && !isInitialFetch(mode) && hasValidInvShippAdd(invShippAdd) && hasValidSubShippAdd(subShippAdd))
		{
			if(isSameAddress(invShippAdd, subShippAdd))
			{
				fillInvShippAdd(invShippAdd, object);
			}
			else
			{
				fillSubShippAdd(subShippAdd, object);
			}
			
		}
		else if(invShippAdd != null && hasValidInvShippAdd(invShippAdd))
		{
			fillInvShippAdd(invShippAdd, object);
		}
		else if(subShippAdd != null && hasValidSubShippAdd(subShippAdd))
		{	
			fillSubShippAdd(subShippAdd, object);
		}
		else
		{
			ssOrder.put("shipTo", ssOrder.getJSONObject("billTo"));
			return true;
		}
		ssOrder.put("shipTo", object);
		return true;
	}

	private boolean isSameAddress(ShippingAddress invShippAdd, Subscription.ShippingAddress subShippAdd)
	{
		if((invShippAdd.firstName() == null && subShippAdd.firstName() == null) || !(invShippAdd.firstName() != null && subShippAdd.firstName() != null && invShippAdd.firstName().equals(subShippAdd.firstName())))
		{
			return false;
		}
		else if((invShippAdd.lastName() == null && subShippAdd.lastName() == null) || !(invShippAdd.lastName() != null && subShippAdd.lastName() != null && invShippAdd.lastName().equals(subShippAdd.lastName())))
		{
			return false;
		}
		else if((invShippAdd.line1() == null && subShippAdd.line1() == null) || !(invShippAdd.line1() != null && subShippAdd.line1() != null && invShippAdd.line1().equals(subShippAdd.line1())))
		{
			return false;
		}
		else if((invShippAdd.line2() == null && subShippAdd.line2() == null) || !(invShippAdd.line2() != null && subShippAdd.line2() != null && invShippAdd.line2().equals(subShippAdd.line2())))
		{
			return false;
		}
		else if((invShippAdd.line3() == null && subShippAdd.line3() == null) || !(invShippAdd.line3() != null && subShippAdd.line3() != null && invShippAdd.line3().equals(subShippAdd.line3())))
		{
			return false;
		}
		else if(!(invShippAdd.city() != null && subShippAdd.city() != null && invShippAdd.city().equals(subShippAdd.city())))
		{
			return false;
		}
		else if(!(invShippAdd.state() != null && subShippAdd.state() != null && invShippAdd.state().equals(subShippAdd.state())))
		{
			return false;
		}
		else if(!(invShippAdd.zip() != null && subShippAdd.zip() != null && invShippAdd.zip().equals(subShippAdd.zip())))
		{
			return false;
		}
		else if(!(invShippAdd.country() != null && subShippAdd.country() != null && invShippAdd.country().equals(subShippAdd.country())))
		{
			return false;
		}
		else if((invShippAdd.phone() == null && subShippAdd.phone() == null) || !(invShippAdd.phone() != null && subShippAdd.phone() != null && invShippAdd.phone().equals(subShippAdd.phone())))
		{
			return false;
		}
		return true;
	}

	private boolean hasValidInvShippAdd(ShippingAddress invShippAdd)
	{
		if ((invShippAdd.firstName() != null || invShippAdd.lastName() != null)
				&& (invShippAdd.line1() != null || invShippAdd.line2() != null
				|| invShippAdd.line3() != null) && invShippAdd.city() != null
				&& invShippAdd.state() != null && invShippAdd.zip() != null
				&& invShippAdd.country() != null)
		{
			return true;
		}
		return false;
	}

	private void fillInvShippAdd(ShippingAddress invShippAdd, JSONObject object) throws Exception
	{
		if (invShippAdd.firstName() != null && invShippAdd.lastName() != null)
		{
			object.put("name", invShippAdd.firstName() + " " + invShippAdd.lastName());
		}
		else if (invShippAdd.firstName() != null)
		{
			object.put("name", invShippAdd.firstName());
		}
		else if (invShippAdd.lastName() != null)
		{
			object.put("name", invShippAdd.lastName());
		}
		if (invShippAdd.line1() != null && invShippAdd.line2() != null && invShippAdd.line3() != null)
		{
			object.put("street1", invShippAdd.line1());
			object.put("street2", invShippAdd.line2());
			object.put("street3", invShippAdd.line3());
		}
		else if(invShippAdd.line1() != null && invShippAdd.line2() != null)
		{
			object.put("street1", invShippAdd.line1());
			object.put("street2", invShippAdd.line2());
		}
		else if(invShippAdd.line2() != null && invShippAdd.line3() != null)
		{
			object.put("street1", invShippAdd.line2());
			object.put("street2", invShippAdd.line3());
		}
		else if(invShippAdd.line1() != null && invShippAdd.line3() != null)
		{
			object.put("street1", invShippAdd.line1());
			object.put("street2", invShippAdd.line3());
		}
		else if(invShippAdd.line1() != null)
		{
			object.put("street1", invShippAdd.line1());
		}
		else if(invShippAdd.line2() != null)
		{
			object.put("street1", invShippAdd.line2());
		}
		else if(invShippAdd.line3() != null)
		{
			object.put("street1", invShippAdd.line3());
		}
		if (invShippAdd.validationStatus() != null)
		{
			object.put("addressVerified", invShippAdd.validationStatus().toString());
		}
		if (invShippAdd.phone() != null)
		{
			object.put("phone", invShippAdd.phone());
		}
		object.put("city", invShippAdd.city());
		object.put("state", invShippAdd.state());
		object.put("postalCode", invShippAdd.zip());
		object.put("country", invShippAdd.country());
	}

	private boolean hasValidSubShippAdd(Subscription.ShippingAddress subShippAdd) throws Exception
	{
		if ((subShippAdd.firstName() != null || subShippAdd.lastName() != null)
				&& (subShippAdd.line1() != null || subShippAdd.line2() != null
				|| subShippAdd.line3() != null) && subShippAdd.city() != null
				&& subShippAdd.state() != null && subShippAdd.zip() != null
				&& subShippAdd.country() != null)
		{
			return true;
		}
		return false;
	}

	private void fillSubShippAdd(Subscription.ShippingAddress subShippAdd, JSONObject object) throws Exception
	{
		if (subShippAdd.firstName() != null && subShippAdd.lastName() != null)
		{
			object.put("name", subShippAdd.firstName() + " " + subShippAdd.lastName());
		}
		else if (subShippAdd.firstName() != null)
		{
			object.put("name", subShippAdd.firstName());
		}
		else if (subShippAdd.lastName() != null)
		{
			object.put("name", subShippAdd.lastName());
		}
		if (subShippAdd.line1() != null && subShippAdd.line2() != null && subShippAdd.line3() != null)
		{
			object.put("street1", subShippAdd.line1());
			object.put("street2", subShippAdd.line2());
			object.put("street3", subShippAdd.line3());
		}
		else if(subShippAdd.line1() != null && subShippAdd.line2() != null)
		{
			object.put("street1", subShippAdd.line1());
			object.put("street2", subShippAdd.line2());
		}
		else if(subShippAdd.line2() != null && subShippAdd.line3() != null)
		{
			object.put("street1", subShippAdd.line2());
			object.put("street2", subShippAdd.line3());
		}
		else if(subShippAdd.line1() != null && subShippAdd.line3() != null)
		{
			object.put("street1", subShippAdd.line1());
			object.put("street2", subShippAdd.line3());
		}
		else if(subShippAdd.line1() != null)
		{
			object.put("street1", subShippAdd.line1());
		}
		else if(subShippAdd.line2() != null)
		{
			object.put("street1", subShippAdd.line2());
		}
		else if(subShippAdd.line3() != null)
		{
			object.put("street1", subShippAdd.line3());
		}
		if (subShippAdd.validationStatus() != null)
		{
			object.put("addressVerified", subShippAdd.validationStatus().toString());
		}
		if (subShippAdd.phone() != null)
		{
			object.put("phone", subShippAdd.phone());
		}
		
		object.put("city", subShippAdd.city());
		object.put("state", subShippAdd.state());
		object.put("postalCode", subShippAdd.zip());
		object.put("country", subShippAdd.country());	
	}

	private void fillCustomerInfo(Invoice invoice, Customer customer, JSONObject order) throws Exception
	{
		logger.log(Level.INFO, "\n\tfilling Customer Info of invoice-id: " + invoice.id());
		order.put("customerUsername", name(customer));
		order.put("customerEmail", customer.email() == null ? "" : customer.email());
	}

	private String name(Customer customer) throws Exception
	{
		if (customer.firstName() != null && customer.lastName() != null)
		{
			return customer.firstName() + " " + customer.lastName();
		}
		else if (customer.firstName() != null)
		{
			return customer.firstName();
		}
		else if (customer.lastName() != null)
		{
			return customer.lastName();
		}
		return "";
	}

	private String getSSOrderKey(String orderNo) throws Exception
	{
		logger.log(Level.INFO, "\n\tMethod: getOrderNoForInvoice");
		JSONObject ordNoVsOrdkey = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdNoVsSSOrderKey);
		return ordNoVsOrdkey.getString(orderNo);
	}

	private void fillOrders(int i, Invoice invoice, JSONObject ssOrder, Date orderDate,
			JSONObject orderVsInvoice, int mode, int billingPeriod) throws Exception
	{
		logger.log(Level.INFO, "\n\tOrderDate : " + getSSTimeZone(orderDate));
		JSONObject cbInvIdVsSSOrdNo = thirdPartyMapping
				.getJSONObject(CBSSIConstants.CBInvIdVSSOrdNo);
		int disc = 0;
		JSONObject product;
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
		ssOrder.put("amountPaid", (getSSCurrency(invoice.amountPaid(), cbCurrCode)));
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
		for(Discount discount : invoice.discounts())
		{
//			 product = new JSONObject();
//			 product.put("adjustment", true);
//			 product.put("name", discount.description());
//			 product.put("unitPrice", getSSCurrency(discount.amount(), cbCurrCode));
//			 product.put("quantity", 1);
//			 lineItems.put(product);
			 disc+= discount.amount();
		}
		logger.log(Level.INFO, "\n\tdiscountAmount after conversion : " + getSSCurrency(disc, cbCurrCode));
		ssOrder.put("items", lineItems);

	}

	private double getSSCurrency(Integer amount, String cbCurrCode) throws Exception
	{
		if (cbCurrCode.equals(ssCurrCode))
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
		return client.target(CBSSIConstants.createOrdersUrl)
				.request(MediaType.APPLICATION_JSON_TYPE).header("Authorization", ssApiKey)
				.post(payload);
	}

	private void createCBOrders(JSONObject ordersFromCBInvoices, int mode) throws Exception
	{
		logger.log(Level.INFO, "\n\tgetting shipstation orders");
		Client client = ClientBuilder.newClient();
		String orderStatus = "";
		String customerNote = "";
		String trackingId = "";
		String batchId = "";
		String orderNo;
		String orderKey;
		String invoiceId;
		String ssOrdId;
		StringBuilder url;
		JSONObject ssOrdNoVsSSOrdKey = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdNoVsSSOrderKey);
		JSONObject ssOrdKeyVsCBInvId = thirdPartyMapping
				.getJSONObject(CBSSIConstants.SSOrdKeyVsCBInvId);
		if (!isInitialFetch(mode))
		{
			Date lastSync = new Date(getSSLastSyncTime());
			url = new StringBuilder(CBSSIConstants.listOrdersAfter).append(getSSTimeZone(lastSync));
		}
		url = new StringBuilder(CBSSIConstants.ordersUrl);
		url.append("?orderStatus=awaiting_shipment");
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
					orderShipInfo = getShipmentInfo(orderNo);
					orderStatus = order.getString("orderStatus");
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
		else if ((status == HttpStatus.SC_UNAUTHORIZED) || (status == HttpStatus.SC_BAD_REQUEST)
				|| (status == HttpStatus.SC_FORBIDDEN) || (status == HttpStatus.SC_NOT_FOUND)
				|| status == HttpStatus.SC_INTERNAL_SERVER_ERROR)
		{
			logger.log(Level.INFO, "\n\t" + status + " : " + messege
					+ "(Wrong user credentials, NO user permissions to this api, Invalid api resource, Internal Error)");
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
			if (orderId.equals(ordFromCB.getString("orderId")))// ordFromCB contains current sync
																// response
			{
				return true;
			}
			else if (ssOrdVsCBOrd.has(orderId))// ssOrdVsCBOrd contains old orders too that may be
												// updated in SS
			{
				return true;
			}
		}
		return false;
	}

	private JSONObject getShipmentInfo(String orderNo) throws JSONException
	{
		Client client = ClientBuilder.newClient();
		Response response = client.target(CBSSIConstants.shipmentUrl + "?orderNumber=" + orderNo)
				.request(MediaType.TEXT_PLAIN_TYPE).header("Authorization", ssApiKey).get();
		return new JSONObject(response.readEntity(String.class));
	}

	private void create(String invoiceId, String ssOrdId, String orderStatus, String batchId,
			String trackingId, String customerNote) throws Exception
	{
		logger.log(Level.INFO, "\n\tCreating ChargeBee orders");
		Order order;
		Result result;
		String cbOrdId = getCBOrdId(ssOrdId);
		if (cbOrdId == null) // new order
		{

			result = Order.create().id(ssOrdId).invoiceId(invoiceId).status(CBSSIConstants.cbOrdStatusForSSOrdStatus.get(orderStatus)).fulfillmentStatus(orderStatus)
					.referenceId(ssOrdId).batchId(batchId).note(customerNote).request();
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
		JSONObject ssOrdVsCBOrd = thirdPartyMapping.getJSONObject(CBSSIConstants.SSOrdVsCBOrd);
		if (ssOrdVsCBOrd.has(ssOrdId))
		{
			return ssOrdVsCBOrd.getString(ssOrdId);
		}
		return null;
	}

	private void updSSOrderVsCBOrder(String ssOrderId, String cbOrderId) throws Exception
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
		// updThirdPartyMapping("SSOrdVsCBOrd updated");
	}

	public String getTime()
	{
		long date = (new Date().getTime()) / 1000L;
		return String.valueOf(date);
	}

	public Long getCBLastSyncTime() throws JSONException
	{
		if (thirdPartyMapping.has(CBSSIConstants.CBLastSyncTime))
		{
			return thirdPartyMapping.getLong(CBSSIConstants.CBLastSyncTime);
		}
		return null;
	}

	public Long getSSLastSyncTime() throws JSONException
	{
		if (thirdPartyMapping.has(CBSSIConstants.SSLastSyncTime))
		{
			return thirdPartyMapping.getLong(CBSSIConstants.SSLastSyncTime);
		}
		return null;
	}

	private String[] listAsArray(JSONArray failedInvoices) throws JSONException
	{
		logger.log(Level.INFO, "\n\tMethod : listAsArray");
		int length = failedInvoices.length();
		String[] array = new String[length];
		for (int i = 0; i < length; i++)
		{
			array[i] = failedInvoices.getString(i);
		}
		return array;
	}

	private JSONObject getThirdPartyMapping() throws JSONException
	{
		JSONObject obj = null;

		if (obj == null)
		{
			obj = new JSONObject();
			obj.put(CBSSIConstants.CBInvIdVsCBSubId, new JSONObject());
			obj.put(CBSSIConstants.CBInvIdVsCBCusId, new JSONObject());
			obj.put(CBSSIConstants.CBCusIdVsCBInvId, new JSONObject());
			obj.put(CBSSIConstants.CBSubIdVsCBInvId, new JSONObject());
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
		logger.log(Level.INFO, "\n\tMethod : getOrderStatus");
		
		return CBSSIConstants.ssOrdStatusOfCBInvStatus.get(invoiceStatus);
	}

}