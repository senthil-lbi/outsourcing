package com.cb.ss;

import com.chargebee.models.CreditNote;
import com.chargebee.models.Invoice;

public class CBSSUserConfig
{

	// same subscription invoices will be added as single order based on user config
	public static boolean mergeInvoices()
	{
		return true;
	}

	public static String[] getOrderStatus()
	{
		return new String[] { Invoice.Status.PAID.toString() };
	}

	public static boolean handleCreditNotes()
	{
		return true;
	}

	public static String[] getCreditNoteStatus()
	{
		return new String[] { CreditNote.Status.ADJUSTED.toString() };
	}

}