/**
* Copyright 2018: Thomson Reuters Global Resources. All Rights Reserved.
 * Proprietary and Confidential information of TRGR. Disclosure, Use or Reproduction without the written 
 * authorization of TRGR is prohibited
 *
 */
package com.west.novus;

import com.westgroup.novus.productapi.*;
import java.io.*;

public class DocTest
{

	/**
	 * 
	 */
	public DocTest()
	{
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		Novus novus = null;
		try
		{
			// setup, set queue criteria and create a PIT
			novus = new Novus();
//			novus.setQueueCriteria("NOVUS1A", "qc");
			novus.setQueueCriteria(null, "prod");
			novus.createPit();
			novus.setResponseTimeout(30000);
			novus.setProductName("Keycite-NIMS:Names_Audit");
			novus.setBusinessUnit("Legal");
			System.out.println("Have productName=" + novus.getProductName());
			System.out.println("Have BusUnit=" + novus.getBusinessUnit());

			Find find = novus.getFind();
//			Document doc = find.getDocument(null,
//					"i0F0A8439000060AE11D3B4BC2AE7D1B1");
//			Document doc = find.getDocument("w_lt_td_motions",
//					"I0c32c46a4c7111da8cc9b4c14e983401");
			Document doc = find.getDocument(null,
					"Id23b0dd24fe811e490d4edf60ce7d742");
//			Document doc = find.getDocument("w_lt_td_ipadmin",
//					"I7470f10064e411e8afa6873146fe5b33");
			if (doc != null)
			{
				String errCode = doc.getErrorCode();
				System.out.println("Have errorCode=" + errCode);
				String text = doc.getText();
				if (text != null)
				{
					System.out.println("text=" + text);
//					PrintWriter writer = new PrintWriter(new BufferedWriter(
//							new FileWriter(doc.getGuid() + ".xml")));
//					writer.write(text);
//					writer.flush();
//					writer.close();
				}
				else
					System.out.println("Could not pull text for document "
							+ doc.getGuid());
				System.out.println("Found document " + doc.getGuid()
						+ " Doc Collection " + doc.getCollection());
				System.out.println("Have filledFilteredText value=" + doc.getFilledFilteredText());
				System.out.println("Have filledFilteredMetadata value=" + doc.getFilledFilteredMetaData());
				
				SearchResult searchRes = doc.getSearchResult();
				if (searchRes != null)
				{
					System.out.println(" have a non-null searchResult!");
					String[] metadataNames = searchRes.getMetaNames();
					for (String currName : metadataNames)
					{
						System.out.println("CurrName=" + currName);
					}
				}
				// get the mime type
				// from MMLocation
				// from BLOB
				
				String[] fieldNames = doc.getDocumentFieldNames();
				if (fieldNames != null)
				{
					for (String fName : fieldNames)
					{
						System.out.println("currFName=" + fName);
					}
				}
				String metadata = doc.getMetaData();
				System.out.println("metadata=" + metadata);
				String collection = doc.getCollection();
				System.out.println("collection = " + collection);
				System.out.println("searchCollectionSet=" + doc.getSearchCollectionSet());
			}
			else
				System.out.println("Could not find document "
						+ "i0F0A8439000060AE11D3B4BC2AE7D1B1");
			find.addDocumentInfo("riafedana",
					"i0F0A8439000060AE11D3B4BC2AE7CC67");
			find.addDocumentInfo("riafedana",
					"i0F0A8439000060AE11D3B4BC2AE7CC6D");
			Document[] docs = find.getDocuments();
			if (docs != null)
			{
				for (int i = 0; i < docs.length; i++)
					System.out.println("Found document " + docs[i].getGuid()
							+ " Doc Collection " + docs[i].getCollection());
				find.fillDocumentText(docs);
				for (int i = 0; i < docs.length; i++)
				{
					String text = docs[i].getText();
					PrintWriter writer = new PrintWriter(new BufferedWriter(
							new FileWriter(docs[i].getGuid() + ".xml")));
					writer.write(text);
					writer.flush();
					writer.close();
				}
				com.westgroup.novus.productapi.Filter filter = new com.westgroup.novus.productapi.Filter();
				filter.addElementToInclude("RNAME");
				FilterContext context = new FilterContext();
				find.fillDocumentText(docs, filter, context);
				for (int i = 0; i < docs.length; i++)
				{
					String part = docs[i].getFilledFilteredText();
					if (part != null)
						System.out.println("Title " + part);
					else
						System.out.println("Filter request returned "
								+ docs[i].getErrorCode());
				}
			}
			else
			{
				System.out.println("Could not find documents "
						+ "i0F0A8439000060AE11D3B4BC2AE7CC67 and "
						+ "i0F0A8439000060AE11D3B4BC2AE7CC6D");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		if (novus != null)
			novus.shutdownMQ();

	}

}
