/**
* Copyright 2019: Thomson Reuters Global Resources. All Rights Reserved.
 * Proprietary and Confidential information of TRGR. Disclosure, Use or Reproduction without the written 
 * authorization of TRGR is prohibited
 *
 */
package com.west.novus;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;
import java.util.Vector;

import com.westgroup.novus.productapi.Document;
import com.westgroup.novus.productapi.FilterContext;
import com.westgroup.novus.productapi.Find;
import com.westgroup.novus.productapi.Novus;
import com.westgroup.novus.productapi.NovusException;
import com.westgroup.novus.productapi.SearchResult;

public class GetDocData
{

	private static boolean fixData = false;
	/**
	 * 
	 */
	public GetDocData()
	{
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args)
	{
		GetDocData gdd = new GetDocData();
		//gdd.reportDataForDocUuid("I43fc32969cb711d993e6d35cc61aab4a");

		Collection<String> docUuidCollect = new Vector<String>();
		//docUuidCollect.add("If239dd819cc111d9bdd1cfdd544ca3a4");
		docUuidCollect.add("I4d05b9e46b9211ebbea4f0dc9fb69570");


		gdd.reportDataForDocSet(docUuidCollect);
		
	}

	private void reportDataForDocUuid(String doc_uuid)
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

			Find find = novus.getFind();
			Document doc = find.getDocument(null,
					doc_uuid);
			
			if (doc != null)
			{
				outputDocData(doc);
			}
			else
				System.out.println("Could not find document "
						+ doc_uuid);
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		if (novus != null)
			novus.shutdownMQ();

	}
	
	private void outputDocData(Document doc) 
	{
		String txtFormat = "TEXT";
		String pdfFormat = "PDF";
		String language = null;
		String docJmaFlag = "N";
		String createDate = null;
		String addedDate = null;

		if (doc != null)
		{
			String errCode = doc.getErrorCode();
			if (errCode != null)
				System.out.println("Have errorCode=" + errCode);
			else
			{
//				System.out.println(" The doc text contains the actual text of the document.  We don't need this to determine the loaded location or parallel cites");
//				String text = doc.getText();
//				if (text != null)
//				{
//					System.out.println("text=" + text);
//				}
				String metadata;
				String docUuid = "";
				try
				{
					metadata = doc.getMetaData();
					//System.out.println("metadata=" + metadata);
					String collection = doc.getCollection();
					//System.out.println("collection = " + collection);
					String component = getCompnentFromMetadata(metadata);
					//System.out.println("component=" + component);
					String prismClipDate = getPrismClipDateFromMetadata(metadata);
					//System.out.println("prismClipDate=" + prismClipDate);
					String pubcode = getPubCodeFromMetadata(metadata);
					//System.out.println("pubCode=" + pubcode);
					Collection<String> rfUuidCollect = new Vector<String>();
					rfUuidCollect.addAll(getRfUuidCollectFromMetadata(metadata));
					docUuid = doc.getGuid();
					
//					for (String currRfUuid : rfUuidCollect)
//					{
//						System.out.println("currRfUuid=" + currRfUuid);
//					}
					String collectionLoadedFlag = "Y";
					String componentLoadedFlag = "Y";
					
					if (fixData)
					{
						/* fixing data in MachV */
						System.out.println("/* Primary */");
						System.out.println(outputDheClearStatement(docUuid));
						System.out.println(outputRfdClearStatement(docUuid));
						System.out.println(outputDocClearStatement(docUuid));
						if (collection.indexOf("pdf") > -1)
							System.out.println(outputDocInsertStatement(docUuid, pdfFormat, language, docJmaFlag, createDate));
						else
							System.out.println(outputDocInsertStatement(docUuid, txtFormat, language, docJmaFlag, createDate));

						System.out.println(outputDheInsertStatement(docUuid, collection, collectionLoadedFlag, component, componentLoadedFlag, addedDate, prismClipDate, pubcode));
						System.out.println(outputPrimaryCiteRfDocInsertStatements(metadata, docUuid));
						if (hasParallelCites(metadata))
						{
							System.out.println("/* Parallel */");
							Collection<String> parallelCiteUuidCollect = new Vector<String>();
							parallelCiteUuidCollect.addAll(getParallelCiteUuids(metadata));
							// build parallelCite docs - but no dhe's
							outputParallelCiteDocInsertStatements(parallelCiteUuidCollect);
							outputParallelCiteDheInsertStatements(parallelCiteUuidCollect);
							// build rfDoc
							outputParallelCiteRfDocInsertStatements(parallelCiteUuidCollect);
						}
						System.out.println(crlf);
					}
					else
					{
						/* just reporting data */
						System.out.println("Just report the data!");
					}
				}
				catch (NovusException e)
				{
					String NotFoundExceptionMsg = "Cannot find the collection for the content guid";
					String exceptionMsg = e.getMessage();
					if (exceptionMsg.indexOf(NotFoundExceptionMsg) > -1)
					{
						if (fixData)
						{
							docUuid = getDocUuidFromExceptionMsg(exceptionMsg);
							System.out.println(e.getMessage() + " so it is not loaded to novus - build out as follows:");
							System.out.println(outputDheClearStatement(docUuid));
							System.out.println(outputRfdClearStatement(docUuid));
							System.out.println(outputDocClearStatement(docUuid));
							System.out.println(outputDocInsertStatement(docUuid, txtFormat, language, docJmaFlag, createDate));
							System.out.println(outputDheInsertStatement(docUuid, null, "N", null, "N", addedDate, null, null));
						}
					}
					else
					{
						String msg = this.getClass().getName() + "." + "outputDocData()" + " encountered:" + e.getClass().getName() + 
								" with msg=" + e.getMessage();
								if (e.getCause() != null)
								              msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
								System.out.println(msg);
					}
				}
			}
		}
		else
			System.out.println("Null Document passed - can't outputDocData on a null value ");	
	}
	/**
	 * @param exceptionMsg
	 * @return
	 */
	private String getDocUuidFromExceptionMsg(String exceptionMsg)
	{
		String retVal = "";
		// Cannot find the collection for the content guid I1b67bd700b9a11e490d4edf60ce7d742.
		String msgBase = "Cannot find the collection for the content guid ";
		if (exceptionMsg.indexOf(msgBase) == 0)
		{
			int start = exceptionMsg.indexOf(msgBase) + msgBase.length();
			retVal = exceptionMsg.substring(start, exceptionMsg.length() -1);
		}
		return retVal;
	}

	/**
	 * @param parallelCiteUuidCollect
	 */
	private void outputParallelCiteDheInsertStatements(
			Collection<String> parallelCiteUuidCollect)
	{
		String retValue = "";
		String wlDheInsert = "";
		String novusDheInsert = "";
		String queryPart = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG) values (";

		for (String currDocUuid : parallelCiteUuidCollect)
		{
			wlDheInsert = queryPart + "10,'" + currDocUuid + "', 'N');";
			novusDheInsert = queryPart + "20,'" + currDocUuid + "', 'N');";
			System.out.println(wlDheInsert);
			System.out.println(novusDheInsert);
		}
	}
	private static String crlf = System.getProperties().getProperty("line.separator");
	
	/**
	 * @param rfUuidCollect
	 * @param docUuid
	 */
	private String outputRfDocInsertStatements(Collection<String> rfUuidCollect,
			String docUuid)
	{
//		Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values ('I073615b09cbc11d9bc61beebb95be672','I073615b19cbc11d9bc61beebb95be672');
		String queryPart = "Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values ('";
		String retValue = "";
		for (String rfUuid : rfUuidCollect)
		{
			retValue += queryPart + rfUuid + "','" + docUuid + "');" + crlf;
		}
		return retValue;
	}
	/**
	 * @param docUuid
	 * @return
	 */
	private String outputDheClearStatement(String docUuid)
	{
		String queryPart = "Delete from machv.doc_hosting_env where doc_uuid = '";
		String retValue = queryPart + docUuid + "';";
		return retValue;
	}

	/**
	 * @param docUuid
	 * @return
	 */
	private String outputRfdClearStatement(String docUuid)
	{
		String queryPart = "Delete from machv.rendition_family_document where doc_uuid = '";
		String retValue = queryPart + docUuid + "';";
		return retValue;
	}

	/**
	 * @param docUuid
	 * @return
	 */
	private String outputDocClearStatement(String docUuid)
	{
		String queryPart = "Delete from machv.document where doc_uuid = '";
		String retValue = queryPart + docUuid + "';";
		return retValue;
	}

	
	/**
	 * @param rfUuidCollect
	 * @param docUuid
	 */
	private String outputRfDocInsertStatement(String rfUuid, String docUuid)
	{
//		Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values ('I073615b09cbc11d9bc61beebb95be672','I073615b19cbc11d9bc61beebb95be672');
		String queryPart = "Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values ('";
		String retValue = "";
		retValue += queryPart + rfUuid + "','" + docUuid + "');" + crlf;
		return retValue;
	}
	/**
	 * @param parallelCiteUuidCollect
	 */
	private void outputParallelCiteRfDocInsertStatements(
			Collection<String> parallelCiteUuidCollect)
	{
		String queryPart = "Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values ('";
		String retValue = "";
		for (String currRfUuid : parallelCiteUuidCollect)
		{
			retValue += queryPart + currRfUuid + "','" + currRfUuid + "');" + crlf;
		}
		System.out.println(retValue);
	}

	/**
	 * for parallel cite docs, we build out a doc with a docUuid = the rfUuid and no dhe's
	 * @param parallelCiteUuidCollect
	 */
	private void outputParallelCiteDocInsertStatements(
			Collection<String> parallelCiteUuidCollect)
	{
		String queryPart = "Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,DOC_JMA_FLAG) values ('";
		String retValue = "";
		String format = "TEXT";
		
		for (String currDocUuid : parallelCiteUuidCollect)
		{
			retValue+= queryPart + currDocUuid + "', '" + format +  "', " + "'N');" + crlf;
		}
		System.out.println(retValue);
	}

	/**
	 * @param docUuid
	 * @param collection
	 * @param collectionLoadedFlag
	 * @param component
	 * @param componentLoadedFlag
	 * @param addedDate
	 * @param prismClipDate
	 * @param pubcode
	 */
	private String outputDheInsertStatement(String docUuid, String collection,
			String collectionLoadedFlag, String component,
			String componentLoadedFlag, String addedDate, String prismClipDate,
			String pubcode)
	{
//		Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE)
//		values (20,'I073615b19cbc11d9bc61beebb95be672','Y','w_cs_sct1',to_date('14-DEC-91 13:25:00','DD-MON-RR HH24:MI:SS'),to_date('01-JUL-08 00:00:01','DD-MON-RR HH24:MI:SS'),578);
//		Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) 
//		values (10,'I073615b19cbc11d9bc61beebb95be672','Y','AWSCTOLD',null,null,null);
		String retValue = "";
		String wlDheInsert = "";
		String novusDheInsert = "";
		String queryPart = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) values (";
		wlDheInsert = queryPart + "10,'" + docUuid + "', '" + componentLoadedFlag + "', '" + component + "', " + null + ", " + null + "," + null + ");";
		novusDheInsert = queryPart + "20,'" + docUuid + "', '" + collectionLoadedFlag + "', '" + collection + "', ";
		
		// expect date to be in format of YYYYMMDDHH24MISS
		if (addedDate != null)
			novusDheInsert += " to_date('" + addedDate + "','YYYYMMDDHH24MISS')";
		else
			novusDheInsert += null;;
		novusDheInsert += ", ";
		if (prismClipDate != null)
			novusDheInsert += " to_date('" + prismClipDate + "','YYYYMMDDHH24MISS')";
		else
			novusDheInsert += null;;
		novusDheInsert +=  ", "  + pubcode + ");";
		retValue = wlDheInsert + crlf + novusDheInsert;
		return retValue;
	}

	/**
	 * @param docUuid
	 * @param format
	 * @param language
	 * @param docJmaFlag
	 * @param createDate
	 */
	private String outputDocInsertStatement(String docUuid, String format,
			String language, String docJmaFlag, String createDate)
	{
//		Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,LANGUAGE,DOC_JMA_FLAG,CREATE_DATE) values ('I073615b19cbc11d9bc61beebb95be672','TEXT',null,null,null);
		String retValue = "";
		String queryPart = "Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,LANGUAGE,DOC_JMA_FLAG,CREATE_DATE) values ('";
		retValue= queryPart + docUuid + "', '" + format +  "', ";
		if (language == null)
			retValue += language + ", '";
		else
			retValue += "'" + language + "', ";
		retValue +=  docJmaFlag + "', ";
		if (createDate != null)
			retValue += " to_date('" + createDate + "','YYYYMMDDHH24MISS')";
		else
			retValue += null;
		retValue += ");";
		return retValue;

	}

	private static String PUB_CODE_START_ELEMENT = "<md.sourcepubid>";
	private static String PUB_CODE_END_ELEMENT = "</md.sourcepubid>";
	/**
	 * @param metadata
	 * @return
	 */
	private String getPubCodeFromMetadata(String metadata)
	{
		String retVal =null;
		if (metadata.indexOf(PUB_CODE_START_ELEMENT) > -1)
		{
			int start = metadata.indexOf(PUB_CODE_START_ELEMENT) + PUB_CODE_START_ELEMENT.length();
			int end = metadata.indexOf(PUB_CODE_END_ELEMENT);
			retVal = metadata.substring(start, end);
			if ((retVal != null) && (retVal.length() == 0))
				retVal = null;
		}
		else
			;
		return retVal;
	}
	private static String RF_UUID_START_ELEMENT = "<md.rendition.uuid>";
	private static String RF_UUID_END_ELEMENT = "</md.rendition.uuid>";
	/**
	 * @param metadata
	 * @return
	 */
	private Collection<? extends String> getRfUuidCollectFromMetadata(
			String metadata)
	{
		String tempMetadata = metadata;
		Collection<String> retCollect = new Vector<String>();
		while (tempMetadata.indexOf(RF_UUID_START_ELEMENT) > -1)
		{
			int start = tempMetadata.indexOf(RF_UUID_START_ELEMENT) + RF_UUID_START_ELEMENT.length();
			int end = tempMetadata.indexOf(RF_UUID_END_ELEMENT);
			retCollect.add(tempMetadata.substring(start, end));
			tempMetadata = tempMetadata.substring(end + RF_UUID_END_ELEMENT.length());
		}
		return retCollect;
	}

	private static String PARALLEL_CITE_ID = "<md.display.parallelcite ID=\"";
	/**
	 * @param metadata
	 * @return
	 */
	private Collection<? extends String> getParallelCiteUuids(String metadata)
	{
		String tempMetadata = metadata;
		Collection<String> retCollect = new Vector<String>();
		while (tempMetadata.indexOf(PARALLEL_CITE_ID) > -1)
		{
			int start = tempMetadata.indexOf(PARALLEL_CITE_ID) + PARALLEL_CITE_ID.length();
			String sUuid = tempMetadata.substring(start, start+33);
			
			// change the leading S to I
			String rfUuid = sUuid.replaceFirst("S", "I");
			retCollect.add(rfUuid);
			tempMetadata = tempMetadata.substring(tempMetadata.indexOf(PARALLEL_CITE_END_ELEMENT) + PARALLEL_CITE_END_ELEMENT.length());
		}
		return retCollect;
	}

	private static String PRISM_CLIP_START_ELEMENT = "<prism-clipdate>";
	private static String PRISM_CLIP_END_ELEMENT = "</prism-clipdate>";
	/**
	 * @param metadata
	 * @return
	 */
	private String getPrismClipDateFromMetadata(String metadata)
	{
		String emptyPrismClipDate = "00000000000000";
		String retVal = null;
		if (metadata.indexOf(COMPONENT_START_ELEMENT) > -1)
		{
			int start = metadata.indexOf(PRISM_CLIP_START_ELEMENT) + PRISM_CLIP_START_ELEMENT.length();
			int end = metadata.indexOf(PRISM_CLIP_END_ELEMENT);
			retVal = metadata.substring(start, end);
			if ((retVal != null) && (retVal.compareTo(emptyPrismClipDate) == 0))
				retVal = null;
		}
		else
			;
		
		return retVal;
	}
	private static String PRIMARY_CITE_START_ELEMENT = "<md.primarycite>";
	private static String PRIMARY_CITE_END_ELEMENT = "<md.primarycite>";
	private static String PRIMARY_CITE_ID = "<md.display.primarycite ID=\"";
	private static String WL_PRIMARY_CITE_ID = "<md.display.primarycite type=\"Westlaw\" ID=\"";
	/**
	 * @param metadata
	 * @param docUuid
	 * @return
	 */
	private String outputPrimaryCiteRfDocInsertStatements(String metadata,
			String docUuid)
	{
		String retVal = null;
		if (metadata.indexOf(PRIMARY_CITE_START_ELEMENT) > -1)
		{
			int start = 0;
			String sUuid = null;
			if (metadata.indexOf(PRIMARY_CITE_ID) > -1)
			{
				start = metadata.indexOf(PRIMARY_CITE_ID) + PRIMARY_CITE_ID.length();
				sUuid = metadata.substring(start, start+33);
			}
			if (metadata.indexOf(WL_PRIMARY_CITE_ID) > -1)
			{
				start = metadata.indexOf(WL_PRIMARY_CITE_ID) + WL_PRIMARY_CITE_ID.length();
				sUuid = metadata.substring(start, start+33);
			}
			
			if (sUuid != null)
			{
				// change the leading S to I
				String rfUuid = sUuid.replaceFirst("S", "I");
				retVal = outputRfDocInsertStatement(rfUuid, docUuid);
			}
			else
				retVal = "problem locating rfUuid in outputPrimaryCiteRfDocInsertStatements";
		}
		return retVal;
	}

	private static String PARALLEL_CITE_START_ELEMENT = "<md.parallelcite>";
	private static String PARALLEL_CITE_END_ELEMENT = "</md.parallelcite>";
	/**
	 * @param metadata
	 * @return
	 */
	private boolean hasParallelCites(String metadata)
	{
		int start = metadata.indexOf(PARALLEL_CITE_START_ELEMENT);
		int end = metadata.indexOf(PARALLEL_CITE_END_ELEMENT);
		if (start < 0)
			return false;
		else
		{
			// there must be something between start and end!
			if ((start + PARALLEL_CITE_START_ELEMENT.length() + 2) > end)
				return false;
			else
				return true;
		}
	}
	/**
	 * @param metadata
	 * @return
	 */
	private String getCompnentFromMetadata(String metadata)
	{
		String retVal = "";
		if (metadata.indexOf(COMPONENT_START_ELEMENT) > -1)
		{
			int start = metadata.indexOf(COMPONENT_START_ELEMENT) + COMPONENT_START_ELEMENT.length();
			int end = metadata.indexOf(COMPONENT_END_ELEMENT);
			retVal = metadata.substring(start, end);
		}
		else
			;
		return retVal;
	}

	private static String COMPONENT_START_ELEMENT = "<md.wl.database.identifier>";
	private static String COMPONENT_END_ELEMENT = "</md.wl.database.identifier>";

	private void reportDataForDocSet(Collection<String> docUuidCollect)
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
			novus.setProductName("Keycite-NIMS:DF_Audit");
			novus.setBusinessUnit("Legal");
//			System.out.println("Have productName=" + novus.getProductName());
//			System.out.println("Have BusUnit=" + novus.getBusinessUnit());

			Find find = novus.getFind();
			for (String currDocUuid : docUuidCollect)
			{
				find.addDocumentInfo(null, currDocUuid);
			}

			// now trying finding multiple docs at once
			Document[] docs = find.getDocuments();
			if (docs != null)
			{
				for (int i = 0; i < docs.length; i++)
				{
					outputDocData(docs[i]);
				}
			}
			else
			{
				System.out.println("Could not find documents ");
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
