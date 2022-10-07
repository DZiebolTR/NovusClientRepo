/**
reportThisSerialData Copyright 2019: Thomson Reuters Global Resources. All Rights Reserved.
 * Proprietary and Confidential information of TRGR. Disclosure, Use or Reproduction without the written 
 * authorization of TRGR is prohibited
 *
 */
package com.west.novus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import com.westgroup.novus.productapi.Document;
import com.westgroup.novus.productapi.Novus;
import com.westgroup.novus.productapi.NovusException;
import com.westgroup.novus.productapi.Progress;
import com.westgroup.novus.productapi.Search;
import com.westgroup.novus.productapi.SearchResult;
import com.west.snowflake.Uuid;

public class GetSerialData
{

	/**
	 * 
	 */
	public GetSerialData()
	{
		// TODO Auto-generated constructor stub
	}
	private static Novus globalNovus = null;
	private static Search globalSearch = null;
	private Connection con = null;
	private PreparedStatement pStmt = null;
	private PreparedStatement pUpdateCompStmt = null;
	private PreparedStatement pUpdateCollectStmt = null;
	private PreparedStatement pUpdateCollectNullPubCodeStmt = null;
	private PreparedStatement pInsertDoc = null;
	private PreparedStatement pDeleteDoc = null;
	private PreparedStatement pInsertDHE = null;
	private PreparedStatement pUpdateDHE = null;
	private PreparedStatement pDeleteDHE = null;
	private PreparedStatement pInsertRFD = null;
	private PreparedStatement pDeleteRFD = null;
	private PreparedStatement pDeleteAnyRFD = null;
	private PreparedStatement pInsertRF = null;
	private PreparedStatement pPubWestValQuery = null;
	private PreparedStatement pDeleteRF = null;
	private PreparedStatement pStatPubStmt = null;
	private PreparedStatement pStatPubColStmt = null;
	private PreparedStatement pUpdateCitePV = null;
	private PreparedStatement pUpdateCite = null;
	private PreparedStatement pALRUpdateCite = null;
	private PreparedStatement pALRCitePubCodes = null;
	private PreparedStatement pUpdateALRCite = null;
	private PreparedStatement pInsertNullPubCodeDHE = null;
	private PreparedStatement pUpdateNullPubCodeDHE = null;
	private PreparedStatement pUpdateDoc = null;
	private PreparedStatement pDeleteRFDByCaseUuid = null;
	private PreparedStatement pDeleteRFDByDocUuid = null;
	private PreparedStatement pDeleteRFByCaseUuid = null;
	private PreparedStatement pDeleteDHEByCaseUuid = null;
	private PreparedStatement pDeleteDocByCaseUuid = null;
	private PreparedStatement pAnyStatPubColStmt = null;
	private PreparedStatement pAnyStatPubStmt = null;
	private PreparedStatement pAFTRCiteRFUuidByCaseUuid = null;
	private PreparedStatement pFindPubCodeFromPubColl = null; 
	private PreparedStatement pFindPubCodeFromStatPubColl = null;

	private static String findPubCodeFromPubCollQuery = "select pc.publication_code " + 
			"from machv.novus_collection nc " + 
			"left join machv.publication_collection pc " + 
			"on nc.collection_code = pc.collection_code " + 
			"where nc.collection_name = ?";

	private static String findPubCodeFromStatPubCollQuery = "select pc.publication_code " + 
			"from machv.novus_collection nc " + 
			"left join machv.status_publication_collection pc " + 
			"on nc.collection_code = pc.collection_code " + 
			"where nc.collection_name = ?";

	private static String PROD_CONN_STRING = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=machv-prod-prm-fsfo.int.thomsonreuters.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=machv-prod-stb-fsfo.int.thomsonreuters.com)(PORT=1521)))(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=machv_prod.int.thomsonreuters.com)))";
	//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
	private static String machVDHEQuery = "select host_env_code, loaded_location, prism_clip_date, publication_Code from machv.doc_hosting_env where doc_uuid = ?";
	private static String machVDHEUpdateCompQuery = "update machv.doc_hosting_env set  loaded_flag = 'Y', loaded_location = ? where host_env_code = 10 and doc_uuid = ?";
	private static String machVDHEUpdateCollQuery = "update machv.doc_hosting_env set loaded_flag = 'Y', loaded_location = ?, prism_clip_date = ?, publication_code = ? where host_env_code = 20 and doc_uuid = ?";
	private static String machVDHEUpdateNullPubCodeCollQuery = "update machv.doc_hosting_env set loaded_flag = 'Y', loaded_location = ?, prism_clip_date = ? where host_env_code = 20 and doc_uuid = ?";
	private static String machVDHEInsertQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) values (?,?,?,?,?,?,?)";
//	private static String machVDHENoPubInsertQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,PRISM_CLIP_DATE) 	values (?,?,?,?,?)";
	private static String machVDHEUpdateQuery = "update MACHV.DOC_HOSTING_ENV set LOADED_FLAG=?,LOADED_LOCATION = ?,PRISM_CLIP_DATE=?, PUBLICATION_CODE=?, ADDED_DATE=? WHERE DOC_UUID=? AND HOST_ENV_CODE=?";
	private static String machVDHEUpdateNullPubCodeQuery = "update MACHV.DOC_HOSTING_ENV set LOADED_FLAG=?,LOADED_LOCATION = ?,PRISM_CLIP_DATE=?,ADDED_DATE=? WHERE DOC_UUID=? AND HOST_ENV_CODE=?";
	private static String machVDHEInsertNullPubCodeQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE) values (?,?,?,?,?,?)";
	private static String machVDHEDeleteQuery = "delete from machv.doc_hosting_env where doc_uuid = ?";
	private static String machVDHEDeleteByCaseUuidQuery = "delete from machv.doc_hosting_env where doc_uuid in (select doc_uuid from machv.rendition_family_document where rf_uuid in (select rf_uuid from machv.rendition_family where case_uuid = ?))";
	private static String machVExistingDocDataQuery = "select cite.citation_volume, cite.publication_code, cite.citation_page, dhe.doc_uuid, dhe.loaded_flag, dhe.loaded_location from machv.citation cite left join machv.rendition_family_document rfd on cite.rf_uuid = rfd.rf_uuid left join machv.doc_hosting_env dhe on rfd.doc_uuid = dhe.doc_uuid where cite.case_uuid = ? order by cite.publication_code desc";
	
	private static String machVDocInsertQuery = "Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,LANGUAGE,DOC_JMA_FLAG,CREATE_DATE) values (?,?,null,'N',null)";
	private static String machVDocUpdateQuery = "Update MACHV.DOCUMENT SET DOC_UUID = ?, FORMAT = ? WHERE DOC_UUID = ?";
	private static String machVDocDeleteQuery = "Delete from machv.document where doc_uuid = ?";
	private static String machVDocDeleteByCaseUuidQuery = "delete from machv.document where doc_uuid in (select doc_uuid from machv.rendition_family_document where rf_uuid in (select rf_uuid from machv.rendition_family where case_uuid = ?))";
	
	private static String machVRFDInsertQuery = "Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values (?, ?)";
	private static String machVRFDDeleteQuery = "delete from machv.rendition_family_document where rf_uuid = ? and doc_uuid = ?";
	private static String machVRFDDeleteAnyQuery = "delete from machv.rendition_family_document where doc_uuid = ?";
	private static String machVRFDDeleteByCaseUuidQuery = "delete from machv.rendition_family_document where rf_uuid in (select rf_uuid from machv.rendition_family where case_uuid = ?)";
	private static String machVRFDDeleteByDocUuidQuery = "delete from machv.rendition_family_document where doc_uuid = ?";

	private static String machVRFInsertQuery = "Insert into MACHV.RENDITION_FAMILY (RF_UUID, CASE_UUID) values (?,?)";
	private static String machVRFDeleteQuery = "delete from machv.rendition_family where rf_uuid =? and case_uuid = ?";
	private static String machVRFDeleteByCaseUuidQuery = "delete from machv.rendition_family where case_uuid = ?";
	
	private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
	private static String machCiteWVolAndPageUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ? AND CITATION_VOLUME = ? AND CITATION_PAGE = ?";
	private static String machALRCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = NULL WHERE CASE_UUID = ?";
	private static String getCitePubCodeQuery = "select publication_code from machv.citation where case_uuid = ?";
	
	private static String machVStatusPubColMappingQuery = "select status_number, publication_code " + 
			"from machv.status_publication_collection " + 
			"where publication_code = ? " +
			"and status_number = " + 
			"(select status_number from machv.status_lookup where status_desc = ?)";

	private static String machVStatusPubMappingQuery = "select status_number, publication_code " + 
			"from machv.status_publication " + 
			"where publication_code = ? " + 
			"and status_number = " + 
			"(select status_number from machv.status_lookup where status_desc = ?)";
	private static String machVAnyStatusPubColMappingQuery = "select status_number, publication_code " + 
			"from machv.status_publication_collection " + 
			"where publication_code = ? "; // +
//			"and status_number = " + 
//			"(select status_number from machv.status_lookup where status_desc = ?)";

	private static String machVAnyStatusPubMappingQuery = "select status_number, publication_code " + 
			"from machv.status_publication " + 
			"where publication_code = ? "; // + 
//			"and status_number = " + 
//			"(select status_number from machv.status_lookup where status_desc = ?)";
	private static String machVCaseAFTRCiteQuery = "select rf_uuid " +
			"from machv.citation " + 
			"where case_uuid = ? " + 
			"and publication_code in (897, 863)";
	
	public static String crlf = System.getProperty("line.separator");
	private String metadata = "";
	//private Collection<String> rfUuidCollect = new Vector<String>();
	private Collection<NovusCiteData> firstCiteDataCollect = new Vector<NovusCiteData>();
	private Collection<NovusCiteData> citeDataCollect = new Vector<NovusCiteData>();
	
	public Boolean REPORT_ONLY = Boolean.FALSE;
	//public Boolean REPORT_ONLY = Boolean.TRUE;
	public Boolean REPORT_NOT_ON_NOVUS_SERIALS_ONLY = Boolean.FALSE;
	boolean hasNovus = false; 
	/**
	 * @param args
	 * When Main is ran - the file TestCaseUuidList.txt in c:\Data will be consumed.
	 * for each caseUuid in the file, a request will be sent to Novus to pull the data contained in novus
	 * PRE CHANGE data will be written to MachVAndNovusLoadData.txt
	 * containing cite (vol:pubCode:page), docUuid, loadedFlag, loadedLocation 
	 * So - if anything gets removed that shouldn't be - we can always revert back to the prechange.
	 * if the NovusData contains star page info - the data will be written to StarpagedSerials.txt
	 */
	public static void main(String[] args)
	{

		GetSerialData gsd = new GetSerialData();

		//		String serialNum = "2050342136";
		////		String dfUuid = "Idf3ce81d4d8911ea890bc662fc86604c ";
		//		System.out.println("Report the data found for serial:" + serialNum);
		//		gsd.reportSerialData(serialNum); 

		GetSerialData currGSD = new GetSerialData();
		try
		{
			System.out.println(currGSD.getClass().getName() + ".main() method entry!");
			//			File currFile = new File("c:\\data\\FedAndDLUpd10DigitSerials.txt");
			//			File currFile = new File("c:\\data\\FedAndDLUpd10DigitSerialsAddedSerials.txt");

//			File currFile = new File("c:\\data\\TestSerialList.txt");
			File currFile = new File("c:\\data\\TestCaseUuidList.txt");
			//			File currFile = new File("c:\\data\\TestFed3SerialList.txt");
//			File currFile = new File("c:\\data\\TestAWMIDReclass.txt");

			//			File currFile = new File("c:\\data\\NotFixedInFirstFiles.txt");
			//			File currFile = new File("c:\\data\\DomesticNonJ10DigitSerials.txt");
			//			File currFile = new File("c:\\data\\DomesticSerialsTouchedByANZ.txt");

			BufferedReader reader = null;
			currGSD.con = GetSerialData.getSimpleConnection();
			currGSD.pStmt = currGSD.con.prepareStatement(GetSerialData.machVDHEQuery);
			reader = new BufferedReader(new FileReader(currFile));

			// setup, set queue criteria and create a PIT
//			globalNovus = new Novus();
//			//			novus.setQueueCriteria("NOVUS1A", "qc");
//			globalNovus.setQueueCriteria(null, "prod");
//			globalNovus.createPit();
//			globalNovus.setResponseTimeout(30000);
//			globalNovus.setProductName("Keycite-NIMS:Names_Audit");
//			globalNovus.setBusinessUnit("Legal");
//
//			globalSearch = globalNovus.getSearch();
//			Collection<String> collectionCollect = new Vector<String>();
//			collectionCollect.addAll(GetSerialData.makeCollectionCollection());
//			for (String currCollect : collectionCollect)
//			{
//				globalSearch.addCollection(currCollect);
//			}
//			// serach by specific collection (is more like 'w_cs_so2')
//			//search.addCollection("N_DUSSCT");
//
//			// search by collection set
//			//			search.addCollectionSet("N_DUSSCT");
//
//			globalSearch.setQueryType(globalSearch.BOOLEAN);
//			globalSearch.setSyntaxType(Search.NATIVE);
//			globalSearch.setDocumentLimit(1000);
//			globalSearch.setUseQueryWarnings(false);
//			globalSearch.setHighlightFlag(false);
//			globalSearch.setExpandNormalizedTerms(true);
//			globalSearch.setExactMatch(false);
//			globalSearch.setIgnoreStopwords(false);
//			globalSearch.setDuplicationFiltering(false);
			String currentSerial = reader.readLine();
			while(currentSerial != null)
			{
				int docCount = 0;
				//String novusLoadInfo = currGSD.reportThisSerialData(currentSerial);
				NovusData nd = null; 
				//nd = currGSD.reportThisSerialData(currentSerial);
				nd = currGSD.reportThisSerialDataReEntrant(currentSerial);
				int idx = currGSD.metadata.indexOf(STAR_PAGE);
				String allMachVDocInfo = "";
				boolean hasRfUuids = false;
				if (idx != -1)
					hasRfUuids = hasRfUuids(nd);
				else
					hasRfUuids = true;
				System.out.println("idx=" + idx);
				allMachVDocInfo = currGSD.getExistingMachVDocData(currentSerial);
				System.out.println(allMachVDocInfo);
//				if (currGSD.metadata.indexOf(STAR_PAGE) == -1)
//				if ((idx == -1) && (hasRfUuids))
				if (hasRfUuids)
				{
					//Collection<String> novusLoadInfoCollect = new Vector<String>();
					Collection<NovusDocData> novusLoadInfoCollect = new Vector<NovusDocData>();
					novusLoadInfoCollect.clear();
					//novusLoadInfoCollect.addAll(currGSD.makeNovusLoadCollect(novusLoadInfo));
					novusLoadInfoCollect.addAll(nd.getNDocDataCollect());
					String preFixMachVLoadInfo = "";
					String postFixMachVLoadInfo = "";
					String fullNovusLoadInfo = "";
					// report out only when more than two docs are on the serial 
					System.out.println("currentSerial:" + currentSerial + " has docCount=" + novusLoadInfoCollect.size());
					if ((novusLoadInfoCollect != null) && (!novusLoadInfoCollect.isEmpty()))
					{
						//if (novusLoadInfoCollect.size() == 1)
						//if (novusLoadInfoCollect.size() > 1)
						if (novusLoadInfoCollect.size() >= 1)
						{
							boolean firstDoc = true;
							for (NovusDocData currNovusLoadInfo : novusLoadInfoCollect)
							{
								++docCount;
								fullNovusLoadInfo += currNovusLoadInfo;
								//								if (currNovusLoadInfo.compareTo(NO_NOVUS_DOCS_RETURNED) != 0)
								//								{
								if (!currGSD.REPORT_NOT_ON_NOVUS_SERIALS_ONLY)
								{
									String machVNovusDocLoadInfo = currGSD.getMachVLoadData(currNovusLoadInfo);
									//										preFixMachVLoadInfo += "Pre-Fix - " + currGSD.getMachVLoadData(currNovusLoadInfo) + crlf;
									preFixMachVLoadInfo += "Pre-Fix - " + machVNovusDocLoadInfo + crlf;

									if (!currGSD.REPORT_ONLY)// && (machVHasDoc(preFixMachVLoadInfo)))
									{
										currGSD.fixMachVLoadData(currNovusLoadInfo, machVNovusDocLoadInfo, firstDoc);
										firstDoc = false;
										postFixMachVLoadInfo += "Post-Fix - " + currGSD.getMachVLoadData(currNovusLoadInfo) + crlf;
									}
									else
									{
										// grab the dfUuid, and docUuid - write these to an output file.
										currGSD.writeToLoadedFile(currNovusLoadInfo, nd);
									}
								}
								//								}
								//								else
								//								{
								//									preFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
								//									postFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
								//								}
							}
							novusLoadInfoCollect.clear();
						}
						else
						{
							System.out.println("Wait here - there are multiple docs on this serial - we need to walk through this process with serialNumber=" + currentSerial);
						}
					}
					else
					{
						preFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
						postFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
						currGSD.writeToNoDocsLoaded(currentSerial);
					}
					if ((docCount >= 1) && (!currGSD.REPORT_ONLY))
						currGSD.writeToReportFile(fullNovusLoadInfo, preFixMachVLoadInfo, postFixMachVLoadInfo, currentSerial, allMachVDocInfo);
					//					}
					// done report out only when more than two docs are on the serial
					//					for (String currNovusLoadInfo : novusLoadInfoCollect)
					//					{
					//						fullNovusLoadInfo += currNovusLoadInfo;
					//						if (novusLoadInfo.compareTo(NO_NOVUS_DOCS_RETURNED) != 0)
					//						{
					//							preFixMachVLoadInfo += "Pre-Fix - " + currGSD.getMachVLoadData(currNovusLoadInfo) + crlf;
					//							//currGSD.fixMachVLoadData(currNovusLoadInfo, preFixMachVLoadInfo);
					//							//postFixMachVLoadInfo += "Post-Fix - " + currGSD.getMachVLoadData(currNovusLoadInfo) + crlf;
					//						}
					//						else
					//						{
					//							preFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
					//							postFixMachVLoadInfo = "Can't query MachV without a docUuid from Novus" + crlf;
					//						}
					//					}
					//					currGSD.writeToReportFile(fullNovusLoadInfo, preFixMachVLoadInfo, postFixMachVLoadInfo, currentSerial);
					fullNovusLoadInfo = null;
					novusLoadInfoCollect.clear();
					currGSD.firstCiteDataCollect.clear();
					currGSD.citeDataCollect.clear();

				}
				else
				{
					currGSD.writeToStarPageFile(currentSerial);
				}
				
				currentSerial = reader.readLine();
			}
			if (reader != null)
			{
				reader.close();
				reader = null;
			}
			currGSD.closeResources(currGSD.pStmt, null, currGSD.con);					

		}
		catch (Exception e)
		{
			e.printStackTrace();
			String msg = currGSD.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
				+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}
		if (globalNovus != null)
			globalNovus.shutdownMQ();

		//gsd.reportDfUuidData(dfUuid);
		System.out.println("Done");
		System.out.println();
		//		System.out.println("Report the data found for dfUuid:" + dfUuid);
		//		gsd.reportDfUuidData(dfUuid);

	}
	
	private String getExistingMachVDocData(String currentSerial)
	{
		String retStr = "";
		//String docUuid = this.getDocUuidFromNouvsLoadData(novusLoadData);
//		private static String machVDHEQuery = "select host_env_code, loaded_location, prism_clip_date, publication_Code from machv.doc_hosting_env where doc_uuid = ?";

		String novusLoadLoc = "";
		String wlLoadLoc = "";
		Date clipDateVal = null;
		int pubCode = 0;
		try
		{
//			if (this.pStmt == null)
				this.pStmt = this.con.prepareStatement(GetSerialData.machVExistingDocDataQuery);
			this.pStmt.setString(1, currentSerial);
			ResultSet rs = this.pStmt.executeQuery();
			boolean docExists = false;
			while (rs.next())
			{
				// vol
				String vol = rs.getString(1);
				// pub code
				int pCode = rs.getInt(2);
				String spubCode = new Integer(pCode).toString();
				// page
				String page = rs.getString(3); 
				// docUuid
				String docUuid = rs.getString(4);
				// loadedFlag
				String loadedFlag = rs.getString(5);
				// loadedLocation
				String loadedLoc = rs.getString(6);

				retStr += vol + ":" + spubCode + ":" + page + " docUuid:" + docUuid + " loaded:" + loadedFlag + " loadedLoc:" + loadedLoc + crlf;
			}
			//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
			this.closeResources(this.pStmt, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "getExistingMachVDocData()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
		}
		return retStr;
	}

	private synchronized static boolean hasRfUuids(NovusData nd)
	{
		boolean retVal = false;
		Collection<NovusDocData> docCollect = nd.getNDocDataCollect();
		for (NovusDocData novusDocData : docCollect)
		{
			Collection<NovusCiteData> ncd = novusDocData.getNovusCiteDataCollect();
			for (NovusCiteData novusCiteData : ncd)
			{
				if (novusCiteData.getRfUuid() != null);
				retVal = true;
			}
		}
		return retVal;
	}
	public synchronized static String generateUuid() throws Exception
	{
		String uuid = null;
		try
		{
			uuid = Uuid.generateUuid();
		}
		catch (IOException e)
		{
			throw new Exception(e.getMessage());
		}
		return uuid;
	}

	/**
	 * @param currentSerial
	 */
	private static void writeToStarPageFile(String currentSerial)
	{
		// for serialNum = serialNum
		//Nouvs has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
		//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800

		File starpageFile = new File("c:\\Data\\StarpagedSerials.txt");
//		File outputFile = new File("c:\\Data\\MachVAndNovusLoadDataWhenDocCountGT1.txt");
		BufferedWriter writer = null;
		String reportLine = "SerialNumber:" + currentSerial + " contains Starpage references"+ crlf;
		try
		{
			if (!starpageFile.exists())
			{
				starpageFile.createNewFile();			
			}
			if (reportLine != null)
			{
				writer = new BufferedWriter(new FileWriter(starpageFile, true));
				writer.write(reportLine);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg =  "GetSerialData." + "writeToStarPageFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static String NO_DOC_FOUND = "does not have Document uuid:";
	/**
	 * @param preFixMachVLoadInfo
	 * @return
	 */
	private static boolean machVHasDoc(String preFixMachVLoadInfo)
	{
		boolean retVal = true;
		if (preFixMachVLoadInfo.indexOf(NO_DOC_FOUND) >= 0)
			retVal = false;
		return retVal;
	}
	/**
	 * @param novusLoadInfo
	 * @return
	 */
	private Collection<? extends String> makeNovusLoadCollect(String novusLoadInfo)
	{
		Collection<String> retCollect = new Vector<String>();
		if (multiDocNovusLoadInfo(novusLoadInfo))
		{
			// have to split up each doc line and add as a separate element
			int nextDocIdex = 1;
			int startIdex = 0;
			while (nextDocIdex > 0)
			{
				int docIdex = novusLoadInfo.indexOf(DOCUMENT_UUID, startIdex);
				nextDocIdex = novusLoadInfo.indexOf(DOCUMENT_UUID, (docIdex + DOCUMENT_UUID.length()));
				String docStr = "";
				if (nextDocIdex == -1)
					docStr = novusLoadInfo.substring(docIdex);
				else
					docStr = novusLoadInfo.substring(docIdex, nextDocIdex - 1);
				retCollect.add(docStr);
				startIdex = nextDocIdex;
			}
		}
		else
			retCollect.add(novusLoadInfo);
		return retCollect;
	}
	/**
	 * @param novusLoadInfo
	 * @return
	 */
	private boolean multiDocNovusLoadInfo(String novusLoadInfo)
	{
		int docIdex = novusLoadInfo.indexOf(DOCUMENT_UUID);
		int nextDocIdex = novusLoadInfo.indexOf(DOCUMENT_UUID, (docIdex + DOCUMENT_UUID.length()));
		if (nextDocIdex > 0)
			return true;
		return false;
	}
	/**
	 * @param novusLoadInfo
	 */
	private void fixMachVLoadData(NovusDocData currNovusLoadInfo, String machVLoadData, boolean firstDoc)
	{
		String docUuid = currNovusLoadInfo.getDocUuid();
//		String novusLoadLoc = "";
//		String wlLoadLoc = "";
//		Date clipDateVal = null;
//		int pubCode = 0;
		//if (machVHasDoc(machVLoadData))
		if (firstDoc)
		{
			//updateDoc(docUuid, currNovusLoadInfo, machVLoadData);
			if (currNovusLoadInfo.isALRData)
				cleanALRDocData(docUuid, currNovusLoadInfo);
			else
				clearDocData(docUuid, currNovusLoadInfo, machVLoadData);
		}
		//else
		if (currNovusLoadInfo.isALRData)
			insertALRDoc(docUuid, currNovusLoadInfo);
		else
			insertDoc(docUuid, currNovusLoadInfo);
			
	}

	private void clearDoc(String docUuid)
	{
		
	}
	private void clearRF (String caseUuid, String rfUuid)
	{
		
	}
	
	private void cleanALRDocData(String docUuid, NovusDocData novusLoadData)
	{
		try
		{
			if (this.pUpdateALRCite == null)
				this.pUpdateALRCite = this.con.prepareStatement(GetSerialData.machALRCiteUpdateQuery);
			// clear out the rfUuid on all the cites
			Collection<NovusCiteData> citeData = novusLoadData.getNovusCiteDataCollect();
			String caseUuid = novusLoadData.getCaseUuid();

			boolean deleteByCaseUuid = true;
			if (deleteByCaseUuid)
			{
				deleteByCaseUuid = false;
				deleteDHEByCaseUuid(caseUuid);
				deleteDocByCaseUuid(caseUuid);
				deleteRFDByCaseUuid(caseUuid);
				deleteRFDByDocUuid(docUuid);
				deleteRFByCaseUuid(caseUuid);
			}
			System.out.println("Update the Citation with the rfUuid");
			//				private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
			pUpdateALRCite.clearParameters();
			pUpdateALRCite.setString(1, caseUuid);
			pUpdateALRCite.executeUpdate();

			// no cite info on ALR Data
			//				if (currNovCiteData.isPrimary)
			//				{
			//					deleteAnyRFD(docUuid);
			//					deleteRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
			//					deleteDHE(docUuid);
			//					deleteDoc(docUuid);
			//				}
			//				else
			//				{
			//					// The parallel only doc will be built out the first time - any time after that, skip it.
			//					deleteRFD(currNovCiteData.getRfUuid(), currNovCiteData.getRfUuid());
			//					deleteRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
			//					deleteDHE(currNovCiteData.getRfUuid());
			//					deleteDoc(currNovCiteData.getRfUuid());
			//				}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
				+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}
	}
	/**
	 * @param docUuid
	 * @param currNovusLoadInfo
	 * @param machVLoadData
	 */
	private void clearDocData(String docUuid, NovusDocData novusLoadData,
			String machVLoadData)
	{
		try
		{
			if (this.pUpdateCite == null)
				this.pUpdateCite = this.con.prepareStatement(GetSerialData.machCiteUpdateQuery);
			// clear out the rfUuid on all the cites
			Collection<NovusCiteData> citeData = novusLoadData.getNovusCiteDataCollect();
			String caseUuid = "";
			boolean deleteByCaseUuid = true;
			for (NovusCiteData currNovCiteData : citeData)	
			{
				caseUuid = currNovCiteData.getCaseUuid();
				if (deleteByCaseUuid)
				{
					deleteByCaseUuid = false;
					deleteDHEByCaseUuid(caseUuid);
					deleteDocByCaseUuid(caseUuid);
					deleteRFDByCaseUuid(caseUuid);
					deleteRFDByDocUuid(docUuid);
					deleteRFByCaseUuid(caseUuid);
				}
/*
				System.out.println("Update the Citation with pubCode =" + currNovCiteData.getPubCode() + " the rfUuid");
				//				private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
				if ((currNovCiteData.getPage() != null) && (currNovCiteData.getVolume() != null))
				{
					System.out.println("Update the Citation with pubCode=" + currNovCiteData.getPubCode() + "the rfUuid=" + currNovCiteData.getRfUuid() + "  and vol=" + currNovCiteData.getVolume() + "and page=" + currNovCiteData.getPage());
					if (this.pUpdateCitePV == null)
						this.pUpdateCitePV = this.con.prepareStatement(GetSerialData.machCiteWVolAndPageUpdateQuery);
					pUpdateCitePV.clearParameters();
					pUpdateCitePV.setString(1, currNovCiteData.getRfUuid());
					pUpdateCitePV.setString(2, currNovCiteData.getCaseUuid());
					pUpdateCitePV.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
					pUpdateCitePV.setString(4, currNovCiteData.getVolume());
					pUpdateCitePV.setString(5, currNovCiteData.getPage());
					int updateCount = pUpdateCitePV.executeUpdate();
					if (updateCount > 1)
						System.out.println("why was more than one cite updated???");
				}
				else
				{
					System.out.println("Update the Citation with the rfUuid and no vol or page");
					//					private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
					if (this.pUpdateCite == null)
						this.pUpdateCite = this.con.prepareStatement(GetSerialData.machCiteUpdateQuery);
					pUpdateCite.clearParameters();
					pUpdateCite.setString(1, currNovCiteData.getRfUuid());
					pUpdateCite.setString(2, currNovCiteData.getCaseUuid());
					pUpdateCite.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
					pUpdateCite.executeUpdate();
				}
*/				
				if (currNovCiteData.isPrimary)
				{
					deleteAnyRFD(docUuid);
					deleteRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
					deleteDHE(docUuid);
					deleteDoc(docUuid);
				}
				else
				{
					// The parallel only doc will be built out the first time - any time after that, skip it.
					deleteRFD(currNovCiteData.getRfUuid(), currNovCiteData.getRfUuid());
					deleteRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
					deleteDHE(currNovCiteData.getRfUuid());
					deleteDoc(currNovCiteData.getRfUuid());
				}
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
				+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}
	}
	
	/**
	 * @param caseUuid
	 * @throws SQLException 
	 */
	private void deleteDHEByCaseUuid(String caseUuid) throws SQLException
	{
		if (this.pDeleteDHEByCaseUuid == null)
			this.pDeleteDHEByCaseUuid = this.con.prepareStatement(GetSerialData.machVDHEDeleteByCaseUuidQuery);
		pDeleteDHEByCaseUuid.clearParameters();
		pDeleteDHEByCaseUuid.setString(1, caseUuid);
		pDeleteDHEByCaseUuid.execute();
	}
	/**
	 * @param caseUuid
	 * @throws SQLException 
	 */
	private void deleteDocByCaseUuid(String caseUuid) throws SQLException
	{
		if (this.pDeleteDocByCaseUuid == null)
			this.pDeleteDocByCaseUuid = this.con.prepareStatement(GetSerialData.machVDocDeleteByCaseUuidQuery);
		pDeleteDocByCaseUuid.clearParameters();
		pDeleteDocByCaseUuid.setString(1, caseUuid);
		pDeleteDocByCaseUuid.execute();
	}
	/**
	 * @param docUuid
	 * @param novusLoadData
	 */
	private void insertALRDoc(String docUuid, NovusDocData novusLoadData)
	{
		// ALR Docs are hooked to all cites
		// ALR Docs only have a NOVUS DHE built out.
		// ALR Docs need to create a RFUuid as it is not contained in the Novus Data
		try
		{
			System.out.println("Need to get the existing MachV citation information - Then step through each cite ");
			if (this.pALRCitePubCodes == null)
				this.pALRCitePubCodes = this.con.prepareStatement(GetSerialData.getCitePubCodeQuery);
			// NEED to update the cite the same as for non-alr data, but need to pull the data from machVLoadData and generate a rfUuid
			if (this.pALRUpdateCite == null)
				this.pUpdateCite = this.con.prepareStatement(GetSerialData.machCiteUpdateQuery);
			
			// insert the doc first
//			private static String machVDocInsertQuery = "Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,LANGUAGE,DOC_JMA_FLAG,CREATE_DATE) values (?,?,null,'N',null)";
			try
			{
				insertDoc(docUuid, "TEXT");
			}
			catch (SQLIntegrityConstraintViolationException e)
			{
				updateDoc(docUuid, "TEXT");
			}
			
			// insert the DHE
//			private static String machVDHEInsertQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) values (?,?,'Y',?,null,?,?)";
			// Start with the WL DHE
			int hostEnvCode = 0;
			String loadedFlag = "N";
//			try
//			{
//				hostEnvCode = 10;
//				loadedFlag = "Y";
//				java.sql.Date clipDate = null;
//				if (novusLoadData.getPrismClipDate() != null)
//					clipDate = new java.sql.Date(getPrismDate(novusLoadData.getPrismClipDate()).getTime());
//				if (docUuid.compareTo("Ie811889071ed11d792e6e58f3e66f41c") == 0)
//					System.out.println("Stop here");
//				if (novusLoadData.getPubCode() == null)
//					insertNullPubDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), clipDate);
//				else
//					insertDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), clipDate, Integer.valueOf(novusLoadData.getPubCode()).intValue());
//			}
//			catch (SQLIntegrityConstraintViolationException e)
//			{
//				updateDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), new java.sql.Date(getPrismDate(novusLoadData.getPrismClipDate()).getTime()), Integer.valueOf(novusLoadData.getPubCode()).intValue());
//			}
			
			java.sql.Date clipDate = null;
			java.sql.Date addDate = null;
			try
			{
				// now the Novus DHE
				hostEnvCode = 20;
				loadedFlag = "Y";
				if (novusLoadData.getPrismClipDate() != null)
					clipDate = new java.sql.Date(getPrismDate(novusLoadData.getPrismClipDate()).getTime());
				if (novusLoadData.getAddedDate() != null)
					addDate = new java.sql.Date(getAddDate(novusLoadData.getAddedDate()).getTime());
				insertNullPubDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getCollection(), clipDate, addDate);
			}
			catch (SQLIntegrityConstraintViolationException e)
			{
				updateNullPubDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getCollection(), clipDate, addDate);
			}
			
			// get the citePubCodes
			String rfUuid = null;
			this.pALRCitePubCodes.clearParameters();
			this.pALRCitePubCodes.setString(1, novusLoadData.getCaseUuid());
			ResultSet rs = this.pALRCitePubCodes.executeQuery();
			while (rs.next())
			{
				int pubCode = rs.getInt(1);
				try
				{
					rfUuid = generateUuid();
				}
				catch (Exception e)
				{
					e.printStackTrace();
					String msg = this.getClass().getName() + "." + "insertALRDoc()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
					if (e.getCause() != null)
					              msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
					System.out.println(msg);
				}
				// build out a RF
				try
				{
					insertRF(rfUuid, novusLoadData.getCaseUuid());
				}
				catch (SQLIntegrityConstraintViolationException e)
				{
					; // nothing to update!
				}
				// build out a RFD
				try
				{
					insertRFD(rfUuid, docUuid);
				}
				catch (SQLIntegrityConstraintViolationException e)
				{
					; // nothing to update!
				}
				
				// Update the cite
				pUpdateCite.clearParameters();
				pUpdateCite.setString(1, rfUuid);
				pUpdateCite.setString(2, novusLoadData.getCaseUuid());
				pUpdateCite.setInt(3, new Integer(pubCode).intValue());
		System.out.println("THIS NEEDS TO BE CORRECTED - pUpdateCite needs to update the cite based on pubCode, page and vol - Exiting!");
		throw new NullPointerException("THIS NEEDS TO BE CORRECTED - pUpdateCite needs to update the cite based on pubCode, page and vol - Exiting!");
		
				/*if (novusLoadData.getVolume() != null)
					pUpdateCite.setString(4, currNovCiteData.getVolume());
				if (currNovCiteData.getPage() != null)
					pUpdateCite.setString(5, currNovCiteData.getPage());
				pUpdateCite.executeUpdate();*/
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
						+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static String AFTRDOC_COMP = "AFTRDOC";

	/**
	 * @param docUuid
	 * @param novusLoadData
	 */
	private void insertDoc(String docUuid, NovusDocData novusLoadData)
	{
		boolean isWLCite = false;
		String primaryCitePubCode = null;
		try
		{
//			if (this.pUpdateCite == null)
//			{
//				this.pUpdateCite = this.con.prepareStatement(GetSerialData.machCiteUpdateQuery);
//			}
			
			// insert the doc first
//			private static String machVDocInsertQuery = "Insert into MACHV.DOCUMENT (DOC_UUID,FORMAT,LANGUAGE,DOC_JMA_FLAG,CREATE_DATE) values (?,?,null,'N',null)";
			try
			{
				insertDoc(docUuid, "TEXT");
			}
			catch (SQLIntegrityConstraintViolationException e)
			{
				updateDoc(docUuid, "TEXT");
			}
			
			// insert the DHE
//			private static String machVDHEInsertQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) values (?,?,'Y',?,null,?,?)";
			// Start with the WL DHE
			int hostEnvCode = 0;
			String loadedFlag = "N";
			java.sql.Date clipDate = null;
			java.sql.Date addedDate = null;
			if (novusLoadData.getPrismClipDate() != null)
				clipDate = new java.sql.Date(getPrismDate(novusLoadData.getPrismClipDate()).getTime());
			if (novusLoadData.getAddedDate() != null)
				addedDate = new java.sql.Date(getAddDate(novusLoadData.getAddedDate()).getTime());
			
			try
			{
				hostEnvCode = 10;
				loadedFlag = "Y";
				if (docUuid.compareTo("Id0811c96940511d993e6d35cc61aab4a") == 0)
					System.out.println("Stop here");
				if (novusLoadData.getPubCode() == null)
					insertNullPubDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), clipDate, addedDate);
				else
					insertDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), clipDate, Integer.valueOf(novusLoadData.getPubCode()).intValue(), addedDate);
			}
			catch (SQLIntegrityConstraintViolationException e)
			{
				if (novusLoadData != null)
					updateDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getComponent(), clipDate, Integer.valueOf(novusLoadData.getPubCode()).intValue(), addedDate);
			}
			
			try
			{
				// now the Novus DHE
				hostEnvCode = 20;
				loadedFlag = "Y";
				insertDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getCollection(), clipDate, Integer.valueOf(novusLoadData.getPubCode()).intValue(), addedDate);
			}
			catch (SQLIntegrityConstraintViolationException e)
			{
				updateDhe(loadedFlag, hostEnvCode, docUuid, novusLoadData.getCollection(), clipDate, Integer.valueOf(novusLoadData.getPubCode()).intValue(), addedDate);
			}
			
			boolean isAFTRDoc = false;
			if (AFTRDOC_COMP.compareTo(novusLoadData.getComponent()) == 0)
				isAFTRDoc = true;
			
			// insert the RFDs - there maybe multiples
			//private static String machVRFDInsertQuery = "Insert into MACHV.RENDITION_FAMILY_DOCUMENT (RF_UUID,DOC_UUID) values (?, ?)";
			if (!isAFTRDoc)
			{
				Collection<NovusCiteData> citeData = novusLoadData.getNovusCiteDataCollect();
				for (NovusCiteData currNovCiteData : citeData)
				{
					if (currNovCiteData.isPrimary)
					{
						primaryCitePubCode = currNovCiteData.getPubCode();
						if (currNovCiteData.getRfUuid() != null)
						{
							try
							{
								insertRFD(currNovCiteData.getRfUuid(), docUuid);
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}

							try
							{
								insertRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}
							if ((currNovCiteData.getPage() != null) && (currNovCiteData.getVolume() != null))
							{
								System.out.println("Update the Citation with pubCode=" + currNovCiteData.getPubCode() + " the rfUuid=" + currNovCiteData.getRfUuid() + "  and vol=" + currNovCiteData.getVolume() + " and page=" + currNovCiteData.getPage());
								if (this.pUpdateCitePV == null)
									this.pUpdateCitePV = this.con.prepareStatement(GetSerialData.machCiteWVolAndPageUpdateQuery);
								//	private static String machCiteWVolAndPageUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ? AND CITATION_VOLUME = ? AND CITATION_PAGE = ?";
								pUpdateCitePV.clearParameters();
								pUpdateCitePV.setString(1, currNovCiteData.getRfUuid());
								pUpdateCitePV.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCitePV.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCitePV.setString(4, currNovCiteData.getVolume());
								pUpdateCitePV.setString(5, currNovCiteData.getPage());
								int updateCount = pUpdateCitePV.executeUpdate();
								if (updateCount != 1)
									System.out.println("why not 1 cite updated??? - is primary");
							}
							else
							{
								System.out.println("Update the Citation with the rfUuid and no vol or page");
								//					private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
								if (this.pUpdateCite == null)
									this.pUpdateCite = this.con.prepareStatement(GetSerialData.machCiteUpdateQuery);
								pUpdateCite.clearParameters();
								pUpdateCite.setString(1, currNovCiteData.getRfUuid());
								pUpdateCite.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCite.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCite.executeUpdate();
							}
						}
					}
					else if (currNovCiteData.isParallelOnly)
					{
						// The parallel only doc will be built out the first time - any time after that, skip it.
						if (currNovCiteData.getRfUuid() != null)
						{
							System.out.println("Build out parallel only doc, dhe, rfd");
							if ((currNovCiteData.getPubCode() != null) && (isPrintOnlyPubCode(currNovCiteData.getPubCode())))
							{
								// if this is a print only publication, then set the doc format to TEXT 
								try
								{
									insertDoc(currNovCiteData.getRfUuid(), "TEXT");
								}
								catch (SQLIntegrityConstraintViolationException e)
								{
									updateDoc(currNovCiteData.getRfUuid(), "TEXT");
								}
							}
							else
								
							{
								// if this is a Parallel Only doc then set the format to PARALLEL ONLY
								try
								{
//									insertDoc(currNovCiteData.getRfUuid(), "PARALLEL ONLY");
									insertDoc(currNovCiteData.getRfUuid(), "TEXT");
								}
								catch (SQLIntegrityConstraintViolationException e)
								{
//									updateDoc(currNovCiteData.getRfUuid(), "PARALLEL ONLY");
									updateDoc(currNovCiteData.getRfUuid(), "TEXT");
								}
							}

							String loadLocation = null;
							clipDate = null;
							addedDate = null;
							try
							{
								hostEnvCode = 10;
								loadedFlag = "N";
								insertDheNoPub(loadedFlag, hostEnvCode, currNovCiteData.getRfUuid(), loadLocation, clipDate, addedDate);
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								updateDheNoPub(loadedFlag, hostEnvCode, currNovCiteData.getRfUuid(), loadLocation, clipDate, addedDate);
							}
							
							try
							{
								hostEnvCode = 20;
								insertDheNoPub(loadedFlag, hostEnvCode, currNovCiteData.getRfUuid(), loadLocation, clipDate, addedDate);
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								updateDheNoPub(loadedFlag, hostEnvCode, currNovCiteData.getRfUuid(), loadLocation, clipDate, addedDate);
							}

							try
							{
								insertRFD(currNovCiteData.getRfUuid(), currNovCiteData.getRfUuid());
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}

							try
							{
								insertRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}
							if ((currNovCiteData.getPage() != null) && (currNovCiteData.getVolume() != null))
							{
								System.out.println("Update the Citation with pubCode=" + currNovCiteData.getPubCode() + " the rfUuid=" + currNovCiteData.getRfUuid() + "  and vol=" + currNovCiteData.getVolume() + " and page=" + currNovCiteData.getPage());
								if (this.pUpdateCitePV == null)
									this.pUpdateCitePV = this.con.prepareStatement(GetSerialData.machCiteWVolAndPageUpdateQuery);
								//	private static String machCiteWVolAndPageUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ? AND CITATION_VOLUME = ? AND CITATION_PAGE = ?";
								pUpdateCitePV.clearParameters();
								pUpdateCitePV.setString(1, currNovCiteData.getRfUuid());
								pUpdateCitePV.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCitePV.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCitePV.setString(4, currNovCiteData.getVolume());
								pUpdateCitePV.setString(5, currNovCiteData.getPage());
								int updateCount = pUpdateCitePV.executeUpdate();
								if (updateCount != 1)
									System.out.println("why not 1 cite updated??? - IsParallelCite");
							}
							else
							{
								System.out.println("Update the Citation with the rfUuid and no vol or page");
								//					private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
								pUpdateCite.clearParameters();
								pUpdateCite.setString(1, currNovCiteData.getRfUuid());
								pUpdateCite.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCite.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCite.executeUpdate();
							}
						}
					}
					else
					{
						// not primary and not parallel
						//fif ((currNovCiteData.getRfUuid() != null) && (citeData.size() == 1))
						if (currNovCiteData.getRfUuid() != null) 
						{
							System.out.println("Strop on this one!!!");
							System.out.println("it is a feed skip error to have a cite ref a renditionFamilyUuid which does not exist in the renditionFamily table.");
							try
							{
								insertRF(currNovCiteData.getRfUuid(), currNovCiteData.getCaseUuid());
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}
							
							if ((currNovCiteData.getPubCode() != null) && (currNovCiteData.getPubCode().compareTo("999") == 0))
							{
								// if primaryCitePubCode is not yet populated - then find it.
								if (primaryCitePubCode == null)
									primaryCitePubCode = getPrimaryCitePubCode(novusLoadData);

								// if the primaryCitePubCode hsa pubWestCode = 'W'
								if (isPubWestCodeW(primaryCitePubCode))
								{
									// then build out the rfd for this document
									try
									{
										insertRFD(currNovCiteData.getRfUuid(), docUuid);
									}
									catch (SQLIntegrityConstraintViolationException e)
									{
										; // nothing to update!
									}
								}
							}
							// This was turned off before - maybe we shouldn't do this? - should be updating the cite - but not creating the rfd, doc and dhe
							if ((currNovCiteData.getPage() != null) && (currNovCiteData.getVolume() != null))
							{
								System.out.println("Update the Citation with pubCode=" + currNovCiteData.getPubCode() + " the rfUuid=" + currNovCiteData.getRfUuid() + "  and vol=" + currNovCiteData.getVolume() + " and page=" + currNovCiteData.getPage());
								if (this.pUpdateCitePV == null)
									this.pUpdateCitePV = this.con.prepareStatement(GetSerialData.machCiteWVolAndPageUpdateQuery);
								//	private static String machCiteWVolAndPageUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ? AND CITATION_VOLUME = ? AND CITATION_PAGE = ?";
								pUpdateCitePV.clearParameters();
								pUpdateCitePV.setString(1, currNovCiteData.getRfUuid());
								pUpdateCitePV.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCitePV.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCitePV.setString(4, currNovCiteData.getVolume());
								pUpdateCitePV.setString(5, currNovCiteData.getPage());
								int updateCount = pUpdateCitePV.executeUpdate();
								if (updateCount != 1)
									System.out.println("why not 1 cite updated??? - Is not paralle and not primary");
							}
							else
							{
								System.out.println("Update the Citation with the rfUuid and no vol or page");
								//					private static String machCiteUpdateQuery = "Update MACHV.CITATION SET RF_UUID = ? WHERE CASE_UUID = ? AND PUBLICATION_CODE = ?";
								pUpdateCite.clearParameters();
								pUpdateCite.setString(1, currNovCiteData.getRfUuid());
								pUpdateCite.setString(2, currNovCiteData.getCaseUuid());
								pUpdateCite.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
								pUpdateCite.executeUpdate();
							}

							/* TEMP turn this off!
							try
							{
								insertRFD(currNovCiteData.getRfUuid(), docUuid);
							}
							catch (SQLIntegrityConstraintViolationException e)
							{
								; // nothing to update!
							}
					
							System.out.println("Update the Citation with the rfUuid");
							pUpdateCite.clearParameters();
							pUpdateCite.setString(1, currNovCiteData.getRfUuid());
							pUpdateCite.setString(2, currNovCiteData.getCaseUuid());
							pUpdateCite.setInt(3, new Integer(currNovCiteData.getPubCode()).intValue());
							pUpdateCite.executeUpdate();
	*/
						}
					}
				}
			}
			else
			{
				// isAFTRDoc == true
				// get the rfUuid from the AFTR Cite Info for this caseUuid

//				private PreparedStatement pAFTRCiteRFUuidByCaseUuid = null;
//				private static String machVCaseAFTRCiteQuery = "select rf_uuid " +
//						"from machv.citation " + 
//						"where case_uuid = ? " + 
//						"and publication_code in (897, 863)";
				if (this.pAFTRCiteRFUuidByCaseUuid == null)
					this.pAFTRCiteRFUuidByCaseUuid = this.con.prepareStatement(GetSerialData.machVCaseAFTRCiteQuery);
				novusLoadData.getCaseUuid();

				this.pAFTRCiteRFUuidByCaseUuid.clearParameters();
				this.pAFTRCiteRFUuidByCaseUuid.setString(1, novusLoadData.getCaseUuid());
				ResultSet rs = this.pAFTRCiteRFUuidByCaseUuid.executeQuery();
				while (rs.next())
				{
					String rfUuid = rs.getString(1);
					try
					{
						insertRFD(rfUuid, docUuid);
					}
					catch (SQLIntegrityConstraintViolationException e)
					{
						; // nothing to update!
					}

					try
					{
						insertRF(rfUuid, novusLoadData.getCaseUuid());
					}
					catch (SQLIntegrityConstraintViolationException e)
					{
						; // nothing to update!
					}
				}
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
						+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}
		
	}
	private static HashMap<String, String> PRINT_ONLY_PUB_CODES = null;
	private static void fillPrintOnlyPubCodeMap()
	{
		PRINT_ONLY_PUB_CODES = new HashMap<String, String>();
		PRINT_ONLY_PUB_CODES.put("156", "156");
		PRINT_ONLY_PUB_CODES.put("431", "431");
		PRINT_ONLY_PUB_CODES.put("536", "536");
		PRINT_ONLY_PUB_CODES.put("537", "537");
		PRINT_ONLY_PUB_CODES.put("583", "583");
		PRINT_ONLY_PUB_CODES.put("590", "590");
		PRINT_ONLY_PUB_CODES.put("591", "591");
		PRINT_ONLY_PUB_CODES.put("623", "623");
		PRINT_ONLY_PUB_CODES.put("624", "624");
		PRINT_ONLY_PUB_CODES.put("630", "630");
		PRINT_ONLY_PUB_CODES.put("633", "633");
		PRINT_ONLY_PUB_CODES.put("634", "634");
		PRINT_ONLY_PUB_CODES.put("651", "651");
		PRINT_ONLY_PUB_CODES.put("705", "705");
		PRINT_ONLY_PUB_CODES.put("791", "791");
		PRINT_ONLY_PUB_CODES.put("914", "914");
		PRINT_ONLY_PUB_CODES.put("994", "994");
		PRINT_ONLY_PUB_CODES.put("995", "995");
		PRINT_ONLY_PUB_CODES.put("996", "996");
		PRINT_ONLY_PUB_CODES.put("4358", "4358");
		PRINT_ONLY_PUB_CODES.put("7458", "7458");
	}
	private boolean isPrintOnlyPubCode(String pubCode)
	{
		boolean retVal = false;
		if ((GetSerialData.PRINT_ONLY_PUB_CODES == null) ||
				((GetSerialData.PRINT_ONLY_PUB_CODES != null) && (GetSerialData.PRINT_ONLY_PUB_CODES.isEmpty())))
			GetSerialData.fillPrintOnlyPubCodeMap();
		return GetSerialData.PRINT_ONLY_PUB_CODES.containsKey(pubCode);
	}

	/**
	 * @param novusLoadData
	 * @return
	 */
	private String getPrimaryCitePubCode(NovusDocData novusLoadData)
	{
		String retVal = null;
		Collection<NovusCiteData> citeDataCollect = novusLoadData.getNovusCiteDataCollect();
		for (NovusCiteData currNovCData : citeDataCollect)
		{
			if (currNovCData.isPrimary)
				retVal = currNovCData.getPubCode();
		}
		return retVal;
	}
	private static String pubWestValQuery = "select publication_code from machv.publication_lookup where pub_west_code = 'W' and publication_code = ?";

/**
	 * @param primaryCitePubCode
	 * @return
 * @throws SQLException 
	 */
	private boolean isPubWestCodeW(String primaryCitePubCode) throws SQLException
	{
		boolean retVal = false;
		if (this.pPubWestValQuery == null)
			this.pPubWestValQuery = this.con.prepareStatement(GetSerialData.pubWestValQuery);
		
		int pubCode = Integer.valueOf(primaryCitePubCode).intValue();
		pPubWestValQuery.setInt(1, pubCode);
		ResultSet rs = this.pPubWestValQuery.executeQuery();
		if ((rs != null) && rs.next())
				retVal = true;
		return retVal;
	}
/**
	 * @param rfUuid
	 * @param caseUuid
 * @throws SQLException 
	 */
	private void insertRF(String rfUuid, String caseUuid) throws SQLException
	{
		if (this.pInsertRF == null)
			this.pInsertRF = this.con.prepareStatement(GetSerialData.machVRFInsertQuery);

		System.out.println("Build out the RF");
		pInsertRF.clearParameters();
		pInsertRF.setString(1, rfUuid);
		pInsertRF.setString(2, caseUuid);
		pInsertRF.execute();
	}
	
	/**
	 * @param rfUuid
	 * @param caseUuid
 * @throws SQLException 
	 */
	private void deleteRF(String rfUuid, String caseUuid) throws SQLException
	{
		if (this.pDeleteRF == null)
			this.pDeleteRF = this.con.prepareStatement(GetSerialData.machVRFDeleteQuery);

		System.out.println("Delete the RF");
		pDeleteRF.clearParameters();
		pDeleteRF.setString(1, rfUuid);
		pDeleteRF.setString(2, caseUuid);
		pDeleteRF.execute();
	}
/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void insertRFD(String rfUuid, String docUuid) throws SQLException
	{
		if (this.pInsertRFD == null)
			this.pInsertRFD = this.con.prepareStatement(GetSerialData.machVRFDInsertQuery);
		System.out.println("Build out rfd");
		pInsertRFD.clearParameters();
		pInsertRFD.setString(1, rfUuid);
		pInsertRFD.setString(2, docUuid);
		pInsertRFD.execute();
	}
	/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteAnyRFD(String docUuid) throws SQLException
	{
		if (this.pDeleteAnyRFD == null)
			this.pDeleteAnyRFD = this.con.prepareStatement(GetSerialData.machVRFDDeleteAnyQuery);
		System.out.println("delete rfd");
		pDeleteAnyRFD.clearParameters();
		pDeleteAnyRFD.setString(1, docUuid);
		pDeleteAnyRFD.execute();
	}

	/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteRFD(String rfUuid, String docUuid) throws SQLException
	{
		if (this.pDeleteRFD == null)
			this.pDeleteRFD = this.con.prepareStatement(GetSerialData.machVRFDDeleteQuery);
		System.out.println("delete rfd");
		pDeleteRFD.clearParameters();
		pDeleteRFD.setString(1, rfUuid);
		pDeleteRFD.setString(2, docUuid);
		pDeleteRFD.execute();
	}
	
	/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteRFDByCaseUuid(String caseUuid) throws SQLException
	{
		if (this.pDeleteRFDByCaseUuid == null)
			this.pDeleteRFDByCaseUuid = this.con.prepareStatement(GetSerialData.machVRFDDeleteByCaseUuidQuery);
		pDeleteRFDByCaseUuid.clearParameters();
		pDeleteRFDByCaseUuid.setString(1, caseUuid);
		pDeleteRFDByCaseUuid.execute();
	}
	
	/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteRFDByDocUuid(String docUuid) throws SQLException
	{
		if (this.pDeleteRFDByDocUuid == null)
			this.pDeleteRFDByDocUuid = this.con.prepareStatement(GetSerialData.machVRFDDeleteByDocUuidQuery);
		pDeleteRFDByDocUuid.clearParameters();
		pDeleteRFDByDocUuid.setString(1, docUuid);
		pDeleteRFDByDocUuid.execute();
	}
	/**
	 * @param rfUuid
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteRFByCaseUuid(String caseUuid) throws SQLException
	{
		if (this.pDeleteRFByCaseUuid == null)
			this.pDeleteRFByCaseUuid = this.con.prepareStatement(GetSerialData.machVRFDDeleteByCaseUuidQuery);
		pDeleteRFByCaseUuid.clearParameters();
		pDeleteRFByCaseUuid.setString(1, caseUuid);
		pDeleteRFByCaseUuid.execute();
	}

/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void insertDheNoPub(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, java.sql.Date addDate) throws SQLException
	{
		if (this.pInsertNullPubCodeDHE == null)
			this.pInsertNullPubCodeDHE = this.con.prepareStatement(GetSerialData.machVDHEInsertNullPubCodeQuery);

//		private static String machVDHEInsertNullPubCodeQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE) values (?,?,?,?,null,?)";
		pInsertNullPubCodeDHE.clearParameters();
		pInsertNullPubCodeDHE.setInt(1, hostEnvCode);
		pInsertNullPubCodeDHE.setString(2, docUuid);
		pInsertNullPubCodeDHE.setString(3, loadedFlag);
		pInsertNullPubCodeDHE.setString(4, loadLocation);
		pInsertNullPubCodeDHE.setDate(5,  addDate);
		pInsertNullPubCodeDHE.setDate(6,  clipDate);
		pInsertNullPubCodeDHE.execute();
	}
	/**
	 * @param docUuid
 * @throws SQLException 
	 */
	private void deleteDHE(String docUuid) throws SQLException
	{
		if (this.pDeleteDHE == null)
			this.pDeleteDHE = this.con.prepareStatement(GetSerialData.machVDHEDeleteQuery);

		//		private static String machVDHEInsertNullPubCodeQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE) values (?,?,?,?,null,?)";
		pDeleteDHE.clearParameters();
		pDeleteDHE.setString(1, docUuid);
		pDeleteDHE.execute();
	}
/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void updateDheNoPub(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, java.sql.Date addDate) throws SQLException
	{
		if (this.pUpdateNullPubCodeDHE == null)
			this.pUpdateNullPubCodeDHE = this.con.prepareStatement(GetSerialData.machVDHEUpdateNullPubCodeQuery);

//		private static String machVDHEUpdateNullPubCodeQuery = "update MACHV.DOC_HOSTING_ENV set LOADED_FLAG=?,LOADED_LOCATION = ?,PRISM_CLIP_DATE=? WHERE DOC_UUID=? AND HOST_ENV_CODE=?";
		pUpdateNullPubCodeDHE.clearParameters();
		pUpdateNullPubCodeDHE.setString(1, loadedFlag);
		pUpdateNullPubCodeDHE.setString(2, loadLocation);
		pUpdateNullPubCodeDHE.setDate(3,  clipDate);
		pUpdateNullPubCodeDHE.setDate(4,  addDate);
		pUpdateNullPubCodeDHE.setString(5, docUuid);
		pUpdateNullPubCodeDHE.setInt(6, hostEnvCode);
		pUpdateNullPubCodeDHE.executeUpdate();
	}
	/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void insertDhe(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, int pubCode, java.sql.Date addDate) throws SQLException
	{
		if (this.pInsertDHE == null)
			this.pInsertDHE = this.con.prepareStatement(GetSerialData.machVDHEInsertQuery);

//		private static String machVDHEInsertQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE,PUBLICATION_CODE) values (?,?,?,?,null,?,?)";
		pInsertDHE.clearParameters();
		pInsertDHE.setInt(1, hostEnvCode);
		pInsertDHE.setString(2, docUuid);
		pInsertDHE.setString(3, loadedFlag);
		pInsertDHE.setString(4, loadLocation);
		pInsertDHE.setDate(5,  addDate);
		pInsertDHE.setDate(6,  clipDate);
		pInsertDHE.setInt(7, pubCode);
		pInsertDHE.execute();
	}
	/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void insertNullPubDhe(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, java.sql.Date addDate) throws SQLException
	{
		if (this.pInsertNullPubCodeDHE == null)
			this.pInsertNullPubCodeDHE = this.con.prepareStatement(GetSerialData.machVDHEInsertNullPubCodeQuery);
		//private static String machVDHEInsertNullPubCodeQuery = "Insert into MACHV.DOC_HOSTING_ENV (HOST_ENV_CODE,DOC_UUID,LOADED_FLAG,LOADED_LOCATION,ADDED_DATE,PRISM_CLIP_DATE) values (?,?,?,?,null,?)";

		pInsertNullPubCodeDHE.clearParameters();
		pInsertNullPubCodeDHE.setInt(1, hostEnvCode);
		pInsertNullPubCodeDHE.setString(2, docUuid);
		pInsertNullPubCodeDHE.setString(3, loadedFlag);
		pInsertNullPubCodeDHE.setString(4, loadLocation);
		pInsertNullPubCodeDHE.setDate(5,  clipDate);
		pInsertNullPubCodeDHE.setDate(6, addDate);
		pInsertNullPubCodeDHE.execute();
	}
/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void updateDhe(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, int pubCode, java.sql.Date addDate) throws SQLException
	{
		if (this.pUpdateDHE == null)
			this.pUpdateDHE = this.con.prepareStatement(GetSerialData.machVDHEUpdateQuery);

		//private static String machVDHEUpdateQuery = "update MACHV.DOC_HOSTING_ENV set LOADED_FLAG=?,LOADED_LOCATION = ?,PRISM_CLIP_DATE=?, PUB_CODE=? WHERE DOC_UUID=? AND HOST_ENV_CODE=?";
		pUpdateDHE.clearParameters();
		pUpdateDHE.setString(1, loadedFlag);
		pUpdateDHE.setString(2, loadLocation);
		pUpdateDHE.setDate(3,  clipDate);
		pUpdateDHE.setInt(4, new Integer(pubCode).intValue());
		pUpdateDHE.setDate(5, addDate);
		pUpdateDHE.setString(6, docUuid);
		pUpdateDHE.setInt(7, hostEnvCode);
		pUpdateDHE.executeUpdate();
	}
	/**
	 * @param rfUuid
	 * @param loadLocation
	 * @param clipDate
 * @throws SQLException 
	 */
	private void updateNullPubDhe(String loadedFlag, int hostEnvCode, String docUuid, String loadLocation,
			java.sql.Date clipDate, java.sql.Date addDate) throws SQLException
	{
		if (this.pUpdateNullPubCodeDHE == null)
			this.pUpdateNullPubCodeDHE = this.con.prepareStatement(GetSerialData.machVDHEUpdateNullPubCodeQuery);
		//private static String machVDHEUpdateNullPubCodeQuery = "update MACHV.DOC_HOSTING_ENV set LOADED_FLAG=?,LOADED_LOCATION = ?,PRISM_CLIP_DATE=? WHERE DOC_UUID=? AND HOST_ENV_CODE=?";		
		
		pUpdateNullPubCodeDHE.clearParameters();
		pUpdateNullPubCodeDHE.setString(1, loadedFlag);
		pUpdateNullPubCodeDHE.setString(2, loadLocation);
		pUpdateNullPubCodeDHE.setDate(3,  clipDate);
		pUpdateNullPubCodeDHE.setDate(4,  addDate);
		pUpdateNullPubCodeDHE.setString(5, docUuid);
		pUpdateNullPubCodeDHE.setInt(6, hostEnvCode);
		pUpdateNullPubCodeDHE.executeUpdate();
	}
	/**
	 * @param currNovCiteData
	 * @throws SQLException 
	 */
	private void updateDoc(String docUuid, String docType) throws SQLException
	{
		if (this.pUpdateDoc == null)
			this.pUpdateDoc = this.con.prepareStatement(GetSerialData.machVDocUpdateQuery);
		
		pUpdateDoc.clearParameters();
		pUpdateDoc.setString(1, docUuid);
		pUpdateDoc.setString(2, docType);
		pUpdateDoc.setString(3, docUuid);
		pUpdateDoc.executeUpdate();
	}
	/**
	 * @param currNovCiteData
	 * @throws SQLException 
	 */
	private void insertDoc(String docUuid, String docType) throws SQLException
	{
		if (this.pInsertDoc == null)
			this.pInsertDoc = this.con.prepareStatement(GetSerialData.machVDocInsertQuery);
		
		pInsertDoc.clearParameters();
		pInsertDoc.setString(1, docUuid);
		pInsertDoc.setString(2, docType);
		pInsertDoc.execute();
	}
	/**
	 * @param currNovCiteData
	 * @throws SQLException 
	 */
	private void deleteDoc(String docUuid) throws SQLException
	{
		if (this.pDeleteDoc == null)
			this.pDeleteDoc = this.con.prepareStatement(GetSerialData.machVDocDeleteQuery);
		
		pDeleteDoc.clearParameters();
		pDeleteDoc.setString(1, docUuid);
		pDeleteDoc.execute();
	}

	private void updateDoc(String docUuid, NovusDocData currNovusLoadInfo,
			String machVLoadData)
	{
		try
		{
			if (!loadComponentCorrect(currNovusLoadInfo.getComponent(), machVLoadData) && (hasCompVals(currNovusLoadInfo.getComponent())))
			{
				//private static String machVDHEUpdateCompQuery = "update machv.doc_hosting_env set loaded_location = ? where host_env_code = 10 and doc_uuid = ?";
				if (this.pUpdateCompStmt == null)
					this.pUpdateCompStmt = this.con.prepareStatement(GetSerialData.machVDHEUpdateCompQuery);
				this.pUpdateCompStmt.setString(1, currNovusLoadInfo.getComponent()) ;
				this.pUpdateCompStmt.setString(2, currNovusLoadInfo.getDocUuid());
				this.pUpdateCompStmt.execute();
				this.pUpdateCompStmt.clearParameters();
			}
			if (!loadCollectionCorrect(currNovusLoadInfo, machVLoadData) && (hasCollVals(currNovusLoadInfo)))
			{
				//private static String machVDHEUpdateCollQuery = "update machv.doc_hosting_env set loaded_location = ?, prism_clip_date = ?, publication_code = ? where host_env_code = 20 and doc_uuid = ?";
				if (currNovusLoadInfo.getPubCode() == null)
				{
					if (this.pUpdateCollectNullPubCodeStmt == null)
						this.pUpdateCollectNullPubCodeStmt = this.con.prepareStatement(GetSerialData.machVDHEUpdateNullPubCodeCollQuery);
					this.pUpdateCollectNullPubCodeStmt.setString(1, currNovusLoadInfo.getCollection());  // set the collection
					if (getPrismDate(currNovusLoadInfo.getComponent()) != null)
						this.	pUpdateCollectNullPubCodeStmt.setDate(2, new java.sql.Date(getPrismDate(currNovusLoadInfo.getPrismClipDate()).getTime()));		// set the prismClipDate
					else
						this.pUpdateCollectStmt.setDate(2, (java.sql.Date)null);
					//this.	pUpdateCollectStmt.setInt(3, getCollPubCode(novusLoadData));		// set the publicationCode
					this.pUpdateCollectNullPubCodeStmt.setString(3, (String)null);
					this.pUpdateCollectNullPubCodeStmt.execute();
					this.pUpdateCollectNullPubCodeStmt.clearParameters();
				}
				else
				{
					if (this.pUpdateCollectStmt == null)
						this.pUpdateCollectStmt = this.con.prepareStatement(GetSerialData.machVDHEUpdateCollQuery);
					this.pUpdateCollectStmt.setString(1, currNovusLoadInfo.getCollection());  // set the collection
					if (getPrismDate(currNovusLoadInfo.getPrismClipDate()) != null)
						this.	pUpdateCollectStmt.setDate(2, new java.sql.Date(getPrismDate(currNovusLoadInfo.getPrismClipDate()).getTime()));		// set the prismClipDate
					else
						this.pUpdateCollectStmt.setDate(2, (java.sql.Date)null);
					this.	pUpdateCollectStmt.setInt(3, new Integer(currNovusLoadInfo.getPubCode()).intValue());		// set the publicationCode
					this.pUpdateCollectStmt.setString(4, currNovusLoadInfo.getDocUuid());
					this.pUpdateCollectStmt.execute();
					this.pUpdateCollectStmt.clearParameters();
				}
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "getMachVLoadData()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
		}		
	}
	private static String PUB_CODE_VAL = "pubCode:";
	/**
	 * @param novusLoadData
	 * @return
	 */
	private Integer getCollPubCode(String loadData)
	{
		Integer retVal = null;
		if (loadData != null)
		{
			String pubCodeStr = loadData.substring(loadData.indexOf(PUB_CODE_VAL) + PUB_CODE_VAL.length());
			int pubCodeReturnLocation = pubCodeStr.length();
			if (pubCodeStr.indexOf('\r') > 0)
			{
				pubCodeReturnLocation  = pubCodeStr.indexOf('\r'); 
			}
			String t = 	pubCodeStr.substring(0, pubCodeReturnLocation);
			if ((t == null) || ((t != null) && (t.compareTo("null") == 0)))
				return (Integer)null;
			return Integer.valueOf(t);
		}
		else
			return retVal;
	}
	private static String PRISM_CLIP_VAL = "prismClipDate:";
    public static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final String NOVUS_PATTERN = "yyyyMMddHHmmss";

	/**
	 * @param novusLoadData
	 * @return
	 */
	private java.util.Date getPrismDate(String loadData)
	{
		//MachV has Document uuid:I6d2595374a9c11e39ac8bab74931929c is loaded to:w_3rd_bnacsm component:BNAFED3 prismClipDate:2013-11-11 pubCode:1538
		// TODO Auto-generated method stub
		Date d = null;
		String dateString = "";

		if (loadData != null)
		{
			if (loadData.indexOf(PRISM_CLIP_VAL) < 0)
			{
				// just have a date value
				dateString = loadData;
			}
			else
			{
				int startIdex = loadData.indexOf(PRISM_CLIP_VAL) + PRISM_CLIP_VAL.length();
				int endIdex = loadData.indexOf(PUB_CODE_VAL) - 1;
				//int length = endIdex - startIdex;
				dateString = loadData.substring(startIdex, endIdex);
			}
			int length = dateString.length();
			try
			{
				if (TIMESTAMP_PATTERN.length() == length)
				{
					return new SimpleDateFormat(TIMESTAMP_PATTERN).parse(dateString);
				}
				if (DATE_PATTERN.length() == length)
				{
					return new SimpleDateFormat(DATE_PATTERN).parse(dateString);
				}
				if (NOVUS_PATTERN.length() == length)
				{
					return new SimpleDateFormat(NOVUS_PATTERN).parse(dateString);
				}
			}
			catch (ParseException e)
			{
				String msg = this.getClass().getName() + "." + "getMachVPrismDate()" + " encountered:" + e.getClass().getName() + 
						" with msg=" + e.getMessage();
				if (e.getCause() != null)
					msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
				System.out.println(msg);
			}
		}
		return d;
	}
	private static String ADDPRISM_CLIP_VAL = "prismClipDate:";

	/**
	 * @param novusLoadData
	 * @return
	 */
	private java.util.Date getAddDate(String loadData)
	{
		//MachV has Document uuid:I6d2595374a9c11e39ac8bab74931929c is loaded to:w_3rd_bnacsm component:BNAFED3 prismClipDate:2013-11-11 pubCode:1538
		// TODO Auto-generated method stub
		Date d = null;
		String dateString = "";

		if (loadData != null)
		{
			dateString = loadData;
			int length = dateString.length();
			try
			{
				if (TIMESTAMP_PATTERN.length() == length)
				{
					return new SimpleDateFormat(TIMESTAMP_PATTERN).parse(dateString);
				}
				if (DATE_PATTERN.length() == length)
				{
					return new SimpleDateFormat(DATE_PATTERN).parse(dateString);
				}
				if (NOVUS_PATTERN.length() == length)
				{
					return new SimpleDateFormat(NOVUS_PATTERN).parse(dateString);
				}
			}
			catch (ParseException e)
			{
				String msg = this.getClass().getName() + "." + "getAddDate()" + " encountered:" + e.getClass().getName() + 
						" with msg=" + e.getMessage();
				if (e.getCause() != null)
					msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
				System.out.println(msg);
			}
		}
		return d;
	}
	/**
	 * @param novusLoadData
	 * @return
	 */
//	private java.util.Date getNovusPrismDate(String loadData)
//	{
//		// TODO Auto-generated method stub
//		Date d = null;
//		if (loadData != null)
//		{
//			int startIdex = loadData.indexOf(PRISM_CLIP_VAL) + PRISM_CLIP_VAL.length();
//			int endIdex = loadData.indexOf(PUB_CODE_VAL) - 1;
//			int length = endIdex - startIdex;
//			String dateString = loadData.substring(startIdex, endIdex);
//			try
//			{
//				if (NOVUS_PATTERN.length() == length)
//				{
//					return new SimpleDateFormat(NOVUS_PATTERN).parse(dateString);
//				}
//			}
//			catch (ParseException e)
//			{
//				String msg = this.getClass().getName() + "." + "getMachVPrismDate()" + " encountered:" + e.getClass().getName() + 
//						" with msg=" + e.getMessage();
//				if (e.getCause() != null)
//					msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
//				System.out.println(msg);
//			}
//		}
//		return d;
//	}
	private static String COLLECTION_VAL = "is loaded to:";
	private static String NULL_STR = "null";
	/**
	 * @param novusLoadData
	 * @return
	 */
	private String getCollVal(String loadData)
	{
		if (loadData != null)
		{
			String loadLoc = loadData.substring(loadData.indexOf(COLLECTION_VAL) + COLLECTION_VAL.length(), (loadData.indexOf(COMPONENT_VAL) - 1));
			if (loadLoc.compareTo(NULL_STR) == 0)
				return null;
			return loadLoc;
		}
		else
			return null;
	}
	private static String PUB_CODE_START = "<md.pubid>"; 
	private static String PUB_CODE_END = "</md.pubid>"; 
	/**
	 * @param substring
	 * @return
	 */
	private String getPubCode(String s)
	{
		// get the citePubCode <md.pubid>7200</md.pubid>
		String retVal = "0";
		if (s != null) 
		{
			int startIdex = s.indexOf(PUB_CODE_START);
			int endIdex = s.indexOf(PUB_CODE_END);
			if ((startIdex >= 0) && (endIdex > startIdex))
			{
				int realStartIdex = startIdex + PUB_CODE_START.length(); 
				retVal = s.substring(realStartIdex, endIdex);
			}
		}
		return retVal;
	}
	private static String SERIAL_START = "<md.ccserial>"; 
	private static String SERIAL_END = "</md.ccserial>"; 
	/**
	 * @param substring
	 * @return
	 */
	private String get10DigitSerial(String s)
	{
		String retVal = "0";
		if (s != null) 
		{
			int startIdex = s.indexOf(SERIAL_START);
			int endIdex = s.indexOf(SERIAL_END);
			if ((startIdex >= 0) && (endIdex > startIdex))
			{
				int realStartIdex = startIdex + SERIAL_START.length(); 
				retVal = s.substring(realStartIdex, endIdex);
			}
		}
		return retVal;
	}
	
	private static String DF_UUID_START_TAG = "<md.doc.family.uuid>";
	private static String DF_UUID_END_TAG = "</md.doc.family.uuid>";
	/**
	 * @param metadata2
	 * @return
	 */
	private String getCaseUuid(String metadata2)
	{
		//<md.doc.family.uuid>I47ff78e0726411d78bcee6281d031f02</md.doc.family.uuid>
		String caseUuid = null;
		if (metadata2 != null) 
		{
			int startIdex = metadata2.indexOf(DF_UUID_START_TAG);
			int endIdex = metadata2.indexOf(DF_UUID_END_TAG);
			if ((startIdex >= 0) && (endIdex > startIdex))
			{
				int realStartIdex = startIdex + DF_UUID_START_TAG.length(); 
				caseUuid = metadata.substring(realStartIdex, endIdex);
			}
		}
		return caseUuid;
	}

	private static String DOCUMENT_UUID = "Document uuid:";
	/**
	 * @param novusLoadData
	 * @return
	 */
	private String getDocVal(String loadData)
	{
		if (loadData != null)
		{
			String docUuid = loadData.substring(loadData.indexOf(DOCUMENT_UUID) + DOCUMENT_UUID.length(), (loadData.indexOf(COLLECTION_VAL) - 1));
			return docUuid;
		}
		else
			return null;
	}
	private static String COMPONENT_VAL = "component:";
	/**
	 * @param novusLoadData
	 * @return
	 */
	private String getCompVal(String loadData)
	{
		if (loadData != null)
		{
			String loadLoc = loadData.substring((loadData.indexOf(COMPONENT_VAL) + COMPONENT_VAL.length()), (loadData.indexOf(PRISM_CLIP_VAL) - 1));
			if (loadLoc.compareTo(NULL_STR) == 0)
				return null;
			return loadLoc;
		}
		else
			return null;
	}
	/**
	 * @param machVLoadData
	 * @return
	 */
	private boolean hasCollVals(String machVLoadData)
	{
		boolean retVal = false;
		// validate passed string contains a collection  load string, docUuid, prismClipDate and pubCode
		if (getCollVal(machVLoadData) == null)
			return false;
		if (getDocVal(machVLoadData) == null)
			return false;
//		if (getPrismDate(machVLoadData) == null)
//			return false;
//		if (getCollPubCode(machVLoadData) == null)
//			return false;
		return true;
	}
	private boolean hasCollVals(NovusDocData ndd)
	{
		boolean retVal = false;
		if (ndd.getCollection() == null)
			return false;
		if (ndd.getDocUuid() == null)
			return false;
		return true;
	}
	/**
	 * @param novusLoadData
	 * @return
	 */
	private boolean hasCompVals(NovusDocData ndd)
	{
		boolean retVal = false;
		// Validate passed string contains a docUuid and a component value
		if (ndd.getComponent() == null)
			return false;
		if (ndd.getDocUuid() == null)
			return false;
		return true;
	}
	/**
	 * @param novusLoadData
	 * @return
	 */
	private boolean hasCompVals(String novusLoadData)
	{
		boolean retVal = false;
		// Validate passed string contains a docUuid and a component value
		if (getCompVal(novusLoadData) == null)
			return false;
		if (getDocVal(novusLoadData) == null)
			return false;
		return true;
	}
	/*
	 * Test to see if the Component Value is the same in MachV and Novus
	* @param novusLoadData
	* @param machVLoadData
	* @return
	*/
	private boolean loadComponentCorrect(String novusLoadData,
			String machVLoadData)
	{
		//MachV has Document uuid:I3a4e44aaf53511d99439b076ef9ec4de is loaded to:null component:AWPAC prismClipDate:null pubCode:0
		String novusCompVal = novusLoadData;
		String machVCompVal = getCompVal(machVLoadData);
		if ((machVCompVal == null) && (novusCompVal == null))
			return true;
		else if ((machVCompVal != null) && (novusCompVal != null) && (machVCompVal.compareTo(novusCompVal) == 0))
			return true;
		else
			return false;
	}
	/*
	 * see if the following values are the same in Novus and MachV
	 * - collection
	 * - prismClipDate
	 * - pubCode
	 * if any differences, then return false
	* @param novusLoadData
	* @param machVLoadData
	* @return
	*/
	private boolean loadCollectionCorrect(NovusDocData currNovusLoadInfo,
			String machVLoadData)
	{
		//MachV has Document uuid:I3a4e44aaf53511d99439b076ef9ec4de is loaded to:null component:AWPAC prismClipDate:null pubCode:0
		String novusCollVal = currNovusLoadInfo.getCollection();
		String machVCollVal = getCollVal(machVLoadData);

		// Take care of the easy stuff first  - can't add it if we don't have a collection!
		if ((machVCollVal == null) && (novusCollVal == null))
			return true;
		else if ((machVCollVal != null) && (novusCollVal != null) && (machVCollVal.compareTo(novusCollVal) != 0))
			return false;
		else if (machVCollVal == null && novusCollVal != null)
			return false;
		else
		{
			if (prismClipDifference(currNovusLoadInfo.getPrismClipDate(),
					machVLoadData))
				return false;
			if (pubCodeDifference(currNovusLoadInfo.getPubCode(),
					machVLoadData))
				return false;
			return true;
		}
	}

	/**
	 * @param novusLoadData
	 * @param machVLoadData
	 * @return
	 */
	private boolean prismClipDifference(String novusLoadData,
			String machVLoadData)
	{
		Date novusClipDate = getPrismDate(novusLoadData);
		Date machVClipDate = getPrismDate(machVLoadData);
		
		if ((novusClipDate == null) && (machVClipDate == null))
			return false;
		else if ((novusClipDate != null) && (machVClipDate != null) && (novusClipDate.getTime() == machVClipDate.getTime()))
			return false;
		else
			return true;
	}
	/**
	 * @param novusLoadData
	 * @param machVLoadData
	 * @return
	 */
	private boolean prismClipDifference(NovusDocData ndd,
			String machVLoadData)
	{
		Date novusClipDate = getPrismDate(ndd.getPrismClipDate());
		Date machVClipDate = getPrismDate(machVLoadData);
		
		if ((novusClipDate == null) && (machVClipDate == null))
			return false;
		else if ((novusClipDate != null) && (machVClipDate != null) && (novusClipDate.getTime() == machVClipDate.getTime()))
			return false;
		else
			return true;
	}
	/**
	 * @param novusLoadData
	 * @param machVLoadData
	 * @return
	 */
	private boolean pubCodeDifference(String novusLoadData,
			String machVLoadData)
	{
		int novusPubCode = new Integer(novusLoadData).intValue();
		int machvPubCode = getCollPubCode(machVLoadData);
		if (novusPubCode == machvPubCode)
			return false;
		else
			return true;
	}
	/**
	 * @param get8DigitSerial
	 * @param con2
	 * @return
	 */
//	private String getMachVLoadData(String novusLoadData)
	private String getMachVLoadData(NovusDocData novusLoadData)
	{
		String retStr = "";
		//String docUuid = this.getDocUuidFromNouvsLoadData(novusLoadData);
		String docUuid = novusLoadData.getDocUuid();
//		private static String machVDHEQuery = "select host_env_code, loaded_location, prism_clip_date, publication_Code from machv.doc_hosting_env where doc_uuid = ?";

		String novusLoadLoc = "";
		String wlLoadLoc = "";
		Date clipDateVal = null;
		int pubCode = 0;
		try
		{
			if (this.pStmt == null)
				this.pStmt = this.con.prepareStatement(GetSerialData.machVDHEQuery);
			this.pStmt.setString(1, docUuid);
			ResultSet rs = this.pStmt.executeQuery();
			boolean docExists = false;
			while (rs.next())
			{
				docExists = true;
				int hostEnvCode = rs.getInt(1);
				if (hostEnvCode == 10)
				{
					wlLoadLoc = rs.getString(2);
					rs.getDate(3);
					rs.getInt(4);
				}
				else if (hostEnvCode == 20)
				{
					novusLoadLoc = rs.getString(2);
					Timestamp ts = rs.getTimestamp(3);
					if (ts != null)
						clipDateVal = new Date(ts.getTime());
					pubCode = rs.getInt(4);
				}
				else
					System.out.println("Not sure what this is - only expecting hostEnvCode in {10, 20}");
			}
			//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
			String clipDateValStr = "not populated";
			if (clipDateVal != null)
				clipDateValStr = new SimpleDateFormat(NOVUS_PATTERN).format(clipDateVal);
			if (docExists)
				retStr = "MachV has Document uuid:" + docUuid + " is loaded to:" + novusLoadLoc + " component:" + wlLoadLoc + " prismClipDate:" + clipDateValStr + " pubCode:" + pubCode;
			else
				retStr = "MachV does not have Document uuid:" + docUuid;
			this.closeResources(null, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "getMachVLoadData()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
		}
		return retStr;
	}
	private String getDocUuidFromNouvsLoadData(String novusLoadData)
	{
		//Nouvs has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
		String retStr = "";
		String uuid = "uuid:";
		String is = "is";
		if (novusLoadData.indexOf(uuid) > 0)
		{
			int startIndex = novusLoadData.indexOf(uuid) + uuid.length();
			int endIndex = novusLoadData.indexOf(is);
			retStr = novusLoadData.substring(startIndex, (endIndex -1));
			if (novusLoadData.indexOf(uuid, endIndex) > endIndex)
			{
				System.out.println("Hold on ther pardner - there are more than 1 document on this novusLoadData");
			}
		}			
		return retStr;
	}
	/**
	 * @param currentSerial
	 * @return
	 */
	private String get8DigitSerial(String currentSerial)
	{
		String eightDigitSerial = "";
		if (currentSerial.length() == 10)
		{
			eightDigitSerial = currentSerial.substring(2);
		}
		Long eightDigitSerialLong = new Long(eightDigitSerial);

		return eightDigitSerialLong.toString();
	}
	public void writeToReportFile(String novusLoadInfo, String machVLoadInfo, String serialNum)
	{
		// for serialNum = serialNum
		//Nouvs has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
		//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800

		File outputFile = new File("c:\\Data\\MachVAndNovusLoadData.txt");
		BufferedWriter writer = null;
		
		String reportLine = "For serialNumber:" + serialNum + crlf;
		reportLine += "Novus has " + novusLoadInfo + crlf;
		reportLine += machVLoadInfo + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			writer = new BufferedWriter(new FileWriter(outputFile, true));
			writer.write(reportLine);
			writer.close();
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = this.getClass().getName() + "." + "writeToReportFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static String NO_NOVUS_DOCS = "Novus has No Novus Docs Returned!Can't query MachV without a docUuid from Novus";
	private static String TERSE_NO_NOVUS_DOCS  = "No Novus Docs Returned!";
	public void writeToReportFile(String novusLoadInfo, String preFixMachVLoadInfo, String postFixMachVLoadInfo, String serialNum, String existingMachVData)
	{
		// for serialNum = serialNum
		//Nouvs has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800
		//MachV has Document uuid:I526280eef5a011d9bf60c1d57ebc853e is loaded to:w_cs_waor1 component:WORWA prismClipDate:19991203000000 pubCode:800

		File outputFile = new File("c:\\Data\\MachVAndNovusLoadData.txt");
//		File outputFile = new File("c:\\Data\\MachVAndNovusLoadDataWhenDocCountGT1.txt");
		BufferedWriter writer = null;
		if ((novusLoadInfo != null) && (novusLoadInfo.isEmpty()))
				novusLoadInfo = NO_NOVUS_DOCS;
		
		String reportLine = crlf + "For serialNumber:" + serialNum + crlf;
		reportLine += " Pre any change - MachV had the following data:" + crlf + existingMachVData;
		reportLine += "Novus has " + novusLoadInfo + crlf;
		reportLine += preFixMachVLoadInfo;
		if (!REPORT_ONLY)
			reportLine += postFixMachVLoadInfo;
		else
		{
			// just looking for a list of serials with > 1 document
			reportLine = serialNum + crlf;
		}
		
//		if (REPORT_NOT_ON_NOVUS_SERIALS_ONLY)
//		{
//			if ((novusLoadInfo.indexOf(NO_NOVUS_DOCS) >= 0) || (novusLoadInfo.indexOf(TERSE_NO_NOVUS_DOCS) >= 0))
//			{
//				reportLine = serialNum + crlf;
//			}
//			else
//				reportLine = null;
//		}
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (reportLine != null)
			{
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(reportLine);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = this.getClass().getName() + "." + "writeToReportFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	public void writeToNoDocsLoaded(String currUuid)
	{
		File outputFile = new File("c:\\Data\\NovusNOTLoadedDFUUIDData.txt");
		BufferedWriter writer = null;
		
		String reportLine = currUuid + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (reportLine != null)
			{				
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(reportLine);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = this.getClass().getName() + "." + "writeToNoDocsLoaded()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}

	public void writeToLoadedFile(NovusDocData currNovusLoadInfo, NovusData nd)
	{
		File outputFile = new File("c:\\Data\\NovusLoadedDFUUIDData.txt");
		BufferedWriter writer = null;
		
		String reportLine = currNovusLoadInfo.caseUuid + "," + currNovusLoadInfo.getDocUuid() + ":" + currNovusLoadInfo.getCollection() + ";" + currNovusLoadInfo.getPrismClipDate() + crlf;
		//String reportLine = nd.getSerNum() + ", "  + nd.getSerialNum() + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (reportLine != null)
			{				
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(reportLine);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = this.getClass().getName() + "." + "writeToLoadedFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}

	/**
	 * @param ps
	 * @param rs
	 * @param conn
	 */
	public void closeResources(Statement ps, ResultSet rs, Connection conn) 
	{
		try
		{
			if(ps != null)
			{
				this.pStmt.close();
				this.pStmt = null;
			}
		}
		catch(Exception e) 
		{
		;	
		}
		try
		{
			if(rs!= null)
				rs.close();
			rs = null;
		}
		catch(Exception e) 
		{
		;	
		}
		try 
		{
			if(conn != null)
			{
				this.con.close();
				this.con = null;
			}
		}
		catch(Exception e) 
		{
		;	
		}
	}

	private void getGlobalSearch() throws NovusException
	{
		System.out.println(".getGlobalSearch() entry");
		globalNovus = new Novus();
		//			novus.setQueueCriteria("NOVUS1A", "qc");
		globalNovus.setQueueCriteria(null, "prod");
		globalNovus.createPit();
		globalNovus.setResponseTimeout(30000);
		globalNovus.setProductName("Keycite-NIMS:Names_Audit");
		globalNovus.setBusinessUnit("Legal");

		globalSearch = globalNovus.getSearch();
		Collection<String> collectionCollect = new Vector<String>();
		collectionCollect.addAll(GetSerialData.makeCollectionCollection());
		for (String currCollect : collectionCollect)
		{
			globalSearch.addCollection(currCollect);
		}
		// serach by specific collection (is more like 'w_cs_so2')
		//search.addCollection("N_DUSSCT");

		// search by collection set
		//			search.addCollectionSet("N_DUSSCT");

		globalSearch.setQueryType(globalSearch.BOOLEAN);
		globalSearch.setSyntaxType(Search.NATIVE);
		globalSearch.setDocumentLimit(1000);
		globalSearch.setUseQueryWarnings(false);
		globalSearch.setHighlightFlag(false);
		globalSearch.setExpandNormalizedTerms(true);
		globalSearch.setExactMatch(false);
		globalSearch.setIgnoreStopwords(false);
		globalSearch.setDuplicationFiltering(false);
		hasNovus = true;
		System.out.println(".getGlobalSearch() exit");
	}
	public NovusData reportThisSerialDataReEntrant(String serialNum) throws Exception
	{
		NovusData nd = null;
		int attempts = 0;
		int maxAttempts = 3;
		while ((nd == null) && (attempts < maxAttempts))
		{
			try
			{
				if (!hasNovus)
					getGlobalSearch();
				nd = reportThisSerialData(serialNum);
				attempts = 0;
			}
			catch (NovusException e)
			{
				hasNovus = false;//retryThis
				nd = null;
				++attempts;
			}
		}
		if (attempts > maxAttempts)
			throw new Exception("Could not getGlobalSearch in attempts=" + attempts);
		return nd;
	}
	/**
	 * @param serialNum
	 * @return - NovusData - if NovusData.getNDocDataCollect() returns an empty collection - then No Novus Docs were returned.
	 * @throws NovusException
	 */
//	public String reportThisSerialData(String serialNum) throws NovusException
	public NovusData reportThisSerialData(String serialNum) throws NovusException
	{
		// String searchText = "=md.ccserial(" + serialNum + ")";
		// String searchText = "=md.dmsserial(" + serialNum + ")";
		String searchText = "=md.doc.family.uuid(" + serialNum + ")";
		System.out.println("in reportThisSerialData() serialNum:" + serialNum);
		globalSearch.setQueryText(searchText);
		Progress progress = globalSearch.submit(true);
		String retStr = "";
		NovusData nd = new NovusData(serialNum);
		while (!progress.isComplete())
		{
			progress = globalSearch.getProgress(null, 100);
		}
		final SearchResult result = globalSearch.getSearchResult();
		if (result != null)
		{
			// String[] metadataNames = new String[1500];
			// metadataNames = result.getMetaNames();
			int docsReturned = globalSearch.getDocsReturned();
			if (docsReturned > 0)
			{
				Document[] docs = result.getDocumentsInRange(1, docsReturned);
				System.out.println("For caseUuid:" + serialNum);
				for (Document document : docs)
				{
					String currDocUuid = document.getGuid();
					System.out.println("DocUuid:" + currDocUuid);
					boolean loadedToWORNYComp = false;
					if (document.getCollection() != null)
					{
						NovusDocData ndd = new NovusDocData();
						ndd.setCaseUuid(serialNum);
						metadata = document.getMetaData();
						String component = null;
						int starPageIdex = metadata.indexOf(STAR_PAGE);
						boolean isStarPage = false;
						if (starPageIdex >= 0)
							isStarPage = true;
						if (!isStarPage)
						{
							System.out.println("serialNum:" + get10DigitSerial(metadata));
							nd.setSerNum(get10DigitSerial(metadata));
							String pubCode = null;
							String prismClipDate = null;
							String addedDate = null;
							// String vol = null;
							// String page = null;
							if (metadata.indexOf("<n-view type=\"default\">ALR") != -1)
							{
								System.out.println("This is ALRFED data - there is no component set!");
								System.out.println("Also has no pubCode");
								ndd.setALRData(true);
							}
							else
							{
								ndd.setALRData(false);
								component = getCompnentFromMetadata(metadata);
								pubCode = getPubCodeFromMetadata(metadata);
								prismClipDate = getPrismClipDateFromMetadata(metadata);
								addedDate = getAdddedDateFromMetadata(metadata);
							}
							retStr = "";
							ndd.setComponent(component);
							String collection = document.getCollection();
							ndd.setCollection(collection);
							ndd.setPrismClipDate(prismClipDate);
							ndd.setAddedDate(addedDate);
							ndd.setPubCode(pubCode);
							ndd.setDocUuid(document.getGuid());
							retStr += "Novus has Document uuid:" + document.getGuid() + " is loaded to:"
									+ document.getCollection() + " component:" + component + crlf + " prismClipDate:"
									+ prismClipDate + " pubCode:" + pubCode + crlf;
							loadedToWORNYComp = isLoadedToWORNYComponent(component);
								
							// this is for the ANZ corruption of Domestic content - but Both MachV and Novus
							// are wrong on this - so I'll need to go by the mappings in MachV to determine
							// which are parallel only cites
							// detemine parallel only cites by looking to see if there is a mapping from the
							// pubCode to a status (status_publication) OR statusPubColl
							// (status_publication_collection)- if no mapping exist, this is a parallel only
							// cite.

							// based on the component - determine the publication which is the primary cite
							// publication code
							// determine the status number 'Westlaw: AWNYSOLD'
							// select status_number from machv.status_lookup where status_desc = 'Westlaw:
							// AWNYSOLD'

							// based on status_publication
							// select publication_code from machv.status_publication where status_number = ?
							// and publication_code in (list of pubCodes)
							// OR based on status_publication_collection
							// select publication_code from machv.status_publication_collection where
							// status_number = ? and publication_code in (list of pubCodes)

							// based on the results of the above queries, we can determine which citation
							// (by pubCode) is the primary cite

							// Parallel only pubs have no mappings in either status_publication or
							// status_publication_collection

							// have a concept of CITE NOT MAPPED
							// PARALLEL ONLY CITE
							// PRIMARY CITE

							// and which are main cites.
							if (!ndd.isALRData())
							{
								citeDataCollect.clear();
								firstCiteDataCollect.addAll(citeDataCollect);
								// citeDataCollect.add(getPrimaryCiteInfo(metadata, pubCode, component));
								NovusCiteData ncd = getPrimaryCiteInfo(metadata, pubCode, component);
								citeDataCollect.addAll(getAllOtherCiteInfo(metadata, pubCode, component, docsReturned, loadedToWORNYComp));
								if ((ncd.getRfUuid() != null) && (ncd.getPubCode().compareTo("0") != 0))
								{
									citeDataCollect.add(ncd);
									retStr += " rfUuid=" + ncd.getRfUuid();
								}
								else
								{
									if (!hasPrimaryCite(citeDataCollect))
									{
										firstCiteDataCollect.addAll(citeDataCollect);
										citeDataCollect.clear();
										NovusCiteData npNcd = getNonPrimaryNonParaCite(firstCiteDataCollect);
										if (npNcd != null)
											citeDataCollect.add(npNcd);
									}
								}
								if (!hasPrimaryCite(citeDataCollect))
								{
									Collection<NovusCiteData> newCiteDataCollect = new Vector<NovusCiteData>();
									newCiteDataCollect.addAll(getPrimaryCite(citeDataCollect));
									if (hasPrimaryCite(newCiteDataCollect))
									{
										citeDataCollect.clear();
										citeDataCollect.addAll(newCiteDataCollect);
									}
								}
								ndd.addNovusCiteData(citeDataCollect);
							}
							retStr += reportNovusCiteData(citeDataCollect);
							System.out.println(retStr + crlf);
							nd.addNovusDocData(ndd);
						}
						else
						{
							// gather the star page doc info and write to the starPage file
							retStr = "";
							ndd.setALRData(false);
							ndd.setCollection(document.getCollection());
							ndd.setPrismClipDate(getPrismClipDateFromMetadata(metadata));
							ndd.setAddedDate(getAdddedDateFromMetadata(metadata));
							ndd.setComponent(getCompnentFromMetadata(metadata));
							ndd.setPubCode(getPubCodeFromCollection(document.getCollection()));
							ndd.setDocUuid(document.getGuid());
							NovusCiteData ncd = getPrimaryCiteInfo(ndd);
							// if we can find the cite info, then add it - but don't fail if we can't find it.
							if (ncd != null)
							{
								citeDataCollect.clear();
								citeDataCollect.add(ncd);
								ndd.addNovusCiteData(citeDataCollect);
								nd.addNovusDocData(ndd);
							}
							String starPageData = "caseUuid:" + serialNum + ",SerialNumber:"
									+ get10DigitSerial(metadata) + ",DocUuid:" + ndd.getDocUuid() + ",Collection:"
									+ ndd.getCollection() + ",Component:" + ndd.getComponent()
									+ ",PubCode:" + ndd.getPubCode() + ",PrismClipDate:" + ndd.getPrismClipDate();
							GetSerialData.writeToStarPageFile(starPageData);

							System.out.println("serialNum:" + get10DigitSerial(metadata));
							retStr += "Novus has Document uuid:" + ndd.getDocUuid() + " is loaded to:"
									+ ndd.getCollection() + " component:" + ndd.getComponent() + crlf + " prismClipDate:"
									+ ndd.getPrismClipDate() + " pubCode:" + ndd.getPubCode() + crlf;

							retStr += reportNovusCiteData(citeDataCollect);
							System.out.println(retStr + crlf);

						}
					}
				}
			}
			else
				retStr = NO_NOVUS_DOCS_RETURNED;
		}
		result.destroyResult();
		return nd;
	}

	private static String WORNYBASE = "WORNY";
	private boolean isLoadedToWORNYComponent(String component)
	{
		if (component.indexOf(WORNYBASE) >= 0)
			return true;
		return false;
	}

	private Collection<NovusCiteData> getPrimaryCite(Collection<NovusCiteData> collect)
	{
		Collection<NovusCiteData> retCollect = new Vector<NovusCiteData>();
		for (NovusCiteData novusCiteData : collect)
		{
			if ((novusCiteData != null) && (novusCiteData.isParallelOnly))
				retCollect.add(novusCiteData);
			else
			{
				if ((novusCiteData != null) && (novusCiteData.getPubCode().compareTo("999") == 0))
					novusCiteData.setPrimary(true);
				// may need to extend this if still not found
				retCollect.add(novusCiteData);
			}
		}
		return retCollect;
	}
	private boolean hasPrimaryCite(Collection<NovusCiteData> citeCollect)
	{
		boolean retVal = false;
		for (NovusCiteData novusCiteData : citeCollect)
		{
			if ((novusCiteData != null) && (novusCiteData.isPrimary))
				return true;
		}
		return retVal;
	}
	private String reportNovusCiteData(Collection<NovusCiteData> novusCiteCollect)
	{
		String retStr = "";
		for (NovusCiteData novusCiteData : novusCiteCollect)
		{
			retStr += crlf;
			if (novusCiteData != null)
			{
				retStr += " citePubCode=" + novusCiteData.getPubCode();
				if (novusCiteData.isPrimary())
					retStr += " isPrimaryCite";
//				else
//					retStr += " isNotPrimaryCite";
				if (novusCiteData.isParallelOnly())
					retStr += " isParallelOnlyCite";
				if ((!novusCiteData.isPrimary()) && (!novusCiteData.isParallelOnly()))
					retStr += " is neither Primary or Parallel cite";
				retStr +=  " with rfUuid=" + novusCiteData.getRfUuid();
				if (novusCiteData.getPubCode() != null)
					retStr += " with pubCode=" + novusCiteData.getPubCode();
				if (novusCiteData.getVolume() != null)
					retStr += " with vol=" + novusCiteData.getVolume();
				if (novusCiteData.getPage() != null)
					retStr += " with page=" + novusCiteData.getPage();
			}
			else
				retStr = "novusCiteData is null";
//			else
//				retStr += " isNotParallelOnlyCite";
		}
		return retStr;
	}
	/**
	 * @param firstCiteDataCollect2
	 * @return
	 */
	private NovusCiteData getNonPrimaryNonParaCite(
			Collection<NovusCiteData> firstCiteDataCollect2)
	{
		NovusCiteData retVal = null;
		for (NovusCiteData novusCiteData : firstCiteDataCollect2)
		{
			if (novusCiteData != null)
			{
				if ((novusCiteData.isParallelOnly == false) && (novusCiteData.isPrimary == false))
					retVal = novusCiteData;
			}
		}
		return retVal;
	}
	private static String PARALLEL_CITE_START = "<md.parallelcite>";
	private static String PARALLEL_CITE_END = "</md.parallelcite>";
	/**
	 * @param metadata2
	 * @return
	 */
	private Collection<NovusCiteData> getAllOtherCiteInfo(String s, String pubCode, String component, int docCount, boolean loadedToWORNYComp)
	{
		Collection<NovusCiteData> retCollect = new Vector<NovusCiteData>();
		// while we still find md.display.parallelcite - there are more parallel cites
		String caseUuid = getCaseUuid(s);
		String temp = s;
		while (temp.indexOf(PARALLEL_CITE_START) >= 0)
		{
			int parallelCiteStartIdex = temp.indexOf(PARALLEL_CITE_START);
			int parallelCiteEndIdex = temp.indexOf(PARALLEL_CITE_END);
			NovusCiteData ncd = getOtherCiteInfo(temp.substring(parallelCiteStartIdex, parallelCiteEndIdex), caseUuid, pubCode, component, docCount, loadedToWORNYComp);
			// if we get passed the initial set of primary and parallel cites, we may get into star page primary and parallel only cites - but the rfUuid is not set on these - so don't add, if no rfUuid value.
			if ((ncd != null) && (ncd.getRfUuid() != null))
				retCollect.add(ncd);
			temp = temp.substring(parallelCiteEndIdex+1);
		}
		return retCollect;
	}
	
	private static String CITE_DISPLAY_START = "<md.display.";
	private static String CITE_DISPLAY_END = "</md.display.";
	private static String GT = ">";
	private static String LT = "<";
	/**
	 * Need to return just the uncracked site string from the passed s
	 * @param s will be either a primarycite or a parallelcite
	 * @return
	 */
	private String getCiteString(String s)
	{
		String retVal = "";
		
		String mdDisplayStr = s.substring(s.indexOf(CITE_DISPLAY_START), s.indexOf(CITE_DISPLAY_END));
		int mdDisplayOpenTagEndIdex = mdDisplayStr.indexOf(GT);
		//int mdDisplayCloseTagStartIdex = mdDisplayStr.lastIndexOf(LT);
//		if ((mdDisplayOpenTagEndIdex != 0)
//				&& (mdDisplayCloseTagStartIdex != 0)
//				&& (mdDisplayOpenTagEndIdex < mdDisplayCloseTagStartIdex))
		if (mdDisplayOpenTagEndIdex != 0)
		{
			retVal = mdDisplayStr.substring((mdDisplayOpenTagEndIdex + 1));
		}
		return retVal;	
	}
	private static String NYSLIPOP_PUB_CODE = "4603";
	private NovusCiteData getOtherCiteInfo(String s, String caseUuid, String pubCode, String component, int docCount, boolean loadedToWORNYComp)
	{
		NovusCiteData retVal = new NovusCiteData();
		// The incomming s will only from the primaryCiteStart to the primaryCiteEnd - so don't need to find it.
		
		// get the RFUUId info
		int start = 0;
		boolean isNYSlipOpCite = false;
		retVal.setRfUuid(getRFUuidFromCite(s));
		// get the citePubCode <md.pubid>7200</md.pubid>
		retVal.setPubCode(getPubCode(s));
		if (getPubCode(s).compareTo(NYSLIPOP_PUB_CODE) == 0)
			isNYSlipOpCite = true;
		String citeString = getCiteString(s);
		retVal.setVolume(getVolumeFromCite(citeString));
		String tmpPage = getPageFromCite(citeString);
		if (tmpPage.indexOf(",") >= 0)
			tmpPage = tmpPage.replace(",", "");
		retVal.setPage(tmpPage);
		retVal.setPrimary(false);
		if (retVal.getPubCode().compareTo(pubCode) == 0)
			retVal.setPrimary(true);
//			if ((hasStatusPubColMappingVal(retVal.getPubCode(), component))
//					|| (hasStatusPubMappingVal(retVal.getPubCode(), component)))
//			{
//				retVal.setPrimary(true);
//			}
//			else
//				retVal.setPrimary(false);
//
			if (isParallelOnlyPub(retVal.getPubCode(), component))
			{
				retVal.setPrimary(false);
				retVal.setParallelOnly(true);
			}
// seems sometimes we need this - and other times we don't.
			if (docCount == 1)
			{
				if (retVal.getPubCode().compareTo("999") == 0)
				{
					// added on 9/14 to make WL cite a primary cite
					retVal.setPrimary(true);
					retVal.setParallelOnly(false);
				}
			}
			if (loadedToWORNYComp && isNYSlipOpCite)
				retVal.setPrimary(true);
			// get the dfUuid
			//<md.doc.family.uuid>I47ff78e0726411d78bcee6281d031f02</md.doc.family.uuid>
			retVal.setCaseUuid(caseUuid);
//		}
//		else
//			retVal = null;
		
		return retVal;
	}
	private static String PRIMARY_CITE_START = "<md.primarycite>";
	private static String PRIMARY_CITE_END = "</md.primarycite>";
	/**
	 * @param metadata2
	 * @return
	 */
	private NovusCiteData getPrimaryCiteInfo(String metadata2, String pubCode, String component)
	{
		NovusCiteData retVal = new NovusCiteData();
		// find <md.primarycite>
		// get the citeRfUuid
		//<md.display.primarycite ID="S  to " type="
		int priCiteStart = metadata2.indexOf(PRIMARY_CITE_START);
		int priCiteEnd = metadata2.indexOf(PRIMARY_CITE_END);
		
		// get the RFUUId info
		String possibleRFUuid =getRFUuidFromCite(metadata2.substring(priCiteStart, priCiteEnd));
		if ((possibleRFUuid != null) && (possibleRFUuid.length() > 33))
			possibleRFUuid = possibleRFUuid.substring(0, 33);
		retVal.setRfUuid(possibleRFUuid);
		// get the citePubCode <md.pubid>7200</md.pubid>
		retVal.setPubCode(getPubCode(metadata2.substring(priCiteStart, priCiteEnd)));
		retVal.setPrimary(true);
		String citeString = getCiteString(metadata2.substring(priCiteStart, priCiteEnd));
		retVal.setVolume(getVolumeFromCite(citeString));
		retVal.setPage(getPageFromCite(citeString));
		if ((hasStatusPubColMappingVal(retVal.getPubCode(), component))
				|| (hasStatusPubMappingVal(retVal.getPubCode(), component)))
		{
			retVal.setPrimary(true);
		}
		else
			retVal.setPrimary(false);
		// PrimaryCiteInfo CANNOT be a ParallelOnly cite
		if ((retVal.getPubCode().compareTo("0") != 0) && (isParallelOnlyPub(retVal.getPubCode(), component)))
		{
			retVal.setParallelOnly(true);
			retVal.setPrimary(false);
		}
		retVal.setCaseUuid(getCaseUuid(metadata2));
		// still trying to figure out how to handle pimary cites which are not reported - hence have no rfUuid - and no pubCode
//		if (retVal.getPubCode().compareTo("0") == 0)
//			retVal = getPubCodeCiteAsPrimaryCite(metadata2, pubCode, component);
		
		return retVal;
	}
	
//	private PreparedStatement pFindPubCodeFromStatPubColl = null;
//	private static String findPubCodeFromStatPubCollQuery = "select pc.publication_code " +
//			"from machv.novus_collection nc " + 
//			"left join machv.status_publication_collection pc " +
//			"on nc.collection_code = pc.collection_code " + 
//			"where nc.collection_name = ?";
//	private PreparedStatement pFindPubCodeFromPubColl = null;
//	private static String findPubCodeFromPubCollQuery = "select pc.publication_code " + 
//			"from machv.novus_collection nc " +
//			"left join machv.publication_collection pc " +
//			"on nc.collection_code = pc.collection_code " + 
//			"where nc.collection_name = ?";
	
	
	/**
	 * return a comma separate list of possible pubCodes
	 * @param collection
	 * @return
	 */
	private String getPubCodeFromCollection(String collection)
	{
		String retVal = null;
		Map<Integer, String> pubCodeHash = new HashMap<Integer, String>();
		// look in statusPublicationCollection first to see if one or more exist.
		try
		{
			if (this.pFindPubCodeFromStatPubColl == null)
				this.pFindPubCodeFromStatPubColl = this.con.prepareStatement(GetSerialData.findPubCodeFromStatPubCollQuery);
			this.pFindPubCodeFromStatPubColl.clearParameters();
			this.pFindPubCodeFromStatPubColl.setString(1, collection);
			ResultSet rs = this.pFindPubCodeFromStatPubColl.executeQuery();
			while (rs.next())
			{
				Integer pubCode = rs.getInt(1);
				if ((pubCode != null) && (pubCode > 0))
					pubCodeHash.put(pubCode, pubCode.toString());
			}
			this.closeResources(pFindPubCodeFromStatPubColl, rs, null); 

			if (this.pFindPubCodeFromPubColl == null)
				this.pFindPubCodeFromPubColl = this.con.prepareStatement(GetSerialData.findPubCodeFromPubCollQuery);
			this.pFindPubCodeFromPubColl.clearParameters();
			this.pFindPubCodeFromPubColl.setString(1, collection);
			rs = this.pFindPubCodeFromPubColl.executeQuery();
			while (rs.next())
			{
				Integer pubCode = rs.getInt(1);
				if ((pubCode != null) && (pubCode > 0))
					pubCodeHash.put(pubCode, pubCode.toString());
			}
			this.closeResources(pFindPubCodeFromPubColl, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "hasStatusPubColMappingVal()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}
		if (!pubCodeHash.isEmpty())
		{
			retVal = "";
			Collection<String>pubCodes = pubCodeHash.values();
			boolean first = true;
			for (String curr : pubCodes)
			{
				if (!first)
					retVal += ", ";
				retVal += curr;
				first = false;
			}
		}
		
		return retVal;
	}

	private NovusCiteData getPrimaryCiteInfo(NovusDocData ndd)
	{
		// recieved ndd should have case uuid, collection, pubCode 
		// need to find the cite on the case uuid with the given pubCode and pull the rfUuid from it.
		NovusCiteData retVal = new NovusCiteData();
		int resultCnt = 0;
		try
		{
			String pubCodeList = ndd.getPubCode();
			String findCiteByCaseUuidAndPubCodeQuery = "select citation_volume, citation_page, publication_code, rf_uuid " +
					"from machv.citation " + 
					" where case_uuid = '" + ndd.getCaseUuid() + "' " + 
					" and publication_code in (" + pubCodeList + ")"; 
			Statement stmt = null;
			ResultSet rs = null;
			if (pubCodeList != null)
			{
				stmt = this.con.createStatement();
				rs = stmt.executeQuery(findCiteByCaseUuidAndPubCodeQuery);
				while (rs.next())
				{
					String sVolume = rs.getString(1);
					String sPage = rs.getString(2);
					Integer pubCode = rs.getInt(3);
					String rfUuid = rs.getString(4);
					retVal.setCaseUuid(ndd.getCaseUuid());
					retVal.setRfUuid(rfUuid);
					retVal.setPage(sPage);
					retVal.setVolume(sVolume);
					retVal.setPubCode(pubCode.toString());
					ndd.setPubCode(pubCode.toString());
					retVal.setPrimary(true);
					++resultCnt;
				}
			}
			this.closeResources(stmt, rs, null);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "getPrimaryCiteInfo()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}

		// return null retVal if the cite was not found.
		// return null if more than one cite found
		if (resultCnt != 1)
			retVal = null;
		return retVal;
	}

	
	private NovusCiteData getPubCodeCiteAsPrimaryCite(String s, String pubCode, String component)
	{
		NovusCiteData retVal = new NovusCiteData();
		int start = 0;

		retVal = getPrimaryCiteFromOtherCiteInfo(s, pubCode, component);
		retVal.setPrimary(true);
//		if (hasStatusPubColMappingVal(retVal.getPubCode(), component))
		if ((hasStatusPubColMappingVal(retVal.getPubCode(), component))
				|| (hasStatusPubMappingVal(retVal.getPubCode(), component)))
		{
			retVal.setPrimary(true);
		}
		else
			retVal.setPrimary(false);

		if (isParallelOnlyPub(retVal.getPubCode(), component))
		{
			retVal.setPrimary(false);
			retVal.setParallelOnly(true);
		}

		// get the dfUuid
		//<md.doc.family.uuid>I47ff78e0726411d78bcee6281d031f02</md.doc.family.uuid>
		retVal.setCaseUuid(getCaseUuid(s));
		
		return retVal;
	}
	/**
	 * If there is no primary cite info, we need to get the 'primary' cite from the other cite info.
	 * The primary cite will have a pubCode which matches the passed in pubCode
	 * @param metadata2
	 * @return
	 */
	private NovusCiteData getPrimaryCiteFromOtherCiteInfo(String s, String pubCode, String component)
	{
		// while we still find md.display.parallelcite - there are more parallel cites
		String caseUuid = getCaseUuid(s);
		String temp = s;
		NovusCiteData primaryCite = null;
		while (temp.indexOf(PARALLEL_CITE_START) >= 0)
		{
			int parallelCiteStartIdex = temp.indexOf(PARALLEL_CITE_START);
			int parallelCiteEndIdex = temp.indexOf(PARALLEL_CITE_END);
			NovusCiteData ncd = getPrimaryOtherCiteInfo(temp.substring(parallelCiteStartIdex, parallelCiteEndIdex), caseUuid, pubCode, component);
			if (ncd != null)
				primaryCite = ncd;
			temp = temp.substring(parallelCiteEndIdex+1);
		}
		return primaryCite;
	}
	/**
	 * Return only the citeData which has a matching pubCode - this will be used as the primary cite
	 * @param s
	 * @param caseUuid
	 * @param pubCode
	 * @param component
	 * @return
	 */
	private NovusCiteData getPrimaryOtherCiteInfo(String s, String caseUuid, String pubCode, String component)
	{
		NovusCiteData retVal = new NovusCiteData();
		// The incomming s will only from the primaryCiteStart to the primaryCiteEnd - so don't need to find it.
		
		// get the RFUUId info
		int start = 0;
		retVal.setRfUuid(getRFUuidFromCite(s));
		// get the citePubCode <md.pubid>7200</md.pubid>
		retVal.setPubCode(getPubCode(s));
		if (retVal.getPubCode().compareTo(pubCode) == 0)
		{
			retVal.setPrimary(true);
			if ((hasStatusPubColMappingVal(retVal.getPubCode(), component))
					|| (hasStatusPubMappingVal(retVal.getPubCode(), component)))
			{
				retVal.setPrimary(true);
			}
			else
				retVal.setPrimary(false);
			String citeString = getCiteString(s);
			retVal.setVolume(getVolumeFromCite(citeString));
			retVal.setPage(getPageFromCite(citeString));

			if (isParallelOnlyPub(retVal.getPubCode(), component))
			{
				retVal.setPrimary(false);
				retVal.setParallelOnly(true);
			}
			retVal.setCaseUuid(caseUuid);
		}
		else
			retVal = null;
		
		return retVal;
	}
	
	
	
	
	
	private boolean isParallelOnlyPub(String pubCode, String component)
	{
		boolean retVal = false;
		boolean hasAnyStatusPubMapping = hasAnyStatusPubMappingVal(pubCode, component); 
		boolean hasAnyStatusPubColMapping = hasAnyStatusPubColMappingVal(pubCode, component);
		if (!hasAnyStatusPubMapping && !hasAnyStatusPubColMapping) 
			retVal = true;
		return retVal;
	}
	
	/**
	 * @param pubCode
	 * @param component
	 * @return
	 */
	private boolean hasStatusPubColMappingVal(String pubCode, String component)
	{
		boolean retVal = false;
		String statusDesc = "Westlaw: " + component;
		try
		{
			if (this.pStatPubColStmt == null)
				this.pStatPubColStmt = this.con.prepareStatement(GetSerialData.machVStatusPubColMappingQuery);
			this.pStatPubColStmt.clearParameters();
			this.pStatPubColStmt.setInt(1, new Integer(pubCode).intValue());
			this.pStatPubColStmt.setString(2, statusDesc);
			ResultSet rs = this.pStatPubColStmt.executeQuery();
			if (rs.next())
				retVal = true;
			this.closeResources(null, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "hasStatusPubColMappingVal()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}

		return retVal;
	}
	/**
	 * @param pubCode
	 * @param component
	 * @return
	 */
	private boolean hasStatusPubMappingVal(String pubCode, String component)
	{
		boolean retVal = false;
		String statusDesc = "Westlaw: " + component;
		try
		{
			if (this.pStatPubStmt == null)
				this.pStatPubStmt = this.con.prepareStatement(GetSerialData.machVStatusPubMappingQuery);
			this.pStatPubStmt.clearParameters();
			this.pStatPubStmt.setInt(1, new Integer(pubCode).intValue());
			this.pStatPubStmt.setString(2, statusDesc);
			ResultSet rs = this.pStatPubStmt.executeQuery();
			boolean docExists = false;
			if (rs.next())
				retVal = true;
			this.closeResources(null, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "hasStatusPubMappingVal()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}

		return retVal;
	}
	/**
	 * @param pubCode
	 * @param component
	 * @return
	 */
	private boolean hasAnyStatusPubColMappingVal(String pubCode, String component)
	{
		boolean retVal = false;
		String statusDesc = "Westlaw: " + component;
		try
		{
			if (this.pAnyStatPubColStmt == null)
				this.pAnyStatPubColStmt = this.con.prepareStatement(GetSerialData.machVAnyStatusPubColMappingQuery);
			this.pAnyStatPubColStmt.clearParameters();
			this.pAnyStatPubColStmt.setInt(1, new Integer(pubCode).intValue());
			//this.pAnyStatPubColStmt.setString(2, statusDesc);
			ResultSet rs = this.pAnyStatPubColStmt.executeQuery();
			if (rs.next())
				retVal = true;
			this.closeResources(null, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "hasStatusPubColMappingVal()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}

		return retVal;
	}
	/**
	 * @param pubCode
	 * @param component
	 * @return
	 */
	private boolean hasAnyStatusPubMappingVal(String pubCode, String component)
	{
		boolean retVal = false;
		String statusDesc = "Westlaw: " + component;
		try
		{
			if (this.pAnyStatPubStmt == null)
				this.pAnyStatPubStmt = this.con.prepareStatement(GetSerialData.machVAnyStatusPubMappingQuery);
			this.pAnyStatPubStmt.clearParameters();
			this.pAnyStatPubStmt.setInt(1, new Integer(pubCode).intValue());
			//this.pAnyStatPubStmt.setString(2, statusDesc);
			ResultSet rs = this.pAnyStatPubStmt.executeQuery();
			boolean docExists = false;
			if (rs.next())
				retVal = true;
			this.closeResources(null, rs, null); 
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			String msg = this.getClass().getName() + "." + "hasStatusPubMappingVal()" + " encountered:" + e.getClass().getName() + 
					" with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName() + " with cause msg=" +  e.getCause().getMessage();
			System.out.println(msg);
			System.exit(-1);
		}

		return retVal;
	}
	private static String CITE_RF_UUID_START = "ID=\"S";
	private static String TYPE = "\" type=";
	private static String DISPLAY = "\" display=";
	private static String STAR_PAGE = "ID=\"Starpage_";
	private String getRFUuidFromCite(String s)
	{
		String retVal = null;
		if (s != null)
		{
			int rfUuidStartIdex = s.indexOf(CITE_RF_UUID_START) ;
			// Have to account for the following display before type, or type before display
			//<md.display.primarycite ID="S073f64809cbc11d9bc61beebb95be672" display="Y" type="official_reporter">191 U.S. 17</md.display.primarycite>
			//<md.display.primarycite ID="Scde697139cc211d993e6d35cc61aab4a" type="official_reporter" display="Y">148 U.S. 372</md.display.primarycite>
			// 
			int typeIdex = s.indexOf(TYPE);
			int dispIdex = s.indexOf(DISPLAY);
			int starPageIdex = s.indexOf(STAR_PAGE);
	
			if (starPageIdex < 0)
			{
				int rfUuidEndIdex = 0;
				if (typeIdex < dispIdex)
					rfUuidEndIdex = typeIdex;
				else
					rfUuidEndIdex = dispIdex;
				if ((rfUuidStartIdex > 0) && (rfUuidEndIdex > rfUuidStartIdex))
					retVal = "I" + s.substring((rfUuidStartIdex + CITE_RF_UUID_START.length()), rfUuidEndIdex);
			}
			else
				; // This is a star page - so there are no rfUuids on here
			if ((retVal != null) && (retVal.length() > 33))
				retVal = retVal.substring(0, 33);
		}
		return retVal;
	}
	private static String SPACE = " ";
	/**
	 * @param s - expected to only the uncracked site info from <md.display.parallelcite ID="Saa17f25012ec11ebb80af3a40f8efd39" type="official_reporter" display="Y">54 Cal.App.5th 885</md.display.parallelcite>
	 * so s will only contain 54 Cal.App.5th 885
	 * @return volume value as a string
	 */
	private String getVolumeFromCite(String s)
	{
		String retVal = null;
		if (s != null)
		{
			// dealing with something along the lines of 54 Cal.App.5th 885
			int startIdex = 0;
			int endIdex = s.indexOf(SPACE) ;
			// Have to account for the following display before type, or type before display
			//<md.display.primarycite ID="S073f64809cbc11d9bc61beebb95be672" display="Y" type="official_reporter">191 U.S. 17</md.display.primarycite>
			//<md.display.primarycite ID="Scde697139cc211d993e6d35cc61aab4a" type="official_reporter" display="Y">148 U.S. 372</md.display.primarycite>
			// 
			int typeIdex = s.indexOf(TYPE);
			int dispIdex = s.indexOf(DISPLAY);
			int starPageIdex = s.indexOf(STAR_PAGE);
	
			if (starPageIdex < 0)
			{
				// prevent an out of bounds exception
				if (endIdex > startIdex)
					retVal = s.substring(startIdex, endIdex);
			}
			else
				; // This is a star page - so there are no rfUuids on here
		}
		return retVal;
	}
	/**
	 * @param s - expected to only the uncracked site info from <md.display.parallelcite ID="Saa17f25012ec11ebb80af3a40f8efd39" type="official_reporter" display="Y">54 Cal.App.5th 885</md.display.parallelcite>
	 * so s will only contain 54 Cal.App.5th 885
	 * @return page value as a string
	 */
	private String getPageFromCite(String s)
	{
		String retVal = null;
		if (s != null)
		{
			// dealing with something along the lines of 54 Cal.App.5th 885
			int startIdex = s.lastIndexOf(SPACE);
			int starPageIdex = s.indexOf(STAR_PAGE);
	
			if (starPageIdex < 0)
			{
				if (startIdex > 0)
					retVal = s.substring(startIdex+1);
			}
			else
				; // This is a star page - so there are no rfUuids on here
		}
		return retVal;
	}

	private static String NO_NOVUS_DOCS_RETURNED = "No Novus Docs Returned!";
	public void reportSerialData(String serialNum)
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

			Search search = novus.getSearch();
			Collection<String> collectionCollect = new Vector<String>();
			collectionCollect.addAll(GetSerialData.makeCollectionCollection());
			for (String currCollect : collectionCollect)
			{
				search.addCollection(currCollect);
			}
			// serach by specific collection (is more like 'w_cs_so2')
			//search.addCollection("N_DUSSCT");

			// search by collection set
//			search.addCollectionSet("N_DUSSCT");
			
			search.setQueryType(search.BOOLEAN);
			String searchText = "=md.ccserial(" + serialNum + ")";
//			String searchText = "=md.docketnum(18-1426)";
//			String searchText = "=md.docketnum(18-9054)";
			
			search.setQueryText(searchText);
			//search.setQueryText("=md.ccserial(1931123996)");
			search.setSyntaxType(Search.NATIVE);
			search.setDocumentLimit(1000);
			search.setUseQueryWarnings(false);
			search.setHighlightFlag(false);
			search.setExpandNormalizedTerms(true);
			search.setExactMatch(false);
			search.setIgnoreStopwords(false);
			search.setDuplicationFiltering(false);
			
			Progress progress = search.submit(true);
			
			while (!progress.isComplete())
			{
				progress = search.getProgress(null, 100);
			}
			final SearchResult result = search.getSearchResult();
			if (result != null)
			{
//				String[] metadataNames = new String[1500];
//				metadataNames = result.getMetaNames();
				int docsReturned = search.getDocsReturned();
				Document[] docs = result.getDocumentsInRange(1, docsReturned);
				System.out.println("For serial:" + serialNum);
				for (Document document : docs)
				{
//					System.out.println(document.getGuid());
					if (document.getCollection() != null)
					{
						String metadata = document.getMetaData();
						String component = getCompnentFromMetadata(metadata);
						//System.out.println("component=" + component);
						String prismClipDate = getPrismClipDateFromMetadata(metadata);
						//System.out.println("prismClipDate=" + prismClipDate);
						String pubcode = getPubCodeFromMetadata(metadata);
						//System.out.println("pubCode=" + pubcode);
						System.out.println("Document uuid:" + document.getGuid() + " is loaded to:" + document.getCollection() + " component:" + component + " prismClipDate:" + prismClipDate + " pubCode:" + pubcode );
						Collection<String> rfUuidCollect = new Vector<String>();
						
						System.out.println("output the citeInfo here");
//						rfUuidCollect.addAll(getRfUuidCollectFromMetadata(metadata));
//						if ((rfUuidCollect == null) || ((rfUuidCollect != null) && (rfUuidCollect.size() == 0)))
//						{
//							System.out.println("Metadata for this document is:" + metadata);
//							System.out.println("This document is not directly related to any rfUuids!");
//						}
//						else
//						{
//							System.out.println("This document is related to the following rfUuids:");
//							for (String rfUuid : rfUuidCollect)
//							{
//								System.out.println(rfUuid);
//							}
//						}
					}
				}
			}
			result.destroyResult();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		if (novus != null)
			novus.shutdownMQ();
	}

	public void reportDfUuidData(String dfUuid)
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

			Search search = novus.getSearch();
			Collection<String> collectionCollect = new Vector<String>();
			collectionCollect.addAll(GetSerialData.makeCollectionCollection());
			//collectionCollect.add("w_wl_names32_machv08");
			for (String currCollect : collectionCollect)
			{
				search.addCollection(currCollect);
			}

			search.setQueryType(search.BOOLEAN);
			String searchText = "=md.doc.family.uuid(" + dfUuid + ")";
			search.setQueryText(searchText);
			//search.setQueryText("=md.ccserial(1931123996)");
			search.setSyntaxType(Search.NATIVE);
			search.setDocumentLimit(1000);
			search.setUseQueryWarnings(false);
			search.setHighlightFlag(false);
			search.setExpandNormalizedTerms(true);
			search.setExactMatch(false);
			search.setIgnoreStopwords(false);
			search.setDuplicationFiltering(false);

			Progress progress = search.submit(true);

			while (!progress.isComplete())
			{
				progress = search.getProgress(null, 100);
			}

			final SearchResult result = search.getSearchResult();
			if (result != null)
			{
				//				String[] metadataNames = new String[1500];
				//				metadataNames = result.getMetaNames();
				int docsReturned = search.getDocsReturned();
				Document[] docs = result.getDocumentsInRange(1, docsReturned);
				for (Document document : docs)
				{
					if (document.getCollection() != null)
					{
						String metadata = document.getMetaData();
						String component = getCompnentFromMetadata(metadata);
						//System.out.println("component=" + component);
						String prismClipDate = getPrismClipDateFromMetadata(metadata);
						//System.out.println("prismClipDate=" + prismClipDate);
						String pubcode = getPubCodeFromMetadata(metadata);
						//System.out.println("pubCode=" + pubcode);
						System.out.println("Document uuid:" + document.getGuid() + " is loaded to:" + document.getCollection() + " component:" + component + " prismClipDate:" + prismClipDate + " pubCode:" + pubcode );
						Collection<String> rfUuidCollect = new Vector<String>();
						if ((rfUuidCollect == null) || ((rfUuidCollect != null) && (rfUuidCollect.size() == 0)))
						{
							//System.out.println("Metadata for this document is:" + metadata);
							System.out.println("This document is not directly related to any rfUuids!");
						}
						else
						{
							System.out.println("This document is related to the following rfUuids:");
							for (String rfUuid : rfUuidCollect)
							{
								System.out.println(rfUuid);
							}
						}
					}
				}
			}
			result.destroyResult();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		if (novus != null)
			novus.shutdownMQ();
	}

	public static Collection<String> makeCollectionCollection()
	{
		Collection<String> retCollect = new Vector<String>();
		retCollect.add("w_cs_so1");
		retCollect.add("w_cs_so2");
		retCollect.add("w_cs_atl1");
		retCollect.add("w_cs_atl2");
		retCollect.add("w_cs_fs1");
		retCollect.add("w_cs_fs2");
		retCollect.add("w_cs_fs3");
		retCollect.add("w_cs_fed1");
		retCollect.add("w_cs_fed2");
		retCollect.add("w_cs_fed3");
		//retCollect.add("w_an_rcc_alrfed");
		retCollect.add("w_an_rcc_alr1");
		retCollect.add("w_an_rcc_alr2");
		retCollect.add("w_an_rcc_alr3");
		retCollect.add("w_an_rcc_alr4");
		retCollect.add("w_an_rcc_alr5");
		retCollect.add("w_an_rcc_alr6");
		retCollect.add("w_an_rcc_alr7");
		retCollect.add("w_an_rcc_alrfed1");
		retCollect.add("w_an_rcc_alrfed2");
		retCollect.add("w_an_rcc_alrfed3");
		retCollect.add("w_cs_sct1");
		retCollect.add("w_cs_sct2");
		retCollect.add("w_cs_sct3");
		retCollect.add("w_cs_ftx1");
		retCollect.add("w_cs_ftx2");
		retCollect.add("w_cs_mjr1");
		retCollect.add("w_cs_trib");
		retCollect.add("w_cs_nys1");
		retCollect.add("w_cs_nys2");
		retCollect.add("w_cs_ne1");
		retCollect.add("w_cs_ne2");
		retCollect.add("w_cs_sw1");
		retCollect.add("w_cs_sw2");
		retCollect.add("w_cs_cal1");
		retCollect.add("w_cs_cal2");
		retCollect.add("w_cs_pac1");
		retCollect.add("w_cs_pac2");
		retCollect.add("w_cs_nw1");
		retCollect.add("w_cs_nw2");
		retCollect.add("w_cs_se1");
		retCollect.add("w_cs_se2");
		retCollect.add("w_adm_smacivcc");
		retCollect.add("w_adm_smisccc");
		retCollect.add("w_adm_scpdcc");
		retCollect.add("w_adm_swc");
		retCollect.add("w_adm_stx");
		retCollect.add("w_adm_sadm");
		retCollect.add("w_adm_fgcadm2");
		retCollect.add("w_adm_eeocguid");
		retCollect.add("w_adm_eeocmd");
		retCollect.add("w_adm_ofcc");
		retCollect.add("w_adm_nlrbdec");
		retCollect.add("w_adm_flbadm3");
		retCollect.add("w_adm_flbadm");
		retCollect.add("w_adm_nystax");
		retCollect.add("w_lt_br_cta1");
		retCollect.add("w_lt_br_sct");
		retCollect.add("w_lt_br_sctpet");
		retCollect.add("w_lt_br_tax");
		retCollect.add("w_adm_nlrbalj");
		retCollect.add("w_lt_br_stsct");
		retCollect.add("w_lt_br_stapp");
		retCollect.add("w_adm_fgcbca3");
		retCollect.add("w_adm_biadec2");
		retCollect.add("w_adm_fcomtemp1");
		retCollect.add("w_adm_mferc2");
		retCollect.add("w_adm_ftxirb");
		retCollect.add("w_adm_ftxwdk");
		retCollect.add("w_adm_fsecrels1");
		retCollect.add("w_cs_caor1");
		retCollect.add("w_cs_caor2");
		retCollect.add("w_3rd_wriatax");
		retCollect.add("w_adm_ssecc");
		retCollect.add("w_lt_td_filings");
		retCollect.add("w_lt_td_pleads");
		retCollect.add("w_lt_td_motions");
		retCollect.add("w_lt_td_jurinst");
		retCollect.add("w_lt_td_and");
		retCollect.add("w_lt_td_ew");
		//retCollect.add("w_adm_stb");  nothing was hooked to this collection - so I removed it from prod machv
		retCollect.add("w_adm_ftran");
		//retCollect.add("w_ct_maestro_dev1");  Test collection
		retCollect.add("w_adm_mspb");
		retCollect.add("w_3rd_jvjryalm");
		retCollect.add("w_3rd_jvjvw");
		retCollect.add("w_3rd_jvjrycal");
		retCollect.add("w_3rd_jvjryjas");
		retCollect.add("w_adm_fatr");
		retCollect.add("w_adm_eeock");
		retCollect.add("w_adm_aodk");
		retCollect.add("w_adm_fcppto2");
		retCollect.add("w_adm_vaeth");
		retCollect.add("w_adm_ftxccn");
		retCollect.add("w_adm_ftxirb2");
		retCollect.add("w_3rd_jvjrysta");
		retCollect.add("w_3rd_jvverd");
		retCollect.add("w_3rd_jvjryjvn");
		//retCollect.add("w_3rd_jvtrica");   novus says record not found
		//retCollect.add("w_3rd_jvjryjvrp");  nobud says record not found
		retCollect.add("w_3rd_jvjry3");
		retCollect.add("w_3rd_jvjryatl");
		retCollect.add("w_cs_waor1");
		retCollect.add("w_cs_waor2");
		retCollect.add("w_adm_epadie2");
		retCollect.add("w_adm_stmed5");
		retCollect.add("w_adm_fennrc2");
		retCollect.add("w_adm_ftxgcmk");
		retCollect.add("w_3rd_jvjrylrp");
		retCollect.add("w_adm_irsinfo");
		//retCollect.add("w_wl_names32_machv01");  novus search says this doesn't exist
//		retCollect.add("w_wl_names32_machv02");
//		retCollect.add("w_wl_names32_machv03");
//		retCollect.add("w_wl_names32_machv04");
//		retCollect.add("w_wl_names32_machv05");
//		retCollect.add("w_wl_names32_machv06");
//		retCollect.add("w_wl_names32_machv07");
//		retCollect.add("w_wl_names32_machv08");
//		retCollect.add("w_wl_names32_machv09");
//		retCollect.add("w_wl_names32_machv10");
		retCollect.add("w_adm_sba");
		retCollect.add("w_adm_ocaho");
		retCollect.add("w_adm_dojfcpa");
		retCollect.add("w_adm_balca");
		retCollect.add("w_adm_coast");
		retCollect.add("w_adm_fendoe");
		retCollect.add("w_adm_oshastate");
		retCollect.add("w_adm_flbsarox");
		retCollect.add("w_adm_ftranfaa");
		retCollect.add("w_3rd_bnacsm");
		//retCollect.add("w_3rd_bnacso");
//		retCollect.add("w_wl_df_machv32_01");
//		retCollect.add("w_wl_df_machv32_02");
//		retCollect.add("w_wl_df_machv32_03");
//		retCollect.add("w_wl_df_machv32_04");
//		retCollect.add("w_wl_df_machv32_05");
//		retCollect.add("w_wl_df_machv32_06");
//		retCollect.add("w_wl_df_machv32_07");
//		retCollect.add("w_wl_df_machv32_08");
//		retCollect.add("w_wl_df_machv32_09");
//		retCollect.add("w_wl_df_machv32_10");
		//
//		retCollect.add("w_wl_df_machv32_docket_01");
//		retCollect.add("w_wl_df_machv32_docket_02");
//		retCollect.add("w_wl_df_machv32_docket_03");
//		retCollect.add("w_wl_df_machv32_docket_04");
//		retCollect.add("w_wl_df_machv32_docket_05");
//
		retCollect.add("w_adm_fldbpr");
		retCollect.add("w_adm_ftxitdk");
		retCollect.add("w_adm_ftxicdk");
		retCollect.add("w_3rd_bnacsu");
		retCollect.add("w_cs_nyor1");
		retCollect.add("w_cs_nyor2");
		retCollect.add("w_cs_nyoram");
		retCollect.add("w_cs_nyorunr");
		retCollect.add("w_adm_ust2");
		retCollect.add("w_adm_ofia");
		retCollect.add("w_lt_td_orders");
//		retCollect.add("w_ct_j3_history01");
//		retCollect.add("w_ct_j3_history02");
//		retCollect.add("w_ct_j3_history03");
//		retCollect.add("w_ct_j3_history04");
//		retCollect.add("w_ct_j3_history05");
//		retCollect.add("w_wl_df_machv32_docket_01");
//		retCollect.add("w_wl_df_machv32_docket_02");
//		retCollect.add("w_wl_df_machv32_docket_03");
//		retCollect.add("w_wl_df_machv32_docket_04");
//		retCollect.add("w_wl_df_machv32_docket_05");
//		retCollect.add("w_wl_kc_citeref01");
//		retCollect.add("w_wl_kc_citeref02");
//		retCollect.add("w_wl_kc_citeref03");
//		retCollect.add("w_wl_kc_citeref04");
//		retCollect.add("w_wl_kc_citeref05");
//		retCollect.add("w_wl_kc_citeref06");
//		retCollect.add("w_wl_kc_citeref07");
//		retCollect.add("w_wl_kc_citeref08");
//		retCollect.add("w_wl_kc_citeref09");
//		retCollect.add("w_wl_kc_citeref10");
//		retCollect.add("w_wl_kc_citeref11");
//		retCollect.add("w_wl_kc_citeref12");
//		retCollect.add("w_wl_kc_citeref13");
//		retCollect.add("w_wl_kc_citeref14");
//		retCollect.add("w_wl_kc_citeref15");
//		retCollect.add("w_wl_kc_citeref16");
//		retCollect.add("w_wl_kc_citeref17");
//		retCollect.add("w_wl_kc_citeref18");
//		retCollect.add("w_wl_kc_citeref19");
//		retCollect.add("w_wl_kc_citeref20");
//		retCollect.add("w_wl_kc_citeref21");
//		retCollect.add("w_wl_kc_citeref22");
//		retCollect.add("w_wl_kc_citeref23");
//		retCollect.add("w_wl_kc_citeref24");
//		retCollect.add("w_wl_kc_citeref25");
//		retCollect.add("w_wl_kc_citeref26");
//		retCollect.add("w_wl_kc_citeref27");
//		retCollect.add("w_wl_kc_citeref28");
		retCollect.add("w_lt_br_sctoa");
		retCollect.add("w_lt_td_orders");
		retCollect.add("w_lt_td_tr");
		retCollect.add("w_cs_mashp");
		retCollect.add("w_3rd_bnacsuk");
		retCollect.add("w_lt_td_avoa");
		retCollect.add("w_adm_untrib");
		retCollect.add("w_adm_ict");
		retCollect.add("w_adm_inticj");
		retCollect.add("w_cs_iccretro");
		retCollect.add("w_cs_pucstk");
//		retCollect.add("w_cs_pura1k");
//		retCollect.add("w_cs_purcomb");
//		retCollect.add("w_cs_purretro");
//		retCollect.add("w_cs_purstk");
		retCollect.add("w_cs_pucretro");
		retCollect.add("w_lt_td_avtt");
		retCollect.add("w_3rd_bnaadk");
		retCollect.add("w_3rd_bnaebck");
		retCollect.add("w_3rd_bnafepk");
		retCollect.add("w_3rd_bnaierk");
		retCollect.add("w_3rd_bnalrrmk");
		retCollect.add("w_3rd_bnawhk");
		retCollect.add("w_3rd_bnalrrm");
		retCollect.add("w_3rd_bnafep");
		retCollect.add("w_3rd_bnaebc");
		retCollect.add("w_3rd_bnaad");
		retCollect.add("w_3rd_bnawh");
		retCollect.add("w_3rd_bnaier");
		retCollect.add("w_cs_tribal");
		retCollect.add("w_cs_markonly");
		//retCollect.add("w_3rd_amcadnon");
		//retCollect.add("w_3rd_amcadkc");
		//retCollect.add("w_3rd_amccskc");
		//retCollect.add("w_3rd_amccsnon");
		//retCollect.add("w_3rd_amcuknon");
		//retCollect.add("w_3rd_amcother");
		//retCollect.add("w_3rd_amcinnon");
		retCollect.add("w_cs_wpadcad");
		retCollect.add("w_cs_wpadca");
		retCollect.add("w_cs_wpadceq");
		retCollect.add("w_cs_cucccs");
		retCollect.add("w_cs_cucc3c");
		retCollect.add("w_cs_qprspank");
		retCollect.add("w_cs_wprspank");
		retCollect.add("w_cs_wpreng");
//		retCollect.add("w_3rd_pct");
//		retCollect.add("w_adm_smaarb");
		retCollect.add("w_cs_puertoag");
		retCollect.add("w_3rd_bnalar");
		retCollect.add("w_3rd_bnaadmeq");
		retCollect.add("w_3rd_bnaadmon");
		retCollect.add("w_lt_td_aa");
		retCollect.add("w_3rd_fsrcc");
//		retCollect.add("w_3rd_usft33");
		//retCollect.add("w_lt_td_excfres");
		retCollect.add("w_lt_td_introg");
//		retCollect.add("w_3rd_usft34");
//		retCollect.add("w_3rd_usft35");
//		retCollect.add("w_3rd_usft36");
//		retCollect.add("w_3rd_usft37");
//		retCollect.add("w_3rd_usft38");
//		retCollect.add("w_3rd_usft39");
//		retCollect.add("w_3rd_usft40");
//		retCollect.add("w_3rd_usft41");
//		retCollect.add("w_3rd_usft42");
//		retCollect.add("w_3rd_usft43");
//		retCollect.add("w_3rd_usft44");
//		retCollect.add("w_3rd_usft45");
//		retCollect.add("w_3rd_usft46");
//		retCollect.add("w_3rd_usft47");
//		retCollect.add("w_3rd_usft48");
//		retCollect.add("w_3rd_usft49");
//		retCollect.add("w_3rd_usft50");
//		retCollect.add("w_3rd_usft51");
//		retCollect.add("w_3rd_usft52");
//		retCollect.add("w_3rd_usft53");
//		retCollect.add("w_3rd_usft54");
//		retCollect.add("w_3rd_usft55");
//		retCollect.add("w_3rd_usft56");
//		retCollect.add("w_3rd_usft57");
//		retCollect.add("w_3rd_usft58");
//		retCollect.add("w_3rd_usft59");
//		retCollect.add("w_3rd_usft60");
//		retCollect.add("w_3rd_usft61");
//		retCollect.add("w_3rd_usft61k");
//		retCollect.add("w_3rd_usft62");
//		retCollect.add("w_3rd_usft62k ");
//		retCollect.add("w_3rd_usft63k");
//		retCollect.add("w_3rd_usft63k ");
//		retCollect.add("w_3rd_usft64k");
//		retCollect.add("w_3rd_usft64k");
//		retCollect.add("w_3rd_usft65k");
//		retCollect.add("w_3rd_usft65k");
//		retCollect.add("w_3rd_usft66k");
//		retCollect.add("w_3rd_usft66k");
//		retCollect.add("w_3rd_usft67k");
//		retCollect.add("w_3rd_usft67k");
//		retCollect.add("w_3rd_usft68k");
//		retCollect.add("w_3rd_usft68k");
//		retCollect.add("w_3rd_usft69k");
//		retCollect.add("w_3rd_uspa2001");
//		retCollect.add("w_3rd_uspa2002");
//		retCollect.add("w_3rd_uspa2002");
//		retCollect.add("w_3rd_uspa2003");
//		retCollect.add("w_3rd_uspa2004a");
//		retCollect.add("w_3rd_uspa2004");
//		retCollect.add("w_3rd_uspa2005a");
//		retCollect.add("w_3rd_uspa2005a");
//		retCollect.add("w_3rd_uspa2005a");
//		retCollect.add("w_3rd_uspa2006a");
//		retCollect.add("w_3rd_uspa2006a");
//		retCollect.add("w_3rd_uspa2007");
//		retCollect.add("w_3rd_uspa2007");
//		retCollect.add("w_3rd_uspa2008");
//		retCollect.add("w_3rd_uspa2008");
//		retCollect.add("w_3rd_uspa2008");
//		retCollect.add("w_3rd_uspa2009");
		retCollect.add("w_lt_td_exhbts");
		retCollect.add("w_lt_td_exhabs");
		retCollect.add("w_lt_br_tdja");
		retCollect.add("w_3rd_wormslp");
		retCollect.add("w_3rd_wormi");
		retCollect.add("w_adm_aaarbawd");
		retCollect.add("w_adm_fntitc2k");
		retCollect.add("w_lt_td_ipadmin");
		retCollect.add("w_adm_aftrdock");
		retCollect.add("w_adm_aftrdoc");
		retCollect.add("w_3rd_gsissr");
		retCollect.add("w_cs_purbvstk");
		retCollect.add("w_lt_td_pleads2");
		retCollect.add("w_lt_br_cta2");
		retCollect.add("w_lt_br_cta3");
		retCollect.add("w_lt_br_stapp2");
		retCollect.add("w_lt_br_stapp3");
		retCollect.add("w_adm_ncadmdec");
		retCollect.add("w_adm_smaas");
		retCollect.add("w_lt_td_motions2");
		retCollect.add("w_lt_td_motions3");
		retCollect.add("w_cs_smftoc2");
		retCollect.add("w_cs_cawccr");
		retCollect.add("w_cs_iccretro2");
		retCollect.add("w_adm_paoor");
//		retCollect.add("ipa_bogus");
		retCollect.add("w_3rd_jvjvrp");
		retCollect.add("w_adm_patdec");
		retCollect.add("w_cs_flbcshis");
		retCollect.add("w_lt_td_pucflg");
		retCollect.add("w_adm_ptab");
		retCollect.add("w_cs_puctest");
		retCollect.add("w_cs_puctst2");
		retCollect.add("anz_au_cases");
		return retCollect;
	}

	private static String COMPONENT_START_ELEMENT = "<md.wl.database.identifier>";
	private static String COMPONENT_END_ELEMENT = "</md.wl.database.identifier>";
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
private static String ADDED_DATE_START_ELEMENT = "<md.publisheddatetime>";
private static String ADDED_DATE_END_ELEMENT = "</md.publisheddatetime>";
/**
 * @param metadata
 * @return
 */
private String getAdddedDateFromMetadata(String metadata)
{
	String emptyAddedDate = "00000000000000";
	String retVal = null;
	if (metadata.indexOf(ADDED_DATE_START_ELEMENT) > -1)
	{
		int start = metadata.indexOf(ADDED_DATE_START_ELEMENT) + ADDED_DATE_START_ELEMENT.length();
		int end = metadata.indexOf(ADDED_DATE_END_ELEMENT);
		retVal = metadata.substring(start, end);
		if ((retVal != null) && (retVal.compareTo(emptyAddedDate) == 0))
			retVal = null;
	}
	else
		;
	
	return retVal;
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

// I DONT' THINK WE CARE ABOUT THIS - GET THE RF UUIDS FROM THE CITES
//private static String RF_UUID_START_ELEMENT = "<md.rendition.uuid>";
//private static String RF_UUID_END_ELEMENT = "</md.rendition.uuid>";
///**
// * @param metadata
// * @return
// */
//private Collection<String> getRfUuidollectFromMetadata(
//		String metadata)
//{
//HERE
//	String tempMetadata = metadata;
//	Collection<String> retCollect = new Vector<String>();
//	while (tempMetadata.indexOf(RF_UUID_START_ELEMENT) > -1)
//	{
//		int start = tempMetadata.indexOf(RF_UUID_START_ELEMENT) + RF_UUID_START_ELEMENT.length();
//		int end = tempMetadata.indexOf(RF_UUID_END_ELEMENT);
//		retCollect.add(tempMetadata.substring(start, end));
//		tempMetadata = tempMetadata.substring(end + RF_UUID_END_ELEMENT.length());
//	}
//	return retCollect;
//}
public static Connection getSimpleConnection() 
{
	//See your driver documentation for the proper format of this string :
	//Provided by your driver documentation. In this case, a MySql driver is used : 
	String DRIVER_CLASS_NAME = "oracle.jdbc.driver.OracleDriver";
	String USER_NAME = "machv_user";
	String PASSWORD = "machv_user";
	String JRECS_USER_NAME = "jrecs_user";
	String JRECS_PASSWORD = "jrecs_user";
	String conStr = "";
	USER_NAME = "MDS_USER";
	PASSWORD = "MDSP68";
	conStr = GetSerialData.PROD_CONN_STRING;
	
	Connection result = null;
	try 
	{
		Class.forName(DRIVER_CLASS_NAME).newInstance();
	}
	catch (Exception ex)
	{
		System.out.println("Check classpath. Cannot load db driver: " + DRIVER_CLASS_NAME);
	}

	for (int attempt = 0; attempt < 10; attempt++)
	{
		try 
		{
			result = DriverManager.getConnection(conStr, USER_NAME, PASSWORD);
			if (result != null)
				return result;
		}
		catch (SQLException e)
		{
			System.out.println( "Driver loaded, but cannot connect to db: username/pass=" + USER_NAME + "/" + PASSWORD + " " + conStr + " msg=" + e.getMessage() + "end msg -  sleep for 2 secs");
			try
			{
				Thread.sleep(2000);
			}
			catch (InterruptedException e1)
			{
				;
			}
		}
	}
	System.out.println("**** COULD NOT GET A CONNECTION ******");
	return result;
}
public class NovusData
{
	Collection<NovusDocData> nDocDataCollect = new Vector<NovusDocData>();
	// serNum really is the 10 serial number 
	String serNum = "";
	
	public String getSerNum() {
		return serNum;
	}

	public void setSerNum(String serNum) {
		this.serNum = serNum;
	}

	/* serialNum is really the caseUuid*/ 
	String serialNum = "";
	
	/**
	 * serialNum is really the caseUuid
	 */
	public NovusData(String serialNum)
	{
		this.serialNum = serialNum; 
	}

	/**
	 * @return what is really the caseUuid
	 */
	public String getSerialNum()
	{
		return serialNum;
	}
	/**
	 * @return the nDocDataCollect
	 */
	public Collection<NovusDocData> getNDocDataCollect()
	{
		return nDocDataCollect;
	}
	public void addNovusDocData(NovusDocData ndd)
	{
		nDocDataCollect.add(ndd);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + getOuterType().hashCode();
		result = prime * result
				+ ((nDocDataCollect == null) ? 0 : nDocDataCollect.hashCode());
		result = prime * result
				+ ((serialNum == null) ? 0 : serialNum.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (!(obj instanceof NovusData))
		{
			return false;
		}
		NovusData other = (NovusData) obj;
		if (!getOuterType().equals(other.getOuterType()))
		{
			return false;
		}
		if (nDocDataCollect == null)
		{
			if (other.nDocDataCollect != null)
			{
				return false;
			}
		}
		else if (!nDocDataCollect.equals(other.nDocDataCollect))
		{
			return false;
		}
		if (serialNum == null)
		{
			if (other.serialNum != null)
			{
				return false;
			}
		}
		else if (!serialNum.equals(other.serialNum))
		{
			return false;
		}
		return true;
	}

	private GetSerialData getOuterType()
	{
		return GetSerialData.this;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "NovusData ["
				+ (nDocDataCollect != null ? "nDocDataCollect="
						+ nDocDataCollect + ", " : "")
				+ (serialNum != null ? "serialNum=" + serialNum : "") + "]";
	}

}
public class NovusCiteData
{
	String rfUuid = null;
	String pubCode = null;
	String volume = null;
	String page = null;
	String caseUuid = null;
	boolean isParallelOnly = false;
	boolean isPrimary = false;
	// if both isParallelOnly is false AND isPrimary is false - then this cite is not used.
	/**
	 * 
	 */
	public NovusCiteData()
	{
	}
	/**
	 * @param rfUuid
	 * @param pubCode
	 * @param caseUuid
	 * @param isParallelOnly
	 * @param isPrimary
	 */
	public NovusCiteData(String rfUuid, String pubCode, String caseUuid,
			boolean isParallelOnly, boolean isPrimary, String vol, String pg)
	{
		this.rfUuid = rfUuid;
		this.pubCode = pubCode;
		this.volume = vol;
		this.page = pg;
		this.caseUuid = caseUuid;
		this.isParallelOnly = isParallelOnly;
		this.isPrimary = isPrimary;
	}
	/**
	 * @return the rfUuid
	 */
	public String getRfUuid()
	{
		return rfUuid;
	}
	/**
	 * @param rfUuid the rfUuid to set
	 */
	public void setRfUuid(String rfUuid)
	{
		this.rfUuid = rfUuid;
	}
	/**
	 * @return the pubCode
	 */
	public String getPubCode()
	{
		return pubCode;
	}
	/**
	 * @param pubCode the pubCode to set
	 */
	public void setPubCode(String pubCode)
	{
		this.pubCode = pubCode;
	}
	/**
	 * @return the caseUuid
	 */
	public String getCaseUuid()
	{
		return caseUuid;
	}
	/**
	 * @param caseUuid the caseUuid to set
	 */
	public void setCaseUuid(String caseUuid)
	{
		this.caseUuid = caseUuid;
	}
	/**
	 * @return the isParallelOnly
	 */
	public boolean isParallelOnly()
	{
		return isParallelOnly;
	}
	/**
	 * @param isParallelOnly the isParallelOnly to set
	 */
	public void setParallelOnly(boolean isParallelOnly)
	{
		this.isParallelOnly = isParallelOnly;
	}
	/**
	 * @return the isPrimary
	 */
	public boolean isPrimary()
	{
		return isPrimary;
	}
	/**
	 * @param isPrimary the isPrimary to set
	 */
	public void setPrimary(boolean isPrimary)
	{
		this.isPrimary = isPrimary;
	}
	/**
	 * @return the volume
	 */
	public String getVolume()
	{
		return volume;
	}
	/**
	 * @param volume the volume to set
	 */
	public void setVolume(String volume)
	{
		this.volume = volume;
	}
	/**
	 * @return the page
	 */
	public String getPage()
	{
		return page;
	}
	/**
	 * @param page the page to set
	 */
	public void setPage(String page)
	{
		this.page = page;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "NovusCiteData ["
				+ (rfUuid != null ? "rfUuid=" + rfUuid + ", " : "")
				+ (pubCode != null ? "pubCode=" + pubCode + ", " : "")
				+ (volume != null ? "volume=" + volume + ", " : "")
				+ (page != null ? "page=" + page + ", " : "")
				+ (caseUuid != null ? "caseUuid=" + caseUuid + ", " : "")
				+ "isParallelOnly=" + isParallelOnly + ", isPrimary="
				+ isPrimary + "]";
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + getOuterType().hashCode();
		result = prime * result
				+ ((caseUuid == null) ? 0 : caseUuid.hashCode());
		result = prime * result + (isParallelOnly ? 1231 : 1237);
		result = prime * result + (isPrimary ? 1231 : 1237);
		result = prime * result + ((page == null) ? 0 : page.hashCode());
		result = prime * result + ((pubCode == null) ? 0 : pubCode.hashCode());
		result = prime * result + ((rfUuid == null) ? 0 : rfUuid.hashCode());
		result = prime * result + ((volume == null) ? 0 : volume.hashCode());
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (!(obj instanceof NovusCiteData))
		{
			return false;
		}
		NovusCiteData other = (NovusCiteData) obj;
		if (!getOuterType().equals(other.getOuterType()))
		{
			return false;
		}
		if (caseUuid == null)
		{
			if (other.caseUuid != null)
			{
				return false;
			}
		}
		else if (!caseUuid.equals(other.caseUuid))
		{
			return false;
		}
		if (isParallelOnly != other.isParallelOnly)
		{
			return false;
		}
		if (isPrimary != other.isPrimary)
		{
			return false;
		}
		if (page == null)
		{
			if (other.page != null)
			{
				return false;
			}
		}
		else if (!page.equals(other.page))
		{
			return false;
		}
		if (pubCode == null)
		{
			if (other.pubCode != null)
			{
				return false;
			}
		}
		else if (!pubCode.equals(other.pubCode))
		{
			return false;
		}
		if (rfUuid == null)
		{
			if (other.rfUuid != null)
			{
				return false;
			}
		}
		else if (!rfUuid.equals(other.rfUuid))
		{
			return false;
		}
		if (volume == null)
		{
			if (other.volume != null)
			{
				return false;
			}
		}
		else if (!volume.equals(other.volume))
		{
			return false;
		}
		return true;
	}
	private GetSerialData getOuterType()
	{
		return GetSerialData.this;
	}
}
public class NovusDocData
{
	String docUuid = null;
	String component = null;
	String collection = null;
	String prismClipDate = null;
	String pubCode = null;
	boolean isALRData = false;
	String caseUuid = null;
	String addedDate = null;
	Collection<NovusCiteData> novusCiteDataCollect = new Vector<NovusCiteData>();
	/**
	 * @param docUuid
	 * @param component
	 * @param prismClipDate
	 * @param pubCode
	 * @param rfUuidCollect
	 */
	public NovusDocData(String docUuid, String component, String collection,
			String prismClipDate, String pubCode, String addDate,
			Collection<NovusCiteData> citeDataCollect)
	{
		this.docUuid = docUuid;
		this.component = component;
		this.collection = collection;
		this.prismClipDate = prismClipDate;
		this.pubCode = pubCode;
		this.addedDate = addDate;
		this.novusCiteDataCollect.addAll(citeDataCollect);
	}
	public String getAddedDate()
	{
		return addedDate;
	}
	public void setAddedDate(String addedDate)
	{
		this.addedDate = addedDate;
	}
	/**
	 * @param citeDataCollect
	 */
	public void addNovusCiteData(Collection<NovusCiteData> citeDataCollect)
	{
		this.novusCiteDataCollect.addAll(citeDataCollect);
	}
	public void addNovusCiteData(NovusCiteData ncd)
	{
		this.novusCiteDataCollect.add(ncd);
	}
	public NovusDocData()
	{
	}
	/**
	 * @return the docUuid
	 */
	public String getDocUuid()
	{
		return docUuid;
	}
	/**
	 * @param docUuid the docUuid to set
	 */
	public void setDocUuid(String docUuid)
	{
		this.docUuid = docUuid;
	}
	/**
	 * @return the component
	 */
	public String getComponent()
	{
		return component;
	}
	/**
	 * @param component the component to set
	 */
	public void setComponent(String component)
	{
		this.component = component;
	}
	/**
	 * @return the collection
	 */
	public String getCollection()
	{
		return collection;
	}
	/**
	 * @param collection the collection to set
	 */
	public void setCollection(String collection)
	{
		this.collection = collection;
	}
	/**
	 * @return the prismClipDate
	 */
	public String getPrismClipDate()
	{
		return prismClipDate;
	}
	/**
	 * @param prismClipDate the prismClipDate to set
	 */
	public void setPrismClipDate(String prismClipDate)
	{
		this.prismClipDate = prismClipDate;
	}
	/**
	 * @return the pubCode
	 */
	public String getPubCode()
	{
		return pubCode;
	}
	/**
	 * @param pubCode the pubCode to set
	 */
	public void setPubCode(String pubCode)
	{
		this.pubCode = pubCode;
	}
	/**
	 * @return the novusCiteDataCollect
	 */
	public Collection<NovusCiteData> getNovusCiteDataCollect()
	{
		return novusCiteDataCollect;
	}
	/**
	 * @param novusCiteDataCollect the novusCiteDataCollect to set
	 */
	public void setNovusCiteDataCollect(
			Collection<NovusCiteData> novusCiteDataCollect)
	{
		this.novusCiteDataCollect = novusCiteDataCollect;
	}
	@Override
	public String toString()
	{
		return "NovusDocData [docUuid=" + docUuid + ", component=" + component + ", collection=" + collection
				+ ", prismClipDate=" + prismClipDate + ", pubCode=" + pubCode + ", isALRData=" + isALRData
				+ ", caseUuid=" + caseUuid + ", addedDate=" + addedDate + ", novusCiteDataCollect="
				+ novusCiteDataCollect + "]";
	}
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + getEnclosingInstance().hashCode();
		result = prime * result + ((addedDate == null) ? 0 : addedDate.hashCode());
		result = prime * result + ((caseUuid == null) ? 0 : caseUuid.hashCode());
		result = prime * result + ((collection == null) ? 0 : collection.hashCode());
		result = prime * result + ((component == null) ? 0 : component.hashCode());
		result = prime * result + ((docUuid == null) ? 0 : docUuid.hashCode());
		result = prime * result + (isALRData ? 1231 : 1237);
		result = prime * result + ((novusCiteDataCollect == null) ? 0 : novusCiteDataCollect.hashCode());
		result = prime * result + ((prismClipDate == null) ? 0 : prismClipDate.hashCode());
		result = prime * result + ((pubCode == null) ? 0 : pubCode.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NovusDocData other = (NovusDocData) obj;
		if (!getEnclosingInstance().equals(other.getEnclosingInstance()))
			return false;
		if (addedDate == null)
		{
			if (other.addedDate != null)
				return false;
		}
		else if (!addedDate.equals(other.addedDate))
			return false;
		if (caseUuid == null)
		{
			if (other.caseUuid != null)
				return false;
		}
		else if (!caseUuid.equals(other.caseUuid))
			return false;
		if (collection == null)
		{
			if (other.collection != null)
				return false;
		}
		else if (!collection.equals(other.collection))
			return false;
		if (component == null)
		{
			if (other.component != null)
				return false;
		}
		else if (!component.equals(other.component))
			return false;
		if (docUuid == null)
		{
			if (other.docUuid != null)
				return false;
		}
		else if (!docUuid.equals(other.docUuid))
			return false;
		if (isALRData != other.isALRData)
			return false;
		if (novusCiteDataCollect == null)
		{
			if (other.novusCiteDataCollect != null)
				return false;
		}
		else if (!novusCiteDataCollect.equals(other.novusCiteDataCollect))
			return false;
		if (prismClipDate == null)
		{
			if (other.prismClipDate != null)
				return false;
		}
		else if (!prismClipDate.equals(other.prismClipDate))
			return false;
		if (pubCode == null)
		{
			if (other.pubCode != null)
				return false;
		}
		else if (!pubCode.equals(other.pubCode))
			return false;
		return true;
	}
	/**
	 * @return the isALRData
	 */
	public boolean isALRData()
	{
		return isALRData;
	}
	/**
	 * @param isALRData the isALRData to set
	 */
	public void setALRData(boolean isALRData)
	{
		this.isALRData = isALRData;
	}
	/**
	 * @return the caseUuid
	 */
	public String getCaseUuid()
	{
		return caseUuid;
	}
	/**
	 * @param caseUuid the caseUuid to set
	 */
	public void setCaseUuid(String caseUuid)
	{
		this.caseUuid = caseUuid;
	}
	private GetSerialData getEnclosingInstance()
	{
		return GetSerialData.this;
	}
}
}