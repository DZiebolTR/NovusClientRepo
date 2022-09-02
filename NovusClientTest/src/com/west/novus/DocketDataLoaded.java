/**
 * 
 */
package com.west.novus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

/**
 * @author 0014135
 *
 */
public class DocketDataLoaded
{

	// moogle url
	//http://c474ummctasdf.int.thomsonreuters.com:8089/moogle/nims?collection=w_wl_df_machv32&id=I0024f1e4cd8411eb850ac132f535d1eb
	private static String moogleUrlBase = "http://c474ummctasdf.int.thomsonreuters.com:8089/moogle/nims?collection=w_wl_df_machv32&id=";
	public static String crlf = System.getProperty("line.separator");

	// Loaded file
	//		File outputFile = new File("c:\\Data\\DocketLoadedToDFNLUUIDData.txt");
	// Need to Load file
	//		File outputFile = new File("c:\\Data\\DocketLoadToDFNLUUIDData.txt");
	// Not Loaded file
	//		File outputFile = new File("c:\\Data\\DocketNotLoadedToDFNLUUIDData.txt");

	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		File currFile = new File("c:\\data\\TestDocketDfUuidList.txt");
		DocketDataLoaded currDD = new DocketDataLoaded();
		HttpClient client = new HttpClient();
		try
		{
			BufferedReader reader = null;
			reader = new BufferedReader(new FileReader(currFile));

			String currentDocketDfUuid = reader.readLine();
			while(currentDocketDfUuid != null)
			{
				System.out.println("current Docket DfUuid:" + currentDocketDfUuid);
				String fullURL = moogleUrlBase + currentDocketDfUuid;
				
				GetMethod method = new GetMethod(fullURL);
				int statusCode = client.executeMethod(method);
				//assertEquals("HTTP Get failed", HttpStatus.SC_OK, statusCode);
				if (statusCode == HttpStatus.SC_OK)
				{
					String response = method.getResponseBodyAsString();
					boolean docLoaded = false;
					if (response.indexOf(NOTHING_FOUND) >= 0)
						writeToNotLoadedFile(currentDocketDfUuid);
					else
					{
						if (currentDocketDfUuid.compareTo("I0007d9e5c56311ea8c24c7be4f705cad") == 0)
							System.out.println("this is the one");
						if (isDocLoaded(response))
							writeToGoodFile(currentDocketDfUuid);
						else
							writeToLoadFile(currentDocketDfUuid);
					}
				}
				else
					System.out.println("Bad status code returned from url for docketDfUuid:" + currentDocketDfUuid);
				//System.out.println(response);
				currentDocketDfUuid = reader.readLine();
				
			}
			if (reader != null)
			{
				reader.close();
				reader = null;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			String msg = currDD.getClass().getName();
			StackTraceElement[] callingFrame = Thread.currentThread()
					.getStackTrace();
			msg += "." + callingFrame[1].getMethodName() + "() encountered:"
					+ e.getClass().getName() + " with msg=" + e.getMessage();
			if (e.getCause() != null)
				msg += " with cause=" + e.getCause().getClass().getName()
				+ " with cause msg=" + e.getCause().getMessage();
			System.out.println(msg);
		}

		System.out.println("Done");
		System.out.println();

	}
	private static void writeToNotLoadedFile(String currentDocketDfUuid)
	{
		File outputFile = new File("c:\\Data\\DocketNotLoadedToDFNLUUIDData.txt");
		BufferedWriter writer = null;
		
		//String reportLine = nd.getSerNum() + ", "  + nd.getSerialNum() + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (currentDocketDfUuid != null)
			{				
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(lineStart + currentDocketDfUuid + lineEnd + crlf);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = "DocketDataLoaded." + "writeToLoadedFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static String lineStart = "sendIt(\"";
	private static String lineEnd = "\", sender);";
	private static void writeToLoadFile(String currentDocketDfUuid)
	{
		File outputFile = new File("c:\\Data\\DocketLoadToDFNLUUIDData.txt");
		BufferedWriter writer = null;
		
		//String reportLine = nd.getSerNum() + ", "  + nd.getSerialNum() + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (currentDocketDfUuid != null)
			{				
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(lineStart + currentDocketDfUuid + lineEnd + crlf);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = "DocketDataLoaded." + "writeToLoadedFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static void writeToGoodFile(String currentDocketDfUuid)
	{
		File outputFile = new File("c:\\Data\\DocketLoadedToDFNLUUIDData.txt");
		BufferedWriter writer = null;
		
		//String reportLine = nd.getSerNum() + ", "  + nd.getSerialNum() + crlf;
		try
		{
			if (!outputFile.exists())
			{
				outputFile.createNewFile();			
			}
			if (currentDocketDfUuid != null)
			{				
				writer = new BufferedWriter(new FileWriter(outputFile, true));
				writer.write(currentDocketDfUuid + crlf);
				writer.close();
			}
		}
		catch (IOException e1)
		{
			e1.printStackTrace();
			String msg = "DocketDataLoaded." + "writeToLoadedFile()" + " encountered:" + e1.getClass().getName() + 
					" with msg=" + e1.getMessage();
			if (e1.getCause() != null)
				msg += " with cause=" + e1.getCause().getClass().getName() + " with cause msg=" +  e1.getCause().getMessage();
			System.out.println(msg);
		}
	}
	private static String DOC_ID_START = "<doc.id"; 
	private static String DOC_ID_END = "</doc.id>";
	private static String NOTHING_FOUND = "<result>nothing found</result>";
	private static boolean isDocLoaded(String response)
	{
		if (response.indexOf(DOC_ID_START) > 0)
			return true;
		else 
			return false;
	}

	
	

	
	
	
	
}
