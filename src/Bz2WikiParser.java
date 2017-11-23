package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

// parses bz2 Wikipages
public class Bz2WikiParser {
	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only pages not having (~).
		namePattern = Pattern.compile("^([^~]+)$");
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	static XMLReader xmlReader; 

	static int i =0;

	Bz2WikiParser(){
		try{
		// initializing in constructor so that it reduces overall running time of parser 
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setFeature(
				"http://apache.org/xml/features/nonvalidating/load-external-dtd",
				false);
		SAXParser saxParser = spf.newSAXParser();
		xmlReader = saxParser.getXMLReader();
		
		}catch(Exception e)
		{
			
		}
	}

	public String parseLine(String inputLine) {
		//System.out.print(i++ + ", ");
		String opLine = "";
		
		try{		
		// 	SAXParserFactory spf = SAXParserFactory.newInstance();
		// spf.setFeature(
		// 		"http://apache.org/xml/features/nonvalidating/load-external-dtd",
		// 		false);
		// SAXParser saxParser = spf.newSAXParser();
		// XMLReader xmlReader = saxParser.getXMLReader();
		List<String> linkPageNames = new LinkedList<>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));		
		
		String line = inputLine;		
		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		if (matcher.find()) {
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
			}
			
			StringBuffer output = new StringBuffer(pageName+":");
			for(String page : linkPageNames)
				output.append(page+",");
			
			opLine = output.toString();
		}
		}catch(Exception e)
		{
			}
		return opLine;
	}

	// Parses a Wikipage and find outlinks in bodyContent
	private static class WikiParser extends DefaultHandler {
		// List of linked pages
		private List<String> linkPageNames;
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName)
					&& "bodyContent"
							.equalsIgnoreCase(attributes.getValue("id"))
					&& count == 0) {
				// bodyContent start
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// increase bodyContent element count
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					
				}
				// matching with link regex to ensure proper format
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// until inside body content
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// bodyContent end
				count--;
			}
		}
	}
}
