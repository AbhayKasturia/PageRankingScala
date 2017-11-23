package main

import collection.immutable.BitSet

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.{ DataInput, DataOutput, IOException }
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    val sc = new SparkContext(conf)

    val numPattern = "^([^~]+)$";

    val linkPattern = ("^\\..*/([^~]+)\\.html$")

    // val inputPages = sc.textFile(args(0)).map(x => {
    //                                                 val index = x.indexOf(":")
    //                                                 val page_name =  x.substring(0,index)
    //                                                 val page =  x.substring(index)
    //                                                 (page_name , page)})
    //                                        .filter(x => {
    //                                           val (k , v) = x
    //                                           (k.matches(numPattern))})
    //                                        .map{x=>{
    //                                           val (k , v) = x
    //                                         try {
    //                                             //v "div" 
    //                                             val xml = scala.xml.XML.loadString(v.trim)
    //                                             xml
    //                                          } catch {
    //                                               case _: Throwable => println("Error")
    //                                                //
    //                                          }}}
    // inputPages.saveAsTextFile(args(1))

    // read bz2 input file sorce
    val inputPages = sc.textFile(args(0))

    // parse each line of the input page using the java parser
    val parsePage = inputPages.mapPartitions(iterator => {
                                                  //creating an object of the parser class
                                                  val bz2: Bz2WikiParser = new Bz2WikiParser()
                                                  // using the iterator to apply parseLine from java class to each line 
                                                  iterator.map{line => bz2.parseLine(line)}})

    // process the parsed pages to get the key value pair as the (page name and the array of it's outlinks)
    val processedPages = parsePage // removing documents which are null
                                  .filter(doc => !(doc == ""))
                                  .map(doc => {
                                    val page = doc.substring(0,doc.indexOf(":"))
                                    (page , doc)})
                  								.map(x => {
                                      val (page , doc) = x
                                      if (doc.length > (page.length+1)) {
                                      /// getting the outlinks from the second part of the document 
                                      val outlinks = doc.substring(doc.indexOf(":")+1)
								                      (page, outlinks.split(","))
									                  } else
                                        // when outlinks are null ,  it is a sink node! 
									                     (page, Array(""))
                                        // adding persist to retain this data for future operations , avoiding lazy approach
                  									}).persist()

    // keeping count of total Documents for calculations
    val totalDocs = processedPages.count
    // intial page rank 
    val initPR = 1.0 / totalDocs
    // pages that we have to traverse on
    var pages = processedPages.map(page => (page._1, (page._2, initPR))).persist()
    // we need to do 10 iterations before we converge
    val totalIter = 10
    // setting the initial value of lambda to be 0.15
    val lambda = 0.15

    val size = inputPages.count

    for (iteration <- 1 to totalIter) {

      // filter the dangling nodes , map to get their page ranks and then sum it
      	var dangSum = pages.filter(x => {
                                    val (page, (outlinks, pr)) = x
                                    (outlinks(0) == "")})
                           .map(x => { val (page, (outlinks, pr)) = x
                                      pr})
                           .sum()
                                 
        // filter non dangling nodes 
        // divide the page rank to each of the outlinks 
        // sum based on the outlink(page) as the key,
        // use sum , dangsum and the formula to get the final answer
        var pageRank = pages.filter(x => {
                                val (page, (outlinks, pr)) = x
                                !(outlinks(0) == "")})
                            .flatMap(x => {
                                val (page, (outlinks, pr)) = x 
                                outlinks.map(outlink => (outlink, pr / outlinks.size)) })
                            .reduceByKey((accum, one_pr) => accum + one_pr)
                            .map(x => {
                                  val (k,v) = x
                                  (k, (((1-lambda) / totalDocs) + (lambda*v) + (lambda*dangSum/totalDocs)))}).persist()

        pages.unpersist()

        // join to get the outlinks back for next iteration
        pages = processedPages.join(pageRank).persist()
    }
    
    // sort descending based on the page rank 
    // and then take top 100 and convert the output to a printable format
    val sortPR = pages.sortBy(_._2._2, false)
                             .take(100)
                             .map( x => {
                                val (page, (outlinks, pr)) = x
                                page + "\t" + pr})

    val top100 = sc.parallelize(sortPR)

    top100.saveAsTextFile(args(1))

    sc.stop()

    println("SIZE = " + size)
  }
}