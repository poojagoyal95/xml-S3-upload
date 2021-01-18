import requests
import traceback
import xml.etree.ElementTree as ET 
import zipfile
import os
import csv
import logging
from datetime import datetime
import boto3
from dotenv import load_dotenv
load_dotenv()

today = datetime.today()
logging.basicConfig(filename='app-'+str(today.day)+'-'+str(today.month)+'.'+'log', filemode='a', format='%(asctime)s - %(message)s',level=logging.INFO)

class XmlConnection:
    def connection(fileName):
        #Establish connection with Xml reader.
        #Parameters:
        #    file (str):Read XML file reader.
        #Returns:
        #    root(object):Root node of Xml 
        logging.info("reading xml connection starts")
        root =''
        try:
            tree = ET.parse(fileName)
            root = tree.getroot()
            logging.info("%s get root from XMl startes",root)
        except Exception as e: 
            print(e)
            print(traceback.format_exc())
            logging.error("%s error in uploading",traceback.format_exc())   
        return root

    def checkFileExists(file):
        #Check File exists in folder.
        #Parameters:
        #    file (str):check file exists in folder or not.
        #Returns:
        #    status(boolean):Return Status of file exists or not.  
        print("check file exists in folder")
        logging.info("check file exists in folder")
        status = os.path.exists(file)
        logging.info("%s file status" , status)
        return status     

class AwsAndUpload:
    def __init__(self):
        #Check File exists in folder and establish connection with aws.
        #Parameters:
        #    self: instance of init function.
       
        status = XmlConnection.checkFileExists(ReadAndSaveXmlData.csvName)
        if status:
            logging.info("Aws functionality started")
            self.s3 = boto3.resource(
                    's3',
                    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("ACCESS_SECRET_KEY")
                )
    
    def upload(self):
        #Upload file on AWS.
        #Parameters:
        #    self: instance of init function.
        logging.info("Upload process started")
        try:
            data = open(ReadAndSaveXmlData.csvName,'rb')    
            status = self.s3.Bucket(os.getenv("BUCKET_NAME")).put_object(Key= ReadAndSaveXmlData.csvName, Body=data)
            logging.info("%s upload process compltetd",status)
        except Exception as e: 
            print(e)
            print(traceback.format_exc())
            logging.error("%s error in uploading",traceback.format_exc())
        print("done")


class ReadAndSaveXmlData:
    fileName = 'response.xml'
    zipFileName = 'data.zip'
    fields = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
    csvName = 'data.csv'

    def __init__(self,url):
        self.url = url

    def readUrl(self):
        #Read the file and save Xml data in file.
        logging.info('Read Url function started')
        r = requests.get(self.url) 
        
        logging.info('Request for Url')
        with open(self.fileName, "w") as f:
            f.write(r.text)
        logging.info("write data in response.xml")

    def getLink(self):   
        #Read the link from Xml file if file type is DLTINS.
        #Parameters:
        #    fileName (str):Name of file where we read XML data.
        #Returns:
        #    link(str1):Return link if condition matches else return 0.    
        root = XmlConnection.connection(self.fileName)
        if root[1][0][7].text == 'DLTINS':
            print("value match and get link")
            link = root[1][0][1].text
            logging.info("%s link from XML",link)
            return link
        else:
            print("Value not found") 
            logging.info("No link Found")
            return 0

    def downloadExtractZip(self,link):
        #Download and extract zip from link in folder.
        #Parameters:
        #    link (str):The string where we download zip file.
        r = requests.get(link, stream=True)
        status = False
        try:
            with open(self.zipFileName, 'wb') as fd:
                logging.info("zip process of downloading started")
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk) 
                status = True    
            logging.info("zip process of downloading ended")
            if status == True:
                with zipfile.ZipFile(self.zipFileName, 'r') as zip_ref:
                    logging.info("unzip process of downloading started")
                    zip_ref.extractall('data')
                logging.info("zip process of downloading ended")     
        except Exception as e: 
            print(e)
            print(traceback.format_exc())
            logging.error("%s extract and inextract",traceback.format_exc())
        
        return XmlConnection.checkFileExists('data/DLTINS_20200108_01of03.xml')  
     
    def ReadAndCreateCsv(self):
        #Retruns data from XML file using namespaces.
        #Returns:
        #    resItems(dictionary):Returns data of XML in dictionary. 
        try: 
            root = XmlConnection.connection('data/DLTINS_20200108_01of03.xml')
            namespaces = {
                'i': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01',
                'j': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02'
            }

            listdata = root.findall("i:Pyld/j:Document/j:FinInstrmRptgRefDataDltaRpt/j:FinInstrm", namespaces)
            resItems = []
            res = {}
            logging.info("xml reading process started")
            for index, child in enumerate(listdata):
                columns = child.find('j:ModfdRcrd', namespaces)
                if columns:
                    res['FinInstrmGnlAttrbts.Id'] = columns.find('j:FinInstrmGnlAttrbts/j:Id',namespaces).text
                    res['FinInstrmGnlAttrbts.FullNm'] = columns.find('j:FinInstrmGnlAttrbts/j:FullNm',namespaces).text
                    res['FinInstrmGnlAttrbts.ClssfctnTp'] = columns.find('j:FinInstrmGnlAttrbts/j:ClssfctnTp',namespaces).text
                    res['FinInstrmGnlAttrbts.CmmdtyDerivInd'] = columns.find('j:FinInstrmGnlAttrbts/j:CmmdtyDerivInd',namespaces).text
                    res['FinInstrmGnlAttrbts.NtnlCcy'] = columns.find('j:FinInstrmGnlAttrbts/j:NtnlCcy',namespaces).text
                    res['Issr'] = columns.find('j:Issr',namespaces).text
                    resItems.append(res)
            logging.info("reading xml data ended")
            with open(self.csvName, 'w',newline='') as csvfile:  
                logging.info("data in csv process started")
                writer = csv.DictWriter(csvfile, fieldnames = self.fields) 
                writer.writeheader() 
                writer.writerows(resItems)  
        except Exception as e: 
            print(e)
            print(traceback.format_exc())
            logging.error("%s error in uploading",traceback.format_exc())        
        
        logging.info("data write into csv")      

obj = ReadAndSaveXmlData(os.getenv("Url"))
obj.readUrl()
link = obj.getLink()
if link:
    logging.info("link found and download and extarction prcess started")
    status = obj.downloadExtractZip(link)
    logging.info("%s status",status)
    if status:
        obj.ReadAndCreateCsv()  
        awsObject = AwsAndUpload() 
        awsObject.upload()
    else:
        logging.info("xml file not found")
else:
    logging.info("link not found")  
                   
