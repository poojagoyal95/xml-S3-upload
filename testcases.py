import unittest
import os
import requests
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import zipfile
import boto3
load_dotenv()
import csv

class CreateUploadS3(unittest.TestCase):

    def test_agetUrlSaveData(self):
        r = requests.get(os.getenv('Url'))
        with open('test.xml', "w") as f:
            f.write(r.text)

        assert r.text is not None

    def test_breadXmlData(self):
        tree = ET.parse('test.xml')
        root = tree.getroot()
        self.assertEqual(root[1][0][7].text, 'DLTINS')

    def test_downloadZip(self):
        tree = ET.parse('test.xml')
        root = tree.getroot()
        link = root[1][0][1].text
        r = requests.get(link, stream=True)
        status = False
        with open('test.zip', 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk) 
            status = True 
        if status == True:
            with zipfile.ZipFile('test.zip','r') as zip_ref:
                zip_ref.extractall('test') 
        self.assertEqual(os.path.exists('test'),True)        
               
    def test_write_csv(self):
        tree = ET.parse('test/DLTINS_20200108_01of03.xml')
        root = tree.getroot()
        namespaces = {
            'i': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01',
            'j': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02'
        }

        listdata = root.findall("i:Pyld/j:Document/j:FinInstrmRptgRefDataDltaRpt/j:FinInstrm", namespaces)
        resItems = []
        res = {}
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
        fields = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']

        with open('test.csv', 'w',newline='') as csvfile:  
            writer = csv.DictWriter(csvfile, fieldnames = fields) 
            writer.writeheader() 
            writer.writerows(resItems)  
        assert writer is not None

    def test_uawsConnection(self):
        s3 = boto3.resource('s3',aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("ACCESS_SECRET_KEY"))
        assert s3 is not None

    def test_vuploadS3(self):
        s3 = boto3.resource('s3',aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("ACCESS_SECRET_KEY"))
        data = open('test.csv','rb')
        status = s3.Bucket(os.getenv("BUCKET_NAME")).put_object(Key= 'test.csv', Body=data)
        assert status is not None    


unittest.main()
