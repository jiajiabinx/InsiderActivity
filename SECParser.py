from bs4 import Beautiful Soup 
import pandas as pd 
import requests 
import datetime 
import re 
import numpy as np 
import gzip 
from tqdm import tqdm


class SECParser():
	"""
	A tool designed to extract data from SEC. 
	The Parser first collects index file from the SEC website, save it to our own storage, and then got to the link in the index file, download the form and parse it into csv data files.
	Example: 
	s = SECParser()
	date = "20200225" 
	s.collect_daily_index(date) 
	InsiderActivity = s.GetInsiderActivity 
	"""

	def __init__(self):
		self.db = pd.DataFrame()
		self.rawbin = pd.DataFrame() 
		self.date = datetime.datetime.today() 
		self.idx = pd.DataFrame() 
		self.foreignissuer = pd.DataFrame() 
		self.proxy={"http": "totbluecoatp3.nomura.com:8080", "https": "totbluecoatp3. nomura.com:8080"}
	#method to set the parser's index file to existing index data - use it if you already have the index file downloaded 
	
	def set_index_bin(self,df):
		self.rawbin = df
	
	#methods to download and save index files
	def collect_daily_index(self, date):
		url = self._getDailyIndex(date)
		if url.split(".")[-1]=="gz":
			r = requests.get(url, proxies Eself.proxy)
			d=pd.DataFrame(gzip.decompress(r.content).decode("utf-8").split("\n"))[0].str.split("\\s{2,}", expand = True)
		else:
			r = requests.get(url, proxies =self.proxy)
			d=pd.DataFrame(r.text.split("\n"))[0].str.split("\\s{2,}", expand = True)
		d["Index file Used"] = url 
		d["Quarter"] = url.split("/")[-2] 
		d["Year"] = url.split("/")[-3] 
		fi = self._processForeignIssuer(d.loc[d[1]=="/FI"])
		d.drop(d.loc[[1]=="/FI"].index, axis=0, inplace=True) 
		d.dropna(thresh=8, inplace=True)
		d = pd.concat([d, fi], axis=0) 
		d.rename(columns = {0: "Filer Name", 1:"Form Type", 2:"Filer CIK",3:"Date Filed", 4:"File Name"}, inplace=True) 
		self.rawbin = d
		self._cleanup()
		self.rawbin.to_csv(date + ".csv", index=False)
		return d
	
	def collect_historical_indice(self, start_year=2015):
		tqdm.pandas()
		pd.DataFrame(self._HistoricalInds()).to_csv(r"G: EDG Trading ML\SECParser\HistoricalInds.csv", index=False)
		for url in self._HistoricalInds (start_year - start_year)[1:]:
			if url.split(".")[-1]=="gz":
				r = requests.get(url, proxies Eself.proxy)
				d=pd.DataFrame(gzip.decompress(r.content).decode("utf-8").split("\n"))[0].str.split("\\s{2,}", expand = True)
			else:
				r = requests.get(url, proxies -self.proxy)
				d=pd. DataFrame(r.text.split("\n"))[0].str.split("\\s{2,}", expand = True)
				d["Index file Used"] = url
				d["Quarter"] = url.split("/")[-2] d["Year"]=url.split("/")[-3] 
				#check for foreign Issuer 
				fi Eself._processForeign Issuer(d.loc[d[1]=="/FI"]) 
				d.drop(d.loc[d[1]=="/FI"].index, axis=0, inplace=True) 
				d.dropna (thresh=8, inplace=True) d = pd.concat([d, fi], axis=0) 
				d.rename (columns = {0: "Filer Name", 1:"Form Type", 2:"Filer CIK",3: "Date Filed",4:"File Name"},inplace=True)
				self.rawbin = pd.concat((self.rawbin, d), axis=0) 
				temp = self.rawbin
				self.rawbin = self._cleanup(rawbin=temp) #clean up for irregularities
			print("index file collection complete")

	'''download and parse the downloaded forms'''
	
	def GetInsiderActivity(self):
		ia = self._Parse_Form4()
		# ia=pd.read_csv(r"G:\EDG Trading ML\SECParser\caches\InsiderActivity_temp.csv") 
		ia.to_csv(r"G:\EDG Trading ML\SECParser\caches\InsiderActivity_temp.csv", index=False) 
		ia = ia.astype (str) 
		output = pd. DataFrame() 
		for _, row in ia.iterrows():
			transactions = self._ExpandForm4(row)
			output = output.append(transactions, sort = False)
		output["Transaction Date"] = pd.to_datetime(output["Transaction Date"].astype(str)) 
		return output

	"""Helper methods"""
	def _cleanup(self):
	"""method to clean up the index file"""
		tqdm.pandas() 
		self.rawbin.replace("", np.nan, regex=True, inplace=True) 
		self.rawbin.dropna (axis=1,how="all", inplace=True) 
		self.rawbin.dropna(axis=0,how="all", inplace-True) 
		if (5 in self.rawbin.columns.tolist():
			if 6 in self.rawbin.columns.tolist():
				clip1 = self.rawbin.loc[self.rawbin[5].notnull() & self.rawbin[6].isnull()].copy()
				clip2=self.rawbin.loc[self.rawbin[6].notnull()].copy()
				clip2.rename(columns={"Date Filed" : "Form Type temp", "Filer CIK":"Filer Name part3", "Form Type" : "Filer Name part2", 5:"Date Filed", 6:"File Name temp", "File Name": "Filer CIK temp"},inplace=True)
				clip2.replace(np.nan,"", regex=True, inplace=True)
				clip2[ "Filer Name temp"]=clip2.progress_apply(lambda x: x["Filer Name"]+ " " + x["Filer Name part2"]+" "+x["Filer Name part3"], axis=1)
				clip2.drop([ "Filer Name part3","Filer Name part2", "Filer Name"], axis-1, inplace = True)
				clip2.rename(columns={"Form Type temp":"Form Type", "Filer Name temp":"Filer Name", "Filer CIK temp":"Filer CIK", "File Name temp":"File Name"},inplace= True)
				clip2.dropna (how="all", axis-1, inplace=True)
  				self.rawbin.drop([6], axis-1, inplace=True)

