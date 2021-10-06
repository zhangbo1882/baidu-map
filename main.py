#!/usr/local/bin/python3 # -*- coding: utf-8 -*-
import urllib
import hashlib
import requests
import json
import time
import datetime
import xlwings as xw

myak="w5i9dYBqFBNR3ukdvsfpuEe40Cr53OSl"
sk="TGXfG0jcHTegDV0aSpQXRMtApCANqtOe"
query= '/place/v2/search?query=%s&region=%s&output=json&ak='+myak
transport_path='/directionlite/v1/transit?origin=%s,%s&destination=%s,%s&timestamp=%s&ak='+myak
walk_path='/directionlite/v1/walking?origin=%s,%s&destination=%s,%s&timestamp=%s&ak='+myak
drive_path='/directionlite/v1/driving?origin=%s,%s&destination=%s,%s&timestamp=%s&ak='+myak
ride_path='/directionlite/v1/riding?origin=%s,%s&destination=%s,%s&timestamp=%s&ak='+myak
default_region="上海"
host="https://api.map.baidu.com"
excel_file_path="data.xlsx"
persons=list()
offices=list()
path_type={"walk": walk_path, "transport": transport_path}

class Poi:
    lat=0.0
    lng=0.0
    def __init__(self, lat=0.0, lng=0.0):
        self.lat=lat
        self.lng=lng
    def __str__(self):
        return "{"+str(self.lat)+","+str(self.lng)+"}"

class Office:
    name=""
    address=""
    url=""
    region=default_region
    poi=Poi()
    def __init__(self, name, address):
        self.name=name
        self.address=address
        queryStr = query % (self.address, self.region) 
        encodeStr=urllib.parse.quote (queryStr, safe="/:=&?#+!$,;'@()*[]")
        rawStr=encodeStr+sk
        self.url = host+queryStr+'&sn='+hashlib.md5(urllib.parse.quote_plus(rawStr).encode()).hexdigest()

    def dump(self):
        print('name: {}'.format(self.name))
        print('address: {}'.format(self.address))
        print('url: {}'.format(self.url))

    def getPoi(self):
        resp = requests.get(self.url)
        result=resp.text
        data = json.loads(result)
        #loc = data["results"][0]["location"]
        #self.poi=Poi(loc["lat"], loc["lng"])
        print("Get poi for %s" %  data)
        print(self.poi)

class Person:
    name=""
    address=""
    url=""
    region=default_region
    poi=Poi()
    distancePathMap = dict()
    durationPathMap = dict()
    officeDistanceMap = dict()
    officeDurationMap = dict()

    def __init__(self, name, address):
        self.name=name
        self.address=address
        queryStr = query % (self.address, self.region) 
        encodeStr=urllib.parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
        rawStr=encodeStr+sk
        self.url = host+queryStr+'&sn='+hashlib.md5(urllib.parse.quote_plus(rawStr).encode()).hexdigest()

    def dump(self):
        print('name: {}'.format(self.name))
        print('address: {}'.format(self.address))
        print('url: {}'.format(self.url))

    def getPoi(self):
        resp = requests.get(self.url)
        result=resp.text
        data = json.loads(result)
        loc = data["results"][0]["location"]
        self.poi=Poi(loc["lat"], loc["lng"])
        #print("Get poi for %s" %  self.url)
        #print(self.poi)

    def _run(self, office, pathType):
        poi = office.poi
        pathStr=path_type[pathType] % (self.poi.lat, self.poi.lng, poi.lat, poi.lng, int(time.time()))
        encodeStr=urllib.parse.quote (pathStr, safe="/:=&?#+!$,;'@()*[]")
        rawStr=encodeStr+sk
        url = host+pathStr+'&sn='+hashlib.md5(urllib.parse.quote_plus(rawStr).encode()).hexdigest()
        #print(url)
        resp = requests.get(url)
        data = json.loads(resp.text)
        distance = data["result"]["routes"][0]["distance"]
        duration = int(data["result"]["routes"][0]["duration"]/60)
        return distance, duration 
      # self.distanceSort = sorted(self.distanceMap.items(), key=lambda x:x[1])
      # self.durationSort = sorted(self.durationMap.items(), key=lambda x:x[1])

    def run(self, office):
        d1=dict()
        d2=dict()
        for pathType in path_type.keys():
            v1, v2=self._run(office, pathType)
            d1[pathType] =v1
            d2[pathType] =v2
        self.officeDistanceMap[office] = sorted(d1.items(), key=lambda x:x[1])
        self.officeDurationMap[office] = sorted(d2.items(), key=lambda x:x[1])

    def show(self):
        print("Name: {}".format(self.name))
        for key in self.officeDurationMap.keys():
            print("\t office: {}".format(key.name))
            for value in self.officeDurationMap[key]:
                print("\t\t {}: {}".format(value[0], value[1]))


def loadData(file):
    app = xw.App(visible=True, add_book=False)
    wb = app.books.open(file)

    personSheet= wb.sheets['人员']
    p=personSheet.range('B2').expand('table')
    i=0
    while i < p.row:
        r = p.rows[i]
        persons.append(Person(r[0].value,r[1].value))
        i=i+1

    officeSheet= wb.sheets['营业厅']
    o=officeSheet.range('A2').expand('table')
    i=0
    while i < o.row:
        r = o.rows[i]
        offices.append(Office(r[0].value,r[1].value))
        i=i+1

def setAllPoi():
    for p in persons:
        p.getPoi()
    for o in offices:
        o.getPoi()

o = Office("长宁支行", "平安银行长宁支行")
o.getPoi()

#loadData(excel_file_path)
#setAllPoi()
#for p in persons:
#    for o in offices:
#        p.run(o)
#    p.show()


