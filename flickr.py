#!/usr/bin/python
#-*- coding: utf-8 -*-

import flickrapi
import sqlite3
import os 
import time 
import datetime
import logging
import urllib2
from ConfigParser import SafeConfigParser
import sys
import shutil
from tendo import singleton


os.setgid(1000)
os.setuid(1000)

me = singleton.SingleInstance()

config_path = ""
if len(sys.argv)>1:
	config_path = sys.argv[1]
else:
	print("Blad, nie podano sciezki do konfiga !!!")
	sys.exit(1)


parser = SafeConfigParser()
parser.read(config_path)

logger = logging.getLogger('flickr')
hdlr = logging.FileHandler(parser.get('main', 'log_path'))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s %(lineno)d    -  %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


auth_token= "7215xxxxxxxxxxx1042xxxxf00fa31cc"
api_key="fd944xxxxxx15abf97xxxxxxxxxxx3"
secret="2axxxx7xxxx"

path=parser.get('main', 'path')
collectionid_all=parser.get('main', 'collectionid_all')

flickr = flickrapi.FlickrAPI(api_key, secret, token=auth_token, format='xmlnode')

conn = sqlite3.connect(parser.get('main', 'db_path'))
conn.row_factory = sqlite3.Row
connc = conn.cursor()


allsets=""

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)



def gettime():
	dd=time.localtime()
	return "%s-%s-%s %s:%s:%s" % (dd[0],dd[1],dd[2],dd[3],dd[4],dd[5])


def func(progress, done):
    if done:
        logger.info("Upload pliku zakonczony")
    else:
	pass
        #print ("At %s%%" % progress)



def is_set_exists_database(c, sets):
	c.execute("select * from sets where name='%s' and deleted=0" % sets)
	r = c.fetchone()
	
	if r==None:
		logger.info("nie ma nic")
		return 0


	if r['id']:
		logger.info("zwracam id")
		return r['id']
	else:
		logger.info("nie wiem czemu tak, ale nie ma id")
		return 0


def is_set_exists_local(p,sets):
	if os.path.exists(p+sets) and os.path.isdir(p+sets):	
		return 1
	return 0

def is_photo_exists_local(p,photo):
	if os.path.exists(p+"/"+photo):	
		return 1
	return 0


def add_sets_local(c, setsname, f, photo,data,data_mod):
	s = f.photosets_create(title=setsname, primary_photo_id=photo)
	if s['stat']=="ok":
		setsid=s.photoset[0]['id']
		c.execute("insert into sets values('%s',%s,1,'%s','%s',0)" % (setsname.decode("utf-8"),setsid,data,data_mod))
		c.commit()
		return setsid
	else:
		return 0
	
#	c.execute("insert into sets i"


def add_photo_local(c, f, photoname, data, date_mod,setsid=""):

	ret=flickr.upload(filename=path+directory+"/"+photoname, callback=func, is_public=0, is_family=1)
        if ret['stat']!='ok':
        	logger.error("Przesylanie zdjecia nie powiodlo sie ... wychodze %s ", photoname)
                return 0

	photoid=ret.photoid[0].text

	c.execute("insert into upload values('%s','%s','%s',%s,'%s',0,1)" % (data,date_mod,photoname.decode("utf-8"),photoid,setsid))
	c.commit()
	if setsid!="":
		ret=f.photosets_addPhoto(photoset_id=setsid,photo_id=photoid)
		if ret['stat']!='ok':
			logger.error("Nie powiodlo sie dodanie zdjecia do sets %s %s"%(sets.decode("utf-8"),photoname)) 
			return 0

	return photoid
	
def replace_photo_local(c, f, photoname, photo_id, data, date_mod):
	ret=flickr.replace(filename=path+directory+"/"+photoname, photo_id=photo_id)
        if ret['stat']!='ok':
        	logger.error("Przesylanie zdjecia nie powiodlo sie ... wychodze")
                return 0

	c.execute("update upload set upload_time='%s', modify_time='%s' where fileid=%s"%(data,date_mod,photo_id))
	c.commit()

	return 1
	

def update_photo_sets(c,photoid,setsid):
	c.execute("update upload set sets='%s' where fileid=%s"%(setsid,photoid))
	c.commit()
		

def update_photo_sets_date(c,data,setsids):
	c.execute("update sets set datemodify='%s' where id=%s"%(data, setsids))
	c.commit()
		



def is_photo_new(c,photoname,setsid):
	c.execute("select * from upload where filename='%s' and sets=%s and deleted=0" % (photoname.decode("utf-8"),setsid))
        r = c.fetchone()

        if r==None:
            	logger.info("zdjecie jest nowe")
                return 1

	return 0
	

def is_sets_modify(c,data,setsids):
	c.execute("select * from sets where datemodify<'%s' and id=%s"%(data,setsids))
	r=c.fetchone()

	if r==None:
		logger.info("sets %s nie potrzebuje modyfikacji" % setsids)
		return 0

	return 1
	

def get_sets_date_remote(f,setsid):
	global allsets
	if allsets=="":
		allsets=f.photosets_getList(photoset_id=setsid)
	dd="0000-00-00 00:00:00"
	for i in allsets.photosets[0].photoset:
		if int(i['id'])==int(setsid):
			dd = int(i['date_update'])
			break
	return datetime.datetime.fromtimestamp(dd)
	

def is_photo_modify(c,conn,photoname,setsid,date_mod):
	c.execute("select * from upload where filename='%s' and sets=%s and deleted=0 and modify_time<'%s'" % (photoname.decode("utf-8"),setsid,date_mod))
        r = c.fetchone()

	
        if r==None:
                logger.info("zdjecie nie potrzebuje modyfikacji")
                return 0
	return r['fileid']
	



def do_upload():
	if is_photo_new(connc,files,setsid):
		logger.info("Nie ma zdjecie %s, dodaje ... "%files)
		try:
			photoid = add_photo_local(conn, flickr, files,data,date_mod_file,setsid)
		except flickrapi.FlickrError as err:
			logger.error("Blad uploadu:  %s" %(err))
			return 0

        	if photoid>0:
        		logger.info("Upload ok %s" % photoid)
			return 0
		else:
        		logger.error("Blad uploadu pliku %s"%files)
			return 1
	
	photoid=is_photo_modify(connc,conn,files,setsid,date_mod_file)
	if photoid:
		logger.info("Modyfikacja zdjecia %s" %files)
		pp = replace_photo_local(conn, flickr, files,photoid, data,date_mod_file)
		
		if pp>0:
                        logger.info("Upload ok %s %s" % (photoid,files))
			return 0
                else:
                        logger.error("Blad uploadu pliku %s" %files)
			return 1
	
	logger.info("Zdjecie jest ale nie ma potrzeby modyfikacji %s" %files)
	return 0
	

def get_list_dirs(p):
	listd=[]
	
	for d in os.listdir(p):
		if d[0]=="." or not os.path.isdir(p+d):
			continue
		listd.append(d)
	return listd

	

def get_list_files(p,d):
	listf=[]
	for f in os.listdir(p+d):
		if f[0]=="." or os.path.isdir(p+d+"/"+f):
			continue
		exts=('.png','.jpg','tiff')
		if not any(f.lower().endswith(ext) for ext in exts):
			continue
		listf.append(f)
	return listf



def get_list_sets_local(c):
	c.execute("select * from sets where deleted=0")
	ll = []
	while True:
		r = c.fetchone()
		if r == None:
			break
		ll.append((r['id'],r['name']))

	return ll		

def get_list_photos_local(c,setsid):
	c.execute("select * from upload where deleted=0 and sets=%s"%setsid)
	ll = []
	while True:
		r = c.fetchone()
		if r == None:
			break
		ll.append((r['fileid'],r['filename']))

	return ll		

def get_list_photos_remote(f,setsid):
	ll = []
	
	
	ret = f.photosets_getPhotos(photoset_id=setsid,extras="original_format, last_update, url_o")
	pages=int(ret.photoset[0]['pages'])
	page=int(ret.photoset[0]['page'])

	while True: 	
		for i in ret.photoset[0].photo:
			ll.append((i['id'],i['title'],i['originalformat'],i['lastupdate'],i['url_o']))

		if page == pages:
			break
		page+=1
		ret = f.photosets_getPhotos(photoset_id=setsid,extras="original_format, last_update, url_o",page=page)
		
	logger.info("++++++++++++++++++ Liczba zdjec to: %s %s %s %s  "%(len(ll),ret.photoset[0]['page'],ret.photoset[0]['pages'],ret.photoset[0]['total']))
	return ll
		
def delete_sets_remote(c,cc,f,setsid):
	#list plikow do usuniecia
	cc.execute("select * from upload where sets=%s"%setsid)


	while True:
		r=cc.fetchone()
		if r == None:
			break
		try:
			flickr.photos_delete(photo_id=r['fileid'])
		except flickrapi.FlickrError as err:		
			logger.error("Wystapil blad podczas usuwania zdjecia z flickra %s %s"%(r['filename'],err))


		c.execute("delete from upload where fileid=%s"%r['fileid'])
		c.commit()

		logger.info("Zdjecie %s zostalo usuniete"%r['filename'])
	try:
		f.photosets_delete(photoset_id=setsid)
	except flickrapi.FlickrError as err:
		logger.error("Blad usuwania sets %s z flickra"%setsid)
	
	c.execute("delete from sets where id=%s"%setsid)
	c.commit()
	
	logger.info("Sets %s zostal usuniety"%setsid)


def delete_photo_remote(c,f,photoid):
	try:
		f.photos_delete(photo_id=photoid)
	except flickrapi.FlickrError as err:
        	logger.error("Wystapil blad podczas usuwania zdjecia z flickra %s %s"%(photoid,err))


	c.execute("delete from upload where fileid=%s"%photoid)
        c.commit()

	logger.info("Zdjecie %s zostalo usuniete"%photoid)


def delete_photo_local(c,photo,sets):
	os.remove(path+sets+'/'+photo[1])

	c.execute("delete from upload where fileid=%s"%photo[0])
        c.commit()

	logger.info("Zdjecie %s zostalo usuniete"%photo[1])



def get_sets_list_remote(f,collectionid):
	ll = []
	try:
		r=f.collections_getTree(collection_id=collectionid)	
	except flickrapi.FlickrError as err:
		logger.error("Wystapil blad podczas pobierania listy setow %s"%err)
		return 0
	for i in r.collections[0].collection[0].set:
		ll.append((i['id'],i['title']))

	return ll
						
def diff_sets(s1,s2):
	diff = []
	for i in s1:
		iss=False
		for j in s2:
			if int(i[0]) == int(j[0]):
				iss=True
				break;
		if not iss:
			diff.append(i)
	return diff

def add_sets_remote(c,setsname,setsid,data):
	c.execute("insert into sets values('%s',%s,1,'%s','%s',0)" % (setsname,setsid,data,data))
        c.commit()
	

def get_photos_from_flickr(f,c,cc,photo,sets,data,path):
	u = urllib2.urlopen(photo[4])
	localFile = open(path+sets[1]+'/'+photo[1]+'.'+photo[2], 'w')
	localFile.write(u.read())
	localFile.close()
	
        c.execute("insert into upload values('%s','%s','%s',%s,'%s',0,1)" % (data,data,photo[1]+'.'+photo[2],photo[0],sets[0]))
        c.commit()
	
	
	 
#sets = flickr.photosets_getList()
#set0 = sets.photosets[0].photoset[0]
#print set0.title[0].text
#c = flickr.collections_getTree()
#cc= c.collections[0].collection[1]
#print cc['title']

########################################
###
### wrzuca nowe katalogi jako sety, uploaduje zdjecia i przypisuje do dpowiedniego seta i kolekcji
###
###
#######################################
setsmodlist=[]
dirs = get_list_dirs(path)
for directory in dirs:
	sets=""
	setsid=0
	photoid=0
	
	logger.info("Nowy katalog %s" %directory)
	sets=directory
	#### tutaj jazda ### 

	ffs = get_list_files(path,directory)
	ii=0

	sets_data_mod = modification_date(path+directory)
	for files in ffs:
		ii += 1

		logger.info("Nowy plik  %s  %s/%s" % (files,ii,len(ffs)))
		

		data=gettime()

			
		date_mod_file = modification_date(path+directory+"/"+files)

		if setsid > 0:
			logger.info(u"Sets jest wiekszy %s %s " % (sets.decode("utf-8"),setsid))
			if do_upload():
				logger.error("Blad uploadu %s"%files)
				break
			if ii == len(ffs):
				update_photo_sets_date(conn, sets_data_mod, setsid)

			continue		
               
			
		setsid=is_set_exists_database(connc, sets)
		if setsid==0:
			logger.info(u"Nie ma takiego seta %s, dodaje pierwsze zdjecie " % sets.decode("utf-8"))

			photoid = add_photo_local(conn, flickr, files,data,date_mod_file)
			if photoid>0:
                                logger.info("Upload ok %s %s" % (photoid,files))
                        else:
                                logger.error("Blad uploadu pliku %s"%files)
                                break
			logger.info("Tworze sets %s " % sets)
	
			setsid=add_sets_local(conn, sets, flickr, photoid,data, "")
			if setsid==0:
				logger.error("Nie udalo sie utworzy sets %s"%(sets.decode("utf-8")))
				break

			else:
				logger.info("Utworzono sets: %s %s" %(setsid,sets.decode("utf-8")))

			flickr.collections_addSet(collection_id=collectionid_all,photoset_id=setsid)
			update_photo_sets(conn,photoid,setsid)
			if ii == len(ffs):
				update_photo_sets_date(conn, sets_data_mod, setsid)
		else:
			logger.info(u"Sets %s istnieje, uplaod zdjecia z setem" % sets.decode("utf-8"))
	
			if not is_sets_modify(connc,sets_data_mod,setsid):
				break
		
			setsmodlist.append(setsid)		
	
			if do_upload():
				logger.error("Blad uploadu %s"%files)	
				continue

			if ii == len(ffs):
				update_photo_sets_date(conn, sets_data_mod, setsid)



############################
#######
#	sprawdzamy czy jakies sety ktore mamy w bazie nie zostaly usuniete z dysku .. czyli sprawdzamy baza, jesli sets jest w bazie, a nie ma go na dysku usuwamy z flickra
###
##


setslist = get_list_sets_local(connc)

for sets in setslist:
	if is_set_exists_local(path,sets[1]):
		logger.info("Sets %s istnieje na dysku, nic nie robie"%sets[1])
	else:
		logger.info("UWAGA!!!!! Sets %s nie istnieje na dysku, trzeba go usunac z flickra"%sets[1])
		delete_sets_remote(conn,connc,flickr,sets[0])




#####################################################################
#
#    Sprawdzamy sety i jesli ktorus zostal zmodyfikowany, sprawdzamy czy jakies zdjecie nie zostalo usuniete z dysku
#
######################################################################


for sets in setslist:
		
	if not sets[0] in setsmodlist:
		continue

	
	ffs = get_list_photos_local(connc, sets[0])

	for files in ffs:
		if is_photo_exists_local(path+sets[1],files[1]):
			logger.info("Photo %s istnieje w %s" %(files[1],sets[1]))
		else:
			logger.info("UWAGA!!!!   Photo %s nie istnieje w %s" %(files[1],sets[1]))
			delete_photo_remote(conn,flickr,files[0])



######## dodajemy nowe sety
setslist = []

setslistremote = get_sets_list_remote(flickr, collectionid_all)	
setslistlocal = get_list_sets_local(connc)

setslist = diff_sets(setslistremote,setslistlocal)

data = gettime()
for i in setslist:
	logger.info("Znaleziono nowy set na flickru, dodaje ... %s %s "%(i[1],i[0]))
	if not os.path.exists(path+i[1]):
		os.makedirs(path+i[1])
		add_sets_remote(conn,i[1],i[0],data)
	photoslocal = get_list_photos_local(connc,i[0])
	photosremote = get_list_photos_remote(flickr, i[0])
	photoslist = diff_sets(photosremote, photoslocal)
	for j in photoslist:
		logger.info("Pobieram nowe zdjecie %s do set %s"%(j[1],i[1]))
		get_photos_from_flickr(flickr,conn,connc,j,i,data,path)


## usuwamy sety


setslistremote = get_sets_list_remote(flickr, collectionid_all)	
setslistlocal = get_list_sets_local(connc)

setslist = diff_sets(setslistlocal,setslistremote)

for i in setslist:
	logger.info("Usunieto  set z flickra, usuwam ... %s %s "%(i[1],i[0]))
	
	shutil.rmtree(path+'/'+i[1], ignore_errors=False, onerror=None)		 
	conn.execute("delete from sets where id=%s"%i[0])
        conn.commit()
	conn.execute("delete from upload where sets=%s"%i[0])
        conn.commit()

	



#sprawdzamy czy jakis set sie zmienil i jesli tak dodajemy zdjecia
#allsets = 
for i in setslistremote:
		date_mod = get_sets_date_remote(flickr,i[0])
		if is_sets_modify(connc,date_mod,i[0]):
			        photoslocal = get_list_photos_local(connc,i[0])
        			photosremote = get_list_photos_remote(flickr, i[0])
        			photoslist = diff_sets(photosremote, photoslocal)
        			for j in photoslist:
					logger.info("Pobieram nowe zdjecie %s do set %s"%(j[1],i[1]))
					get_photos_from_flickr(flickr,conn,connc,j,i,data,path)
				
				photoslocal = get_list_photos_local(connc,i[0])
				photosremote = get_list_photos_remote(flickr, i[0])
				photoslist = diff_sets(photoslocal, photosremote)
				for j in photoslist:
					logger.info("Usuwam  zdjecie %s do set %s"%(j[1],i[1])) 	
					delete_photo_local(conn,j,i[1])
				update_photo_sets_date(conn,date_mod,i[0])
