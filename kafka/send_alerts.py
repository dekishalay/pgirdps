import requests 
import json
import argparse
from datetime import datetime
import numpy as np
import re
import io, psycopg2, psycopg2.extras
import socket
import os, sys
from io import BytesIO
import fastavro
import confluent_kafka
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import warnings
from astropy.io import ascii, fits
from astropy.time import Time
import time
#Should be loaded in at the beginning of the pipeline
from keras import optimizers
import gzip
from keras.models import model_from_json
# Should be loaded in once at the beginning of the pipeline
# taken from https://machinelearningmastery.com/save-load-keras-deep-learning-models/
json_directory= '/data/kde/json_models/'
current_model_json= 'modela4.json'
current_model_h5= 'modela4.h5'
# load json and create model
json_file = open(json_directory+current_model_json, 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(loaded_model_json)
# load weights into new model
loaded_model.load_weights(json_directory+current_model_h5)
print("Loaded model from disk")
# evaluate loaded model on test data
loaded_model.compile(loss='binary_crossentropy', optimizer=optimizers.adam(lr=3e-4), metrics=['accuracy'])

#read credentials
userdata = ascii.read('config/gdb.config', format = 'no_header')
username = userdata['col1'][0]
password = userdata['col1'][1]
gattinibot_login = "dbname=gattini user=%s password=%s"%(username, password)
#print(gattinibot_login)
hostname = socket.gethostname()

if 'gattinidrp' in hostname:
	#how to login into the database
	gattinibot_login += " host=localhost"
else:
	#how to login into the database
	gattinibot_login += " host=gattinidrp"
	
	
def getDBCursor(silent = False):
	#Let's connect to the DB
	if not silent:
		print('Connecting to the gattini database .. ')
	try:
		conn = psycopg2.connect(gattinibot_login)
	except psycopg2.Error as err: 
		print("I cannot access gattini. ERROR: %s\n"%err)	
		return -1, -1
	if not silent:
		print('I am connected to gattini .. ')

	#Initializing a cursor
	cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
	return conn, cur
	
def closeCursor(conn, cur, silent = False):
	try: 
		conn.commit()
		cur.close()
		conn.close()
	except psycopg2.Error as err:
		print('Could not close cursor. ERROR: %s\n'%err)
		return
	if not silent:
		print('Closed connection to gattinibot')

	
	
def combine_schemas(schema_files):
	"""Combine multiple nested schemas into a single schema.
	Taken from Eric's lsst-dm Github page
	"""
	known_schemas = avro.schema.Names()
	print(known_schemas)
	for s in schema_files:
		schema = load_single_avsc(s, known_schemas)
	# using schema.to_json() doesn't fully propagate the nested schemas
	# work around as below
	props = dict(schema.props)
	fields_json = [field.to_json() for field in props['fields']]
	props['fields'] = fields_json
	return props


def load_single_avsc(file_path, names):
	"""Load a single avsc file.
	Taken from Eric's lsst-dm Github page
	"""
	curdir = os.path.dirname(__file__)
	file_path = os.path.join(curdir, file_path)

	with open(file_path) as file_text:
		json_data = json.load(file_text)
		
	schema = avro.schema.SchemaFromJSONData(json_data, names)
	return schema


def send(topicname, records, schema):
	""" Send an avro "packet" to a particular topic at IPAC
	Parameters
	----------
	topic: name of the topic, e.g. ztf_20191221_programid2_zuds
	records: a list of dictionaries
	schema: schema definition
	"""
	# Parse the schema file
	#schema_definition = fastavro.schema.load_schema(schemafile)

	# Write into an in-memory "file"

	out = BytesIO()
	
	fastavro.writer(out, schema, records)
	out.seek(0) # go back to the beginning
	
	# Connect to the IPAC Kafka brokers
	#producer = confluent_kafka.Producer({'bootstrap.servers': 'ztfalerts04.ipac.caltech.edu:9092,ztfalerts05.ipac.caltech.edu:9092,ztfalerts06.ipac.caltech.edu:9092'})

	# Send an avro alert
	#producer.produce(topic=topicname, value=out.read())
	#producer.flush()
	
	print('Sent record')
	

# Function for predicting RB score
def RB_score(stack):
	# stack should be a numpy array of shape (61,61,3)-- a stack of science, reference and difference images
	image_test= stack[np.newaxis, :, :, :]
	probs = loaded_model.predict_proba(image_test)
	return probs[0][0] #returned as float value
	
def normalize(image):
	myimage = image - image.mean()
	myimage = myimage / myimage.std()
	return myimage	

def getscore(canddict):
	try:
		sci_cutout = fits.open(io.BytesIO(gzip.open(io.BytesIO(canddict['sci_image']), 'rb').read()))[0].data
		ref_cutout = fits.open(io.BytesIO(gzip.open(io.BytesIO(canddict['ref_image']), 'rb').read()))[0].data
		diff_cutout = fits.open(io.BytesIO(gzip.open(io.BytesIO(canddict['diff_image']), 'rb').read()))[0].data
		
		#Normalizing data for Ashish model; comment if not using
		sci_cutout = normalize(sci_cutout)
		ref_cutout = normalize(ref_cutout)
		diff_cutout = normalize(diff_cutout)
		
		stack= np.zeros((61,61,3))
		stack[:,:,0]= diff_cutout
		stack[:,:,1]= sci_cutout
		stack[:,:,2]= ref_cutout
	except ValueError as err:
		print('Could not compute rbscore for candid %d. Skipping .. '%(canddict['candid']))
		return -99
	
	rbscore = RB_score(stack)
	print('Candid %d got rbscore of %.2f from nightid %d'%(canddict['candid'], rbscore, canddict['nid']))
	return float(rbscore)
	

def create_alert_packet(cand, cm_radius = 10.0, search_history = 18000.0): #cross-match radius in arcsec, history in days

	#Connect to PGIR DB
	conn, cur = getDBCursor()
	
	#POPULATE CANDIDATE SUBSCHEMA
	candidate = {}
	for key in ['jd', 'stackquadid', 'subid', 'diffmaglim', 'pdiffimfilename', 'program', 'candid', 
	'isdiffpos', 'nid', 'quadpos', 'subquadpos', 'field', 'xpos', 'ypos', 'ra', 'dec', 'fluxpsf',
	 'sigmafluxpsf', 'magpsf', 'sigmapsf', 'chipsf', 'magap', 'sigmagap', 'fwhm', 'aimage', 'bimage',
	  'elong', 'nneg', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'tmmag1', 'tmdist1', 'tmmag2',
	   'tmdist2', 'tmmag3', 'tmdist3', 'ndethist', 'jdstarthist', 'scorr', 'jdstartref', 'jdendref',
	    'nframesref', 'magzpsci', 'magzpsciunc', 'magzpscirms', 'ncalmatches', 'clrcoeff', 'clrcounc',
	    'distnearbrstar', 'magnearbrstar', 'exptime', 'ndithexp', 'sciweight', 'refweight', 'drb',
	     'drbversion']:
		candidate[key] = cand[key]
	

	#RETRIEVE AND POPULATE CANDIDATE HISTORY AT THIS POSITION
	prevcands = []
	
	query = "SELECT cand.jd, cand.subid, cand.stackquadid, sub.limmag as diffmaglim,"\
	  "ss.descript as program, cand.candid, cand.ispos as isdiffpos, cand.nightid as nid,"\
	  "cand.quadpos, cand.subquadpos, cand.field, cand.xpos, cand.ypos, cand.ra, cand.dec,"\
	  "cand.psf_mag as magpsf, cand.psf_mag_err as sigmapsf, cand.fwhm, cand.scorr_peak as scorr,"\
	  "cand.rbscore as drb, cand.rbver as drbversion "\
	  "FROM candidates cand INNER JOIN subtractions sub ON cand.subid = sub.subid "\
	  "INNER JOIN splitstacks ss ON ss.stackquadid = sub.stackquadid "\
	  "WHERE q3c_radial_query(ra, dec, %.5f, %.5f, %.5f)"\
	  "AND cand.jd < %.5f and cand.jd > %.5f;"%(cand['ra'], cand['dec'], cm_radius/3600, cand['jd'], cand['jd'] - search_history)
	
	cur.execute(query)
	
	out = cur.fetchall()
	
	for i in range(len(out)):
		prevcand = {}
		for key in ['jd', 'subid', 'stackquadid', 'diffmaglim', 'program', 'candid', 'isdiffpos',
		 'nid', 'quadpos', 'subquadpos', 'field', 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf',
		  'fwhm', 'scorr', 'drb', 'drbversion']:
			prevcand[key] = out[i][key]
		prevcands.append(prevcand)
		
	#GET UPPER LIMIT HISTORY AT THIS POSITION
	
	query = "SELECT ss.jd, sub.subid, ss.stackquadid, sub.limmag as diffmaglim, ss.descript as program,"\
	"ss.nightid as nid, ss.quadpos, ss.subquadpos, ss.field FROM subtractions sub INNER JOIN"\
	" splitstacks ss on ss.stackquadid = sub.stackquadid INNER JOIN candidates c1 ON (sub.field ="\
	" c1.field AND sub.quadpos = c1.quadpos AND sub.subquadpos = c1.subquadpos) WHERE c1.candid = %d  AND"\
	" NOT EXISTS (SELECT * FROM candidates c2 WHERE q3c_radial_query(c2.ra, c2.dec, c1.ra, c1.dec, %.5f)"\
	"  AND c2.subid = sub.subid) AND ss.jd < %.5f AND ss.jd > %.5f"\
	" ORDER BY ss.jd"%(cand['candid'], cm_radius/3600, cand['jd'], cand['jd'] - search_history)
	
	cur.execute(query)
	
	out = cur.fetchall()
	
	for i in range(len(out)):
		prevcand = {}
		for key in ['jd', 'subid', 'stackquadid', 'diffmaglim', 'program', 'nid', 'quadpos',
		 'subquadpos', 'field']:
			prevcand[key] = out[i][key]
			
		for key in ['candid', 'isdiffpos', 'nid', 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf',
		 'fwhm', 'scorr', 'drb', 'drbversion']:
			prevcand[key] = None
		prevcands.append(prevcand)	
	
	
	closeCursor(conn, cur)		 

	alert = {"schemavsn": "0.1", "publisher": "pgirdps", 
		"cutoutScience": io.BytesIO(cand['sci_image']).read(),
		"cutoutTemplate": io.BytesIO(cand['ref_image']).read(),
		"cutoutDifference": io.BytesIO(cand['diff_image']).read(),
		"candid": cand['candid'], 
		'candidate': candidate,
		"prv_candidates": prevcands
		}
		
	return alert
	
	
	
def main(nightid, redo = False, candlimit = 10000):
	
	t0 = time.time()
	#Connect to PGIR DB
	conn, cur = getDBCursor()
	
	query = "SELECT cand.candid, "\
		"cand.jd, cand.stackquadid, cand.subid, cand.candid, cand.ispos as isdiffpos, "\
		"cand.nightid as nid, cand.quadpos, cand.subquadpos, cand.field, cand.xpos, cand.ypos, "\
		"cand.ra, cand.dec, cand.flux as fluxpsf, cand.flux_err as sigmafluxpsf, "\
		"cand.psf_mag as magpsf, cand.psf_mag_err as sigmapsf, cand.psf_chi2 as chipsf, "\
		"cand.mag as magap, cand.mag_err as sigmagap, cand.fwhm, cand.a_psf as aimage, "\
		"cand.b_psf as bimage, cand.a_psf/cand.b_psf as elong, cand.numnegpix as nneg, "\
		"cand.ssdistnr, cand.ssmagnr, cand.ssnamenr, cand.sumrat, cand.distnearbrstar,  "\
		"cand.magnearbrstar, cand.sci_weight as sciweight, cand.ref_weight as refweight, "\
		"cand.tmmag1, cand.tmdist1, cand.tmmag2, cand.tmdist2, cand.tmmag3, cand.tmdist3, "\
		"cand.nmatches as ndethist, cand.firstdet as jdstarthist, cand.scorr_peak as scorr, "\
		"cand.sent_kafka as sent, sub.limmag as diffmaglim, sub.filename as pdiffimfilename, "\
		"sp.zp_psf as magzpsci, sp.zp_psf_unc as magzpsciunc, sp.zp_psf_rms as magzpscirms, "\
		"sp.nstars_l as ncalmatches, sp.color_coeff as clrcoeff, sp.color_coeff_unc as clrcounc, "\
		"ss.descript as program, ss.exptime, ss.ndithexp, "\
		"cut.sci_image, cut.ref_image, cut.diff_image, "\
		"ref.jdstart as jdstartref, ref.jdend as jdendref, ref.numimages as nframesref "\
		"FROM candidates cand INNER JOIN cutouts cut ON cut.candid = cand.candid "\
		"INNER JOIN subtractions sub ON cand.subid = sub.subid "\
		"INNER JOIN splitstacks ss ON ss.stackquadid = sub.stackquadid "\
		"INNER JOIN squadphoto sp ON sp.stackquadid = ss.stackquadid "\
		"INNER JOIN reference_new ref ON ref.refid = cand.refid "\
		"WHERE cand.nightid=%d ORDER BY cand.jd ASC LIMIT %d;"%(nightid, candlimit)

	if not redo:
		print('Skipping done sources..')
		query = query.replace('ORDER', 'AND (NOT sent_kafka OR sent_kafka IS NULL) ORDER')
	
	cur.execute(query)
	out = cur.fetchall()

	jd_list = np.array([o['jd'] for o in out])

	print('Found %d candidates to process'%(len(jd_list)))
	if len(jd_list) == 0:
		return 0
	
	#Calculate the alert_date for this night
	minjd = np.min(jd_list)
	mjd = minjd - 2400000.5
	alert_date = Time(mjd, format = 'mjd').tt.datetime.strftime('%Y%m%d')
	
	schema = combine_schemas(["alert_schema/candidate.avsc", "alert_schema/prv_candidate.avsc", "alert_schema/alert.avsc"])
	
	topicname = 'pgir_%s'%alert_date	
	aplist = []
	for i in range(len(jd_list)):
		rbscore = getscore(out[i])		
		out[i]['drb'] = rbscore
		out[i]['drbversion'] = current_model_json
		
		pkt = create_alert_packet(out[i])
		aplist.append(pkt)
		send(topicname, [pkt], schema)
	
	
	t1 = time.time()
	print('Took %.2f seconds to process cross-matching for %d sources'%(t1 - t0, len(ra_list)))
	
	
	closeCursor(conn, cur)
	
	
if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description = 'Code to produce kafka stream for PGIR transients and send to IPAC topic')
	parser.add_argument('nightid', help = 'Night ID for cross-match')
	parser.add_argument('--redo', help = 'Add to resend sources that have already been sent', action = 'store_true', default = False)
	args = parser.parse_args()

	redo = False
	nightid = args.nightid
	if args.redo:
		redo = True
	main(int(nightid), redo = redo)
	
