import sys, os
sys.path.append('../')
from masterKeys import *
import dbOps
#Should be loaded in at the beginning of the pipeline
from keras import optimizers
import gzip
import re
from keras.models import model_from_json
# Should be loaded in once at the beginning of the pipeline
# taken from https://machinelearningmastery.com/save-load-keras-deep-learning-models/
json_directory= './json_models/'
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

#Some SS crossmatch parameters
mpc_cat_folder = '/data/kde/catalogs/mpc/'
hold_file = '/data/kde/utils/kafka_running.txt'
mpc_night_folder = './nightfiles/'
candlimit = 100000
ast_cm_radius = 100.0 # arcsec

	
def combine_schemas(schema_files):
	"""Combine multiple nested schemas into a single schema.
	Taken from Eric's lsst-dm Github page
	"""
	known_schemas = avro.schema.Names()
	#print(known_schemas)
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
	producer = confluent_kafka.Producer({'bootstrap.servers': 'ztfalerts04.ipac.caltech.edu:9092,ztfalerts05.ipac.caltech.edu:9092,ztfalerts06.ipac.caltech.edu:9092'})

	# Send an avro alert
	producer.produce(topic=topicname, value=out.read())
	producer.flush()
	

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

def getscore(canddict, silent = False):
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
	if not silent:
		print('Candid %d got rbscore of %.2f from nightid %d'%(canddict['candid'], rbscore, canddict['nid']))
	return float(rbscore)
	
	
def read_astcheck_file(astfile):
	output = astfile.readlines()
	
	output = [x.strip() for x in output]
	candnums = []
	ssdists = []
	ssmags = []
	ssnames = []

	for i in range(len(output)):
		line = output[i]
		words = line.split()
		if 'only' in words:
			candnum = int(line.split()[0][:-1])

			if len(candnums)!=0 and candnum < max(candnums):
				print('ERROR: Looks like an object is missing in the cross-match .. Exit')
				sys.exit(1)

			candline = output[i+1]
			if len(candline.split()) == 0:
				ssdist = -99
				ssmag = -99
				ssname = 'NONE'

			else:
				if re.search('[a-zA-Z]', candline.split()[1]):
					try:
						ssdist = float(candline.split()[4])
					except ValueError:
						print('Could not parse distance for candidate %d'%candnum)
						ssdist = -999
					try:
						ssmag = float(candline.split()[5])
					except ValueError:
						print('Could not parse mag for candidate %d'%candnum)
						ssmag = -999
					try:
						ssname = " ".join(candline.split()[0:2])
					except ValueError:
						print('Could not parse name for candidate %d'%candnum)
						ssname = 'ERROR'
						
				else:
					try:
						ssdist = float(candline.split()[3])
					except ValueError:
						print('Could not parse distance for candidate %d'%candnum)
						ssdist = -999
					try:
						ssmag = float(candline.split()[4])
					except ValueError:
						print('Could not parse mag for candidate %d'%candnum)
						ssmag = -999
					try:
						ssname = candline.split()[0]
					except ValueError:
						print('Could not parse name for candidate %d'%candnum)
						ssname = 'ERROR'
						
				print('Candidate number %d is %.2f arcsec away from %.2f mag rock %s'%(candnum, ssdist, ssmag, ssname))
			
			candnums.append(candnum)
			ssdists.append(ssdist)
			ssmags.append(ssmag)
			ssnames.append(ssname)
			
	return np.array(candnums), np.array(ssdists), np.array(ssmags), np.array(ssnames)
	
	
def ss_crossmatch(nightid, jd_list, ra_list, dec_list):

	print('Doing asteroid cross-matches for %d candidates .. '%len(jd_list))
	#Now do the asteroid cross-matches
	curdir = os.getcwd()
	os.chdir(mpc_cat_folder)
	astp_timelist = Time(jd_list, format = 'jd')
	sky_coord_list = SkyCoord(ra = ra_list, dec = dec_list, unit = 'degree', frame = 'icrs')
	
	candlistfile = mpc_night_folder + 'sscm_nightid%d.txt'%nightid
	
	f = open(candlistfile, 'w')
	for i in range(len(ra_list)):
		year = astp_timelist[i].datetime.year
		month = astp_timelist[i].datetime.month
		day = astp_timelist[i].datetime.day
		datefrac = (astp_timelist[i].datetime.hour + astp_timelist[i].datetime.minute / 60 + astp_timelist[i].datetime.second / 3600) / 24.0
		rahour = sky_coord_list[i].ra.hms.h
		ramin = sky_coord_list[i].ra.hms.m
		rasec = sky_coord_list[i].ra.hms.s
		decdeg = sky_coord_list[i].dec.dms.d
		decmin = sky_coord_list[i].dec.dms.m
		decsec = sky_coord_list[i].dec.dms.s

		if decdeg < 0 or decmin < 0 or decsec <0:
			f.write('%12d  C%d %02d %08.5f %02d %02d %05.2f -%02d %02d %04.1f          99.9 f      I41\n'%(i, year, month, day + datefrac, rahour, ramin, rasec, abs(decdeg), abs(decmin), abs(decsec)))
		else:
			f.write('%12d  C%d %02d %08.5f %02d %02d %05.2f +%02d %02d %04.1f          99.9 f      I41\n'%(i, year, month, day + datefrac, rahour, ramin, rasec, abs(decdeg), abs(decmin), abs(decsec)))

	f.close()
	
	astcheck_command = ['lunar/astcheck', candlistfile, '-p.', '-r%.2f'%ast_cm_radius]
	print(astcheck_command)
	#if os.path.exists(candlistfile + '.astcheck'):
	#	os.remove(candlistfile + '.astcheck')

	try:
		rval = subprocess.run(astcheck_command, check = True, stdout = subprocess.PIPE)
	except subprocess.CalledProcessError as err:
		print('Could not run astcheck .. ')
		return

	f = open(candlistfile + '.astcheck', 'w')
	f.write(rval.stdout.decode('utf-8'))
	f.close()
	
	os.chdir(curdir)

	return mpc_cat_folder + candlistfile
	
	
	
def doSScrossmatch(nightid, candid_list, jd_list, ra_list, dec_list):

	candlistfile = ss_crossmatch(nightid, jd_list, ra_list, dec_list)
	#DO THE ASTEROID CROSS_MATCHES HERE
	if candlistfile is not None:
		g = open(candlistfile + '.astcheck', 'r')
		candnums, ssdists, ssmags, ssnames = read_astcheck_file(g)
		g.close()
	else:
		ssdists = [-999 for c in candid_list]
		ssmags = [-999 for c in candid_list]
		ssnames = ['FAILED' for c in candid_list]

	if len(ssdists) != len(candid_list):
		print('ERROR: List of MPC candidates not the same as input candidates')
		return None, None, None
	
	return ssdists, ssmags, ssnames
	

def create_alert_packet(cand, scicut, refcut, diffcut, cur, conn, cm_radius = 8.0, search_history = 90.0): #cross-match radius in arcsec, history in days
	
	#POPULATE CANDIDATE SUBSCHEMA
	candidate = {}
	for key in ['jd', 'stackquadid', 'subid', 'diffmaglim', 'pdiffimfilename', 'program', 'candid', 'nid',
	'quadpos', 'subquadpos', 'field', 'xpos', 'ypos', 'ra', 'dec', 'fluxpsf',
	 'sigmafluxpsf', 'magpsf', 'sigmapsf', 'chipsf', 'magap', 'sigmagap', 'fwhm', 'aimage', 'bimage',
	 'elong', 'nneg', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'isstar', 'sumrat', 'dmag2mass',
	  'tmmag1', 'tmdist1', 'tmmag2', 'tmdist2', 'tmmag3', 'tmdist3', 'ndethist', 'jdstarthist', 'scorr',
	  'jdstartref', 'jdendref', 'nframesref', 'magzpsci', 'magzpsciunc', 'magzpscirms', 'ncalmatches', 
	  'clrcoeff', 'clrcounc', 'distnearbrstar', 'magnearbrstar', 'exptime', 'ndithexp', 'sciweight',
	   'refweight', 'drb', 'drbversion']:
		candidate[key] = cand[key]
	if cand['isdiffpos'] == 1:
		candidate['isdiffpos'] = '1'
	

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
		for key in ['jd', 'subid', 'stackquadid', 'diffmaglim', 'program', 'candid',
		 'nid', 'quadpos', 'subquadpos', 'field', 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf',
		  'fwhm', 'scorr', 'drb', 'drbversion']:
			prevcand[key] = out[i][key]
		if out[i]['isdiffpos'] == 1:
			prevcand['isdiffpos'] = '1'
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
		 'subquadpos', 'field', 'nid']:
			prevcand[key] = out[i][key]
			
		for key in ['candid', 'isdiffpos', 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf',
		 'fwhm', 'scorr', 'drb', 'drbversion']:
			prevcand[key] = None
		prevcands.append(prevcand)	
			 

	alert = {"schemavsn": "0.1", "publisher": "pgirdps", 
		"cutoutScience": scicut,
		"cutoutTemplate": refcut,
		"cutoutDifference": diffcut,
		"objectId": cand['objectId'],
		"candid": cand['candid'], 
		"candidate": candidate,
		"prv_candidates": prevcands
		}
		
	return alert
	
	
	
def broadcast_alert_packet(cand, scicut, refcut, diffcut, topicname, schema, mycandnum, numcands):
		
	#Connect to PGIR DB
	conn, cur = dbOps.getDBCursor(silent = True)	
	
	pkt = create_alert_packet(cand, scicut, refcut, diffcut, cur, conn)
	try:
		send(topicname, [pkt], schema)
		dbOps.updateSingleTab(cur, conn, 'candidates', ['sent_kafka'], ['t'], 'candid', cand['candid'])
		print('Sent candid %d name %s, %d out of %d'%(cand['candid'], cand['objectId'], mycandnum, numcands))
		dbOps.closeCursor(conn, cur, silent = True)
		return cand['candid']
	except OSError:
		print('Could not send candid %d'%cand['candid'])
		dbOps.closeCursor(conn, cur, silent = True)
		return -1
		
		
		
def get_next_name(lastname, candjd, bwfile = 'badwords.txt', basename = 'PGIR', begcount = 'aaaaaa'):

	#ASSUMING names of the form 'PGIR19aaaaaa' 

	curyear = Time(candjd, format = 'jd').datetime.strftime('%Y')[2:4]
	
	if lastname is None:
		#If this is the first source being named
		newname = basename + curyear + begcount
		return newname
	
	lastyear = lastname[4:6]

	if curyear != lastyear:
		#If this is the first candidate of the new year, start with aaaaa
		newname = basename + curyear + begcount
		return newname
	else:
		lastcount = lastname[6:]
		charpos = len(lastcount) - 1
		#will iteratively try to increment characters starting from the last
		inctrue = False
		usestring = ''
		while charpos >= 0:
			cref = lastcount[charpos]
			if inctrue:
				usestring = cref + usestring
				charpos -= 1
				continue
			creford = ord(cref)
			#increment each character, if at 'z', increment the next one
			if creford + 1 > 122:
				usestring = 'a' + usestring
			else:
				nextchar = chr(creford+1)
				usestring = nextchar + usestring
				inctrue = True
			charpos -= 1
			continue
		
		newname = basename + curyear + usestring
		bwlist = ascii.read(bwfile, format = 'no_header')
		isbw = False
		#check for bad word
		for i in range(len(bwlist)):
			if usestring.find(str(bwlist['col1'][i])) != -1:
				#Shame shame
				isbw = True
				break
		if isbw:
			#increment the name with a recursive call
			return get_next_name(newname, candjd)
		else:
			return newname

def check_and_insert_source(conn, cur, candname, ra, dec, candid, cm_radius = 8.0):
	#check if source exists

	query = 'SELECT name from pgirnames p where q3c_radial_query(p.ra, p.dec, %.5f, %.5f, %.5f);'%(ra, dec, cm_radius/3600)

	cur.execute(query)
	out = cur.fetchall()
	nummatches = len(out)
	if nummatches > 0:
		prename = out[0]['name']
		print('Candname %s already exists!'%prename)
		return prename, False

	tabName = 'pgirnames'
	insert_schema = 'name, ra, dec, candid'
	values = [candname, ra, dec, candid]
	numValues = len(values)
	valueString = ",".join(["%s" for x in range(numValues)])

	print('Inserting source %s at ra %.5f dec %.5f'%(candname, ra, dec))

	insertString = 'INSERT INTO %s (%s) VALUES (%s) RETURNING %s'%(tabName, insert_schema, valueString, 'pgirid')
	cur.execute(insertString, values)
	retVal = cur.fetchone()
	print(retVal)
	conn.commit()
	
	return candname, True

		

def main(nightid, redo = False, skiprb = False, skipss = False, candlimit = 10000, rbcut = 0.5):
	
	t0 = time.time()
	#Connect to PGIR DB
	conn, cur = dbOps.getDBCursor()
	
	query = "SELECT cand.candid, "\
		"cand.jd, cand.stackquadid, cand.subid, cand.candid, cand.ispos as isdiffpos, "\
		"cand.nightid as nid, cand.quadpos, cand.subquadpos, cand.field, cand.xpos, cand.ypos, "\
		"cand.ra, cand.dec, cand.flux as fluxpsf, cand.flux_err as sigmafluxpsf, "\
		"cand.psf_mag as magpsf, cand.psf_mag_err as sigmapsf, cand.psf_chi2 as chipsf, "\
		"cand.mag as magap, cand.mag_err as sigmagap, cand.fwhm, cand.a_psf as aimage, "\
		"cand.b_psf as bimage, cand.a_psf/cand.b_psf as elong, cand.numnegpix as nneg, "\
		"cand.ssdistnr, cand.ssmagnr, cand.ssnamenr, cand.isstar, cand.sumrat, cand.dmag2mass,"\
		"cand.distnearbrstar, cand.rbscore, cand.rbver, "\
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
		
	if skiprb:
		print('Skipping RB computation and using only RB-existing candidates')
		query = query.replace('WHERE', 'WHERE cand.rbscore != -1 AND')
		
	if skipss:
		print('Skiping SS crossmatch and using only SS existing candidates')
		#At this time, there is nothing that says that a SS crossmatch has taken place
		#So we rely only the rbscore criteria to determine this
		query = query.replace('WHERE', 'WHERE cand.rbscore != -1 AND')
		
	print(query)
	
	cur.execute(query)
	candlist = cur.fetchall()
	
	#Get the latest name in the list of PGIR names
	query = 'SELECT max(name) as lastname from pgirnames;'
	cur.execute(query)
	out = cur.fetchone()
	lastname = out['lastname']

	candid_list = np.array([o['candid'] for o in candlist])
	jd_list = np.array([o['jd'] for o in candlist])
	ra_list = np.array([o['ra'] for o in candlist])
	dec_list = np.array([o['dec'] for o in candlist])
	num_cands = len(jd_list)
	print('Found %d candidates to process'%(num_cands))
	if num_cands == 0:
		return 0
	
	#Calculate the alert_date for this night
	minjd = np.min(jd_list)
	alert_date = Time(minjd, format = 'jd').tt.datetime.strftime('%Y%m%d')
	
	schema = combine_schemas(["alert_schema/candidate.avsc", "alert_schema/prv_candidate.avsc", "alert_schema/alert.avsc"])
	
	if not skipss:
		ssdists, ssmags, ssnames = doSScrossmatch(nightid, candid_list, jd_list, ra_list, dec_list)
	
	topicname = 'pgir_%s'%alert_date	
	numgoodcands = 0
	for i in range(num_cands):
		if skiprb:
			candlist[i]['drb'] = candlist[i]['rbscore']
			candlist[i]['drbversion'] = candlist[i]['rbver']
		else:
			rbscore = getscore(candlist[i], silent = True)		
			candlist[i]['drb'] = rbscore
			candlist[i]['drbversion'] = current_model_json
			
		if not skipss:
			candlist[i]['ssdistnr'] = ssdists[i]
			candlist[i]['ssmagnr'] = ssmags[i]
			candlist[i]['ssnamenr'] = ssnames[i]
		
		if candlist[i]['drb'] >= rbcut:
			numgoodcands += 1
		
		candname = get_next_name(lastname, candlist[i]['jd'])
		
		candname, newstatus = check_and_insert_source(conn, cur, candname, candlist[i]['ra'], candlist[i]['dec'], candlist[i]['candid'])
		if newstatus:
			lastname = candname
			candlist[i]['objectId'] = candname
		else:
			candlist[i]['objectId'] = candname
			continue
		
		
	#Now create and broadcast alert packets in parallel
	pool = mp.Pool(processes = 10)
	process_list = []
	goodcandcount = 0
	for i in range(num_cands):
		if candlist[i]['drb'] < rbcut:
			dbOps.updateSingleTab(cur, conn, 'candidates', ['sent_kafka'], ['t'], 'candid', candlist[i]['candid'])
			print('Candidate %d Object %s with low RB %.2f not sent'%(candlist[i]['candid'], candlist[i]['objectId'], candlist[i]['drb']))
			continue
			
		goodcandcount += 1
		canddict = candlist[i].copy()
		#converting memoryview cutouts to bytes before sending to parallelized broadcast
		scicut = io.BytesIO(canddict.pop('sci_image')).read()
		refcut = io.BytesIO(canddict.pop('ref_image')).read()
		diffcut = io.BytesIO(canddict.pop('diff_image')).read()
		process_list.append(pool.apply_async(broadcast_alert_packet, args = (canddict, scicut, refcut, diffcut, topicname, schema, goodcandcount, numgoodcands,)))
	
	#Run all the broadcasts in parallel
	num_broadcast = len(process_list)
	results = [p.get() for p in process_list]
	pool.close()
	
	t1 = time.time()
	print('Took %.2f seconds to process %d candidates and broadcast %d candidates'%(t1 - t0, num_cands, num_broadcast))
	
	
	dbOps.closeCursor(conn, cur)
	
	
if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description = 'Code to produce kafka stream for PGIR transients and send to IPAC topic')
	parser.add_argument('nightid', help = 'Night ID for cross-match; Use -1 for today', type = int)
	parser.add_argument('--redo', help = 'Add to resend sources that have already been sent', action = 'store_true', default = False)
	parser.add_argument('--skiprb', help = 'Add to skip RB computation and use only candidates with existing RBs', action = 'store_true', default = False)
	parser.add_argument('--skipss', help = 'Add to skip SS crossmatch and use only candidates with existing SS crossmatch', action = 'store_true', default = False)
	args = parser.parse_args()
	
	dateNow = datetime.now()                #local time
	dateUTCNow = datetime.utcnow()          # UTC now
	
	if os.path.exists(hold_file):
		print('Another kafka upload instance running -- will not run..')
		sys.exit(1)
		
	f = open(hold_file, 'w')
	f.write(dateNow.strftime('%Y %M %D %H:%M:%S'))
	f.close()
	
	nightid = args.nightid
	if nightid == -1:
		#Compute current night ID
		nightid = (dateNow - drpRefDate).days
	
	print('Running at %s. Night ID is %d .. looking for new candidates'%(dateNow, nightid))
	
	main(nightid, redo = args.redo, skiprb = args.skiprb, skipss = args.skipss)
	
	os.remove(hold_file)
	
