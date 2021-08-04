from survey.masterKeys import *

logger = logging.getLogger(__name__)

def getDBCursor(silent = False):
	#Let's connect to the DB
	if not silent:
		logger.info('Connecting to the gattini database .. ')
	try:
		conn = psycopg2.connect(gattinibot_login)
	except psycopg2.Error as err: 
		logger.warning("I cannot access gattini. ERROR: %s\n"%err)	
		return -1, -1
	if not silent:
		logger.info('I am connected to gattini .. ')

	#Initializing a cursor
	cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
	return conn, cur
	
def getYupanaDBCursor(silent = False):
	#Let's connect to the DB
	if not silent:
		logger.info('Connecting to the gattini database .. ')
	try:
		conn = psycopg2.connect(gattini_yupana_login)
	except psycopg2.Error as err: 
		logger.warning("I cannot access gattini. ERROR: %s\n"%err)	
		return -1, -1
	if not silent:
		logger.info('I am connected to gattini on yupana .. ')

	#Initializing a cursor
	cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
	return conn, cur
	
	
def closeCursor(conn, cur, silent = False):
	try: 
		conn.commit()
		cur.close()
		conn.close()
	except psycopg2.Error as err:
		logger.warning('Could not close cursor. ERROR: %s\n'%err)
		return
	if not silent:
		logger.info('Closed connection to gattinibot')

	

def getNearestCal(cursor, JD, calTable, program = None):
	'''
	Get calibration file acquired nearest to the input JD
	'''
	
	if program is None:
		query = 'SELECT * FROM %s WHERE abs(%.3f - jd) = (SELECT min(abs(%.3f - jd)) FROM %s WHERE qualityflag = 1) and qualityflag = 1'%(calTable, JD, JD, calTable)
	else:
		query = "SELECT * FROM %s WHERE abs(%.3f - jd) = (SELECT min(abs(%.3f - jd)) FROM %s WHERE qualityflag = 1 and program = '%s') and qualityflag = 1 and program = '%s'"%(calTable, JD, JD, calTable, program, program)		
	
	try:
		cursor.execute(query)
		out = cursor.fetchone()
	except psycopg2.Error as err:
		logger.warning(error)
		return None
	else:
		return out

		
def getExistingReference(cursor, field, quadPos):
	query = 'select * from %s where field = %d and quadpos = %d and numimages > 0;'%(referenceTable, field, quadPos)
	
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as err:
		logger.warning(error)
		return None
	else:
		return out


def getReferenceProcFiles(cursor, field, quadPos, nightID):
	query = 'select * from %s qa, %s p where p.quadid = qa.quadid and p.field = %d and p.quadpos = %d and nightid = %d and astromsigma_reference1 != 0 and astromsigma_reference1 != -99 and p.envhum < %.1f and p.airmass < %.2f and p.moondist > %.2f and exists (select * from %s qs, %s sp where qs.quadid = p.quadid and qs.stackid = sp.stackid and sp.limmagrms2 > %.2f)'%(astQuadTable, quadTable, field, quadPos, nightID,  ref_limhum, ref_limairmass, ref_limmoondist, stackQuadTable, stackPhotoTable, ref_limmag)

	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as err:
		logger.warning(error)
		return None
	else:
		return out


def getRawImagesForDPMs(cursor, nightid):
	'''
	Get raw images acquired on a given night ID as sky flats with different exposure times to make a dead pixel mask
	'''

	query = 'SELECT * FROM %s r WHERE nightid > %d AND nightid <= %d AND field = %d AND envhum != -1 and moondist > %f and envhum < %f and data50p > %.2f and data50p < %.2f and not exists (select * from %s cd where r.nightid = cd.nightid) order by envhum asc'%(rawTable, nightid - int(dpm_datelim), nightid, dpmlow_field, flat_moonDistLim, flat_humLim, dpm_low_lc, dpm_low_hc, calDPMTable)
	
	print(query)
	
	
	try:
		cursor.execute(query)
		filelowlist = cursor.fetchall()
	except psycopg2.Error as err:
		logger.warning(error)
		return [], []

	query = 'SELECT * FROM %s r WHERE nightid > %d AND nightid <= %d AND field = %d AND envhum != -1 and moondist > %f and envhum < %f and data50p > %.2f and data50p < %.2f and not exists (select * from %s cd where r.nightid = cd.nightid) order by envhum asc'%(rawTable, nightid - int(dpm_datelim), nightid, dpmhigh_field, flat_moonDistLim, flat_humLim, dpm_high_lc, dpm_high_hc, calDPMTable)
	
	print(query)
	
	try:
		cursor.execute(query)
		filehighlist = cursor.fetchall()
	except psycopg2.Error as err:
		logger.warning(error)
		return [], []
			
	return filelowlist, filehighlist


def getRawImagesForFlats(cursor, JD):
	'''
	Get all images acquired between JD-flatFieldDateLim and JD, larger then moonDistLim away from the moon, humidity lower than flat_humLim and counts below a certain level
	'''

	query = 'SELECT * FROM %s r WHERE jd > %f AND jd < %f AND field != %d AND envhum != -1 and moondist > %f and envhum < %f and data50p < %.2f and not exists (select * from %s cf where r.nightid = cf.nightid) order by envhum asc'%(rawTable, JD-flatFieldDateLim, JD, field_testID, flat_moonDistLim, flat_humLim, flat_countLim, calFlatTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as err:
		logger.warning(error)
		return None
	else:
		return out


def getFieldsForReferences(nightID, cursor):
	query = 'select * from %s r where exists (select * from %s qa, %s p where p.quadid = qa.quadid and astromsigma_reference1 !=0 and astromsigma_reference1 != -99 and p.nightid= %d and r.field = p.field and r.quadpos = p.quadpos and p.envhum < %.1f and p.airmass < %.2f and p.moondist > %.2f and exists (select * from %s qs, %s sp where qs.quadid = p.quadid and qs.stackid = sp.stackid and sp.limmagrms2 > %.2f)) and r.numimages < %d  order by field, quadpos;'%(referenceTable, astQuadTable, quadTable, nightID, ref_limhum, ref_limairmass, ref_limmoondist, stackQuadTable, stackPhotoTable, ref_limmag, numReferenceFiles)

	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	logger.info('Found %d field quadrants that need references'%len(out))
	return out
		
	
def getStackQuadsReadyForPhoto(cursor, nightID):
	query = 'SELECT sq.* FROM %s sq WHERE sq.nightid = %d and longstack = false and NOT EXISTS (SELECT * FROM %s p WHERE p.stackquadid = sq.stackquadid)'%(stackScienceQuadTable, nightID, stackScienceQuadPhotoTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	stackPhotoList = []
	allNewFieldSeq = [d['fieldseq'] for d in out]
	allNewFields = [d['field'] for d in out]
	unique_fieldSeq = np.unique(allNewFieldSeq)

	#Get all sub-quadrants for a given field and field sequence
	for fieldseq in unique_fieldSeq:
		fieldSeqFileIndices = np.where(allNewFieldSeq == fieldseq)[0]
		
		unique_fields = np.unique(np.array(allNewFields)[fieldSeqFileIndices])
		
		for field in unique_fields:
			fieldFileIndices = np.where((allNewFieldSeq == fieldseq) & (allNewFields == field))[0]
			if len(fieldFileIndices) == 0:
				continue

			stackPhotoList.append([out[x] for x in fieldFileIndices])

	return stackPhotoList
	

def getStacksForSplitting(cursor, nightID):
	query = 'SELECT * FROM %s ss where nightid = %d and longstack = false and not exists (select * from %s sq where sq.stackid = ss.stackid);'%(stackScienceTable, nightID, stackScienceQuadTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	return out
	

def getQuadsReadyForStacking(cursor, nightID):
	'''
	Function to get a list of quadrants for stacking
	
	Arguments
	----------------------
	cursor: psycopg2 cursor
	For accessing the DB
	
	nightID: int
	the night ID for processing
	
	Returns
	---------------------
	masterStackQuadList: list
	Each element of the array corresponds to a single field sequence and quadrant and contains a list of dicts corresponding to each quadrant to be stacked in that field sequence and quadrant
	'''

	#get all quadrants for a particular night ID, and field not equal to test field, and there is a corresponding succesful astrometric solution, and has not been used for stacking yet
	#UPDATE ON 2020-02-26: Only select quadrants that have more than one dither
	query = 'SELECT * FROM %s q WHERE nightid = %d and field != %d and EXISTS (SELECT * FROM %s a WHERE a.quadid = q.quadid and astromsigma_reference1 != 0 and astromsigma_reference1 != -99) and NOT EXISTS (SELECT * FROM %s s WHERE s.quadid = q.quadid) and q.ndithexp > 1;'%(quadTable, nightID, field_testID, astQuadTable, stackQuadTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	#Looking for all quadrants where all the dithers have been acquired
	allNewFieldSeq = [d['fieldseq'] for d in out]
	allNewFields = [d['field'] for d in out]
	allNewFileQuadPos = [d['quadpos'] for d in out]
	unique_fieldSeq = np.unique(allNewFieldSeq)
	
	masterStackQuadList = []
	
	#There's probably a better way to do this :/
	#For each field sequence
	for fieldseq in unique_fieldSeq:
		#For each quadrant
		for j in quad_ids:
			fieldSeqQuadList = []
			fieldSeqFileIndices = np.where((allNewFieldSeq == fieldseq) & (np.array(allNewFileQuadPos) == j))[0]

			unique_fields = np.unique(np.array(allNewFields)[fieldSeqFileIndices])
			
			#For each field			
			for field in unique_fields:
				fieldFileIndices = np.where((allNewFieldSeq == fieldseq) & (np.array(allNewFileQuadPos) == j) & (allNewFields == field))[0]
				
				if len(fieldFileIndices) == 0:
					continue
				numDithExp = out[fieldFileIndices[0]]['ndithexp']
				#If the number of images is equal to the number of dithers expected, add this to the stacking list
				if len(fieldFileIndices) == numDithExp:
					fieldSeqQuadList.extend([out[x] for x in fieldFileIndices])
	

			if len(fieldSeqQuadList) == 0:
				continue
			#Add the quadrant dither sequence to the stacking list
			masterStackQuadList.append(fieldSeqQuadList)
	
	return masterStackQuadList
	
	
def getNearObjects(cursor_tmass, cursor_ps1, ra, dec):
	sg_table = 'ps1_sg'
	nearby_star_dist = 60.0/3600
	
	tmass_query = 'select * from twomass_psc where q3c_radial_query(ra, decl, %.4f, %.4f, %.4f) and j_m < 15'%(ra, dec, nearby_star_dist)
	ps1_sg_query = 'select * from ps1_sg where q3c_radial_query(rastack, decstack, %.4f, %.4f, %.4f)'%(ra, dec, nearby_star_dist)
	
	try:
		cursor_tmass.execute(tmass_query)
		out_tmass = cursor_tmass.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
		
	try:
		cursor_ps1.execute(ps1_sg_query)
		out_ps1 = cursor_ps1.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
		
	
	cand_coord = SkyCoord(ra, dec, unit='deg', frame='icrs')	
	
	near_star_dict = {}
	
	seps_tmass = []
	for i in range(len(out_tmass)):
		objra = out_tmass[i]['ra']
		objdec = out_tmass[i]['decl']
		obj_coord = SkyCoord(ra=objra, dec=objdec, unit='deg', frame='icrs')
		seps_tmass.append(obj_coord.separation(cand_coord).arcsec)

	seps_tmass = np.array(seps_tmass)
	dist_order_tmass = np.argsort(seps_tmass)
	seps_tmass = np.sort(seps_tmass)
	
	tmass_mags_sorted = [out_tmass[x]['j_m'] for x in dist_order_tmass]
	
	for i in range(3):
		if i<len(seps_tmass):
			near_star_dict['tmass_mag%d'%(i+1)] = tmass_mags_sorted[i]
			near_star_dict['tmass_dist%d'%(i+1)] = seps_tmass[i]
		else:
			near_star_dict['tmass_mag%d'%(i+1)] = -99
			near_star_dict['tmass_dist%d'%(i+1)] = -99	
	
	seps_ps1 = []
	for i in range(len(out_ps1)):
		objra = out_ps1[i]['rastack']
		objdec = out_ps1[i]['decstack']
		obj_coord = SkyCoord(ra=objra, dec=objdec, unit='deg', frame='icrs')
		seps_ps1.append(obj_coord.separation(cand_coord).arcsec)
	
	seps_ps1 = np.array(seps_ps1)
	dist_order_ps1 = np.argsort(seps_ps1)
	seps_ps1 = np.sort(seps_ps1)
	
	rfscores_sorted = [out_ps1[x]['rfscore'] for x in dist_order_ps1]
	
	for i in range(3):
		if i<len(seps_ps1):
			near_star_dict['ps1_sgscore%d'%(i+1)] = rfscores_sorted[i]
			near_star_dict['ps1_dist%d'%(i+1)] = seps_ps1[i]
		else:
			near_star_dict['ps1_sgscore%d'%(i+1)] = -99
			near_star_dict['ps1_dist%d'%(i+1)] = -99
		
	return near_star_dict
	
		
def getQuadsReadyForAstrometry(cursor, nightID):
	'''
	Function to get a list of quadrants for which an astrometric solution is to be derived
	
	Arguments
	-----------------
	cursor: psycopg2 cursor
	To access the database
	
	nightID: int
	night ID in Gattini
	
	Returns
	------------------
	masterAstromQuadList: list
	Each element of the array corresponds to a single field sequence and contains a list of dicts
	corresponding to each quadrant to be solved in that field sequence
	'''
	
	#get quadrants with the same night id where the field is not a test field and there is no corresponding entry in the table with quad astrometry done
	query = 'SELECT * FROM %s q WHERE nightid = %d and field != %d and field != %d and field != %d and NOT EXISTS (SELECT * FROM %s a WHERE q.quadid = a.quadid)'%(quadTable, nightID, field_testID, dpmlow_field, dpmhigh_field, astQuadTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	#I only want to select quadrants for which all the dithers in the dither sequence have been completed
	allNewFieldSeq = np.array([d['fieldseq'] for d in out])
	allNewFields = np.array([d['field'] for d in out])
	allNewFileQuadPos = np.array([d['quadpos'] for d in out])
	unique_fieldSeq = np.unique(allNewFieldSeq)
	unique_fields = np.unique(allNewFields)
	
	masterAstromQuadList = []	
	
	#There's probably a better way to do this :/ But the scheduler keeps mixing up the fieldseq numbers, so have to stick to this
	for fieldseq, field in itertools.product(unique_fieldSeq, unique_fields):
		fieldSeqList = []
		for j in quad_ids:
			fieldFileIndices = np.where((allNewFieldSeq == fieldseq) & (np.array(allNewFileQuadPos) == j) & (allNewFields == field))[0]
			if len(fieldFileIndices) == 0:
				continue
			numDithExp = out[fieldFileIndices[0]]['ndithexp']
			if len(fieldFileIndices) == numDithExp:
				fieldSeqList.extend([out[x] for x in fieldFileIndices])
			
		if len(fieldSeqList) == 0:
			continue
		masterAstromQuadList.append(fieldSeqList)		
	
	#Return the list of fields to be solved for astrometry
	return masterAstromQuadList
	
	

def getStackedScienceWithNightID(cursor, nightID):
	query = 'SELECT * FROM %s WHERE nightid = %d and longstack = false'%(stackScienceTable, nightID)
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	else:
		return out
		
		

def getSplitStacksWithNightID(cursor, nightID):
	query = 'SELECT * FROM %s WHERE nightid = %d'%(stackScienceQuadTable, nightID)
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	else:
		return out
		

def getFieldsWithinRadius(cursor, ra, dec, radius):
	query = 'SELECT * FROM fields WHERE q3c_radial_query(objrad, objdecd, %.3f, %.3f, %.3f) and field != -99;'%(ra, dec, radius)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	else:
		return out
		

def getAllPhotoStacksFromField(cursor, field, quad, minNight, start_jd):
	query = 'SELECT * FROM %s ss WHERE nightid >= %d and jd >= %.2f and EXISTS (select * from %s sp where zp1_mean != -1 and sp.stackid = ss.stackid) and field = %d and quadpos = %d;'%(stackScienceTable, minNight, start_jd, stackPhotoTable, field, quad)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	else:
		return out
		
		
def getFilesFromFieldID(cursor, fieldID, nightID):
	query = 'SELECT * from %s WHERE nightid = %d and field = %d order by datebeg asc'%(quadTable, nightID, fieldID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	else:
		return out
		
def getQuadrantsWithNightID(cursor, nightID):
	query = 'SELECT * FROM %s WHERE nightid = %d AND field != %d'%(quadTable, nightID, field_testID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None
	else:
		return out
		
def getNumIncompleteQuadSequence(cursor, nightID):
	
	query = 'select fieldseq, field, quadpos, ndithexp, count(*) c from %s where nightid = %d group by fieldseq, field, quadpos, ndithexp order by c;'%(quadTable, nightID)

	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None

	numIncomplete = 0
	
	for i in range(len(out)):
		if out[i]['c'] != out[i]['ndithexp']:
			numIncomplete += out[i]['c']

	return numIncomplete
	
	
def getNumExpStacks(cursor, nightID):
	query = 'select fieldseq, field, quadpos, ndithexp, count(*) c from %s p where nightid = %d and exists (select * from %s qa where qa.quadid = p.quadid) group by fieldseq, field, quadpos, ndithexp order by c;'%(quadTable, nightID, astQuadTable)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None
	else:
		return len(out)
		

def getNightReportStats(cursor, nightID):

	outall, outsci = getRawImageWithNightID(cursor, nightID)
	outast, outsciast = getRawImageWithNightID(cursor, nightID, astrometry = True)
	outprocquad = getQuadrantsWithNightID(cursor, nightID)
	outscistackquads = getStackedScienceWithNightID(cursor, nightID)
	outsplitstacks = getSplitStacksWithNightID(cursor, nightID)
	numastquads_incomplete = getNumIncompleteQuadSequence(cursor, nightID)
	numexpstacksciquads = getNumExpStacks(cursor, nightID)

	humiditystats = getHumidityStats(cursor, nightID)
	moonstats = getMoonStats(cursor, nightID)
	aststats = getAstrometryStats(cursor, nightID)
	photstats = getPhotometryStats(cursor, nightID)
	focstats = getfocstats(cursor, nightID)

	numallingest = len(outall)
	numsciingest = len(outsci)
	numastexists = len(outast)
	numProcQuads = len(outprocquad)
	numExpAstQuads = numProcQuads - numastquads_incomplete
	numStackSciQuads = len(outscistackquads)
	numsplitstacks = len(outsplitstacks)
	focstring = "\n".join(['%s\t%s'%(f['focpos'], f['count']) for f in focstats])
	
	reportStats = {"numallingest": numallingest, "numsciingest": numsciingest, "numastexists": numastexists, "numprocquads": numProcQuads, "numexpastquads": numExpAstQuads, "numstacksciquads": numStackSciQuads, "numexpstacksciquads": numexpstacksciquads, "numsplitstacks":numsplitstacks, "avgenvhum": humiditystats['avgenvhum'], "maxenvhum": humiditystats['maxenvhum'], "minenvhum": humiditystats['minenvhum'], "moondistavg": moonstats["moondistavg"], "moondistmin": moonstats["moondistmin"], "moondistmax": moonstats["moondistmax"], "moonphaseavg": moonstats["moonphaseavg"], "numast": aststats['count'], "avgsig1": aststats['avgsig1'], "avgsig2": aststats['avgsig2'], "maxsig1": aststats['maxsig1'], "maxsig2": aststats['maxsig2'], "minsig1": aststats['minsig1'], "minsig2": aststats['minsig2'], "numphot": photstats['count'], "avglimmag": photstats['avglimmag'], "maxlimmag": photstats['maxlimmag'], "minlimmag": photstats['minlimmag'], "avgnstars": photstats['avgnstars'],  "minnstars": photstats['minnstars'], "maxnstars": photstats['maxnstars'], "avgfwhm": photstats['avgfwhm'], "minfwhm": photstats['minfwhm'], "maxfwhm": photstats['maxfwhm'], "minzpstd": photstats['minzpstd'], "maxzpstd": photstats['maxzpstd'], "avgzpstd": photstats['avgzpstd'], "focstatus": focstring}
	
	return reportStats
	
def getfocstats(cursor, nightID):
	query = "SELECT distinct(focus1, focus2, focus3) as focpos, count(*) FROM %s WHERE nightid = %d AND field != %d AND field > 0 AND descript != 'FOCUS' group by focpos"%(rawTable, nightID, field_testID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None
		
	return out
	
	
def getRawImageWithNightID(cursor, nightID, field = None, astrometry = False):
	if field is None:
		query = 'SELECT * FROM %s WHERE nightid = %d AND field != %d'%(rawTable, nightID, field_testID)
	else:
		query = 'SELECT * FROM %s WHERE nightid = %d AND field = %d'%(rawTable, nightID, field)
		
	if astrometry:
		query += ' AND astromok = 1'
		
	try:
		cursor.execute(query)
		out_all = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None, None
	
	query = 'SELECT * FROM %s WHERE nightid = %d AND field != %d and field != %d and field != %d'%(rawTable, nightID, field_testID, dpmlow_field, dpmhigh_field)

	if astrometry:
		query += 'AND astromok = 1'

	try:
		cursor.execute(query)
		out_sci = cursor.fetchall()
	except psycopg2.Error as error:
		print(error)
		return None, None

	return out_all, out_sci
		

def getPhotometryStats(cursor, nightID):

	def boxcoord(q, sq):
		if q == 0:
			xlow = 0
			ylow = 0
		elif q == 1:
			xlow = 0.5
			ylow = 0
		elif q == 2:
			xlow = 0
			ylow = 0.5
		elif q == 3:
			xlow = 0.5
			ylow = 0.5
			
		if sq == 0:
			xlow += 0
			ylow += 0
		elif sq == 1:
			xlow += 0.25
			ylow += 0
		elif sq == 2:
			xlow += 0
			ylow += 0.25
		elif sq == 3:
			xlow += 0.25
			ylow += 0.25
			
		return xlow, ylow


	query = "select count(*), avg(psffwhm_mean) avgfwhm, min(psffwhm_mean) minfwhm, max(psffwhm_mean) maxfwhm, min(zp_psf_rms) minzpstd, max(zp_psf_rms) maxzpstd, avg(zp_psf_rms) avgzpstd, avg(limmagpsf) avglimmag, max(limmagpsf) maxlimmag, min(limmagpsf) minlimmag, avg(nstars_l) avgnstars, min(nstars_l) minnstars, max(nstars_l) maxnstars, avg(moonra) as moonra, avg(moondec) as moondec from %s sp, %s ss where nightid=%d and limmagpsf != -1 and descript != 'FOCUS' and sp.stackquadid = ss.stackquadid;"%(stackScienceQuadPhotoTable, stackScienceQuadTable, nightID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchone()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	photstats = {}
	if out['count'] != 0:
		photstats['count'] = out['count']
		photstats['avglimmag'] = out['avglimmag']
		photstats['maxlimmag'] = out['maxlimmag']
		photstats['minlimmag'] = out['minlimmag']
		photstats['avgnstars'] = out['avgnstars']
		photstats['minnstars'] = out['minnstars']
		photstats['maxnstars'] = out['maxnstars']
		photstats['avgfwhm'] = out['avgfwhm']*4.35
		photstats['minfwhm'] = out['minfwhm']*4.35
		photstats['maxfwhm'] = out['maxfwhm']*4.35
		photstats['minzpstd'] = out['minzpstd']
		photstats['maxzpstd'] = out['maxzpstd']
		photstats['avgzpstd'] = out['avgzpstd']
	else:
		photstats['count'] = 0
		photstats['avglimmag'] = 0
		photstats['maxlimmag'] = 0
		photstats['minlimmag'] = 0
		photstats['avgnstars'] = 0
		photstats['minnstars'] = 0
		photstats['maxnstars'] = 0
		photstats['avgfwhm'] = 0
		photstats['minfwhm'] = 0
		photstats['maxfwhm'] = 0
		photstats['minzpstd'] = 0
		photstats['maxzpstd'] = 0
		photstats['avgzpstd'] = 0
		return photstats
		
	moonra = out['moonra'] * 180 / np.pi
	moondec = out['moondec'] * 180 / np.pi
	####CREATE A SKY COVERAGE, DEPTH AND ZP SCATTER MAP
	#First create the photometric solution map
	query = 'select field, quadpos, subquadpos, avg(limmagpsf) as limmagpsf, avg(zp_psf_rms) as zp_rms, count(*) as count from %s ss inner join %s sp on sp.stackquadid = ss.stackquadid where nightid = %d and limmagpsf != -1 group by field, quadpos, subquadpos;'%(stackScienceQuadTable,stackScienceQuadPhotoTable, nightID)

	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
		
	limmags = [o['limmagpsf'] + vegatoab for o in out]
	zprms = [o['zp_rms'] for o in out]
	numvisits = [o['count'] for o in out]
	fields = [o['field'] for o in out]
	quadpos = [o['quadpos'] for o in out]
	subquadpos = [o['subquadpos'] for o in out]
	
	racent = np.zeros(len(fields))
	deccent = np.zeros(len(fields))

	for i in range(len(fields)):
		f = fields[i]
		q = quadpos[i]
		s = subquadpos[i]
	
		fieldwcsloc = fieldstack_wcsLoc + 'field%d_q%d.wcs'%(f, q)
		h = fits.Header.fromtextfile(fieldwcsloc)
		w = WCS(h)
		if s == 0:
			ra, dec = w.all_pix2world(522, 522, 1)
		elif s == 1:
			ra, dec = w.all_pix2world(1566, 522, 1)
		elif s == 2:
			ra, dec = w.all_pix2world(522, 1566, 1)
		elif s == 3:
			ra, dec = w.all_pix2world(1566, 1566, 1)	
		
		racent[i] = ra
		deccent[i] = dec
	

	ra = coord.Angle(racent * u.degree)
	ra = ra.wrap_at(180 * u.degree)
	ra = -ra
	dec = coord.Angle(deccent * u.degree)
	moonra = coord.Angle(moonra * u.degree)
	moonra = moonra.wrap_at(180 * u.degree)
	moonra = -moonra
	moondec = coord.Angle(moondec * u.degree)

	fig = plt.figure(figsize = (10,5))
	ax = fig.add_subplot(111, projection = 'mollweide')
	sc = ax.scatter(ra.radian, dec.radian, c = limmags, s = 5)
	#moon = ax.plot(moonra.radian, moondec.radian, color = 'red', markersize = 20)
	ax.set_xticklabels(np.flip(['14h', '16h', '18h', '20h', '22h', '0h', '2h', '4h', '6h', '8h', '10h'], axis = 0), fontsize = 20)
	ax.set_yticklabels(['-75d', '-60d', '-45d', '-30d', '-15d', '0d', '15d', '30d', '45d', '60d', '75d'], fontsize = 20)
	ax.grid(True)
	cbar = plt.colorbar(sc, fraction = 0.025, pad = 0.04)
	cbar.ax.tick_params(labelsize = 20)
	plt.title('Limiting AB magnitude', fontsize = 20)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'depth_nid%d.pdf'%nightID)
	
	fig = plt.figure(figsize = (10,5))
	ax = fig.add_subplot(111, projection = 'mollweide')
	sc = ax.scatter(ra.radian, dec.radian, c = numvisits, s = 5)
	ax.set_xticklabels(np.flip(['14h', '16h', '18h', '20h', '22h', '0h', '2h', '4h', '6h', '8h', '10h'], axis = 0), fontsize = 20)
	ax.set_yticklabels(['-75d', '-60d', '-45d', '-30d', '-15d', '0d', '15d', '30d', '45d', '60d', '75d'], fontsize = 20)
	ax.grid(True)
	cbar = plt.colorbar(sc, fraction = 0.025, pad = 0.04)
	cbar.ax.tick_params(labelsize = 20)
	plt.title('Number of visits', fontsize = 20)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'visits_nid%d.pdf'%nightID)
	
	fig = plt.figure(figsize = (10,5))
	ax = fig.add_subplot(111, projection = 'mollweide')
	sc = ax.scatter(ra.radian, dec.radian, c = zprms, s = 5)
	ax.set_xticklabels(np.flip(['14h', '16h', '18h', '20h', '22h', '0h', '2h', '4h', '6h', '8h', '10h'], axis = 0), fontsize = 20)
	ax.set_yticklabels(['-75d', '-60d', '-45d', '-30d', '-15d', '0d', '15d', '30d', '45d', '60d', '75d'], fontsize = 20)
	ax.grid(True)
	cbar = plt.colorbar(sc, fraction = 0.025, pad = 0.04)
	cbar.ax.tick_params(labelsize = 20)
	plt.title('Photometric solution scatter', fontsize = 20)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'zprms_nid%d.pdf'%nightID)
	
	#Get the number of visits in the last two nights to create a 2 day coverage map
	query = 'select field, quadpos, subquadpos, avg(limmagpsf) as limmagpsf, avg(zp_psf_rms) as zp_rms, count(*) as count from %s ss inner join %s sp on sp.stackquadid = ss.stackquadid where (nightid = %d or nightid = %d) and limmagpsf != -1 group by field, quadpos, subquadpos;'%(stackScienceQuadTable,stackScienceQuadPhotoTable, nightID, nightID-1)

	try:
		cursor.execute(query)
		out_tn = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
		
	limmags = [o['limmagpsf'] + vegatoab for o in out_tn]
	zprms = [o['zp_rms'] for o in out_tn]
	numvisits = [o['count'] for o in out_tn]
	fields = [o['field'] for o in out_tn]
	quadpos = [o['quadpos'] for o in out_tn]
	subquadpos = [o['subquadpos'] for o in out_tn]
	racent = np.zeros(len(fields))
	deccent = np.zeros(len(fields))

	for i in range(len(fields)):
		f = fields[i]
		q = quadpos[i]
		s = subquadpos[i]
	
		fieldwcsloc = fieldstack_wcsLoc + 'field%d_q%d.wcs'%(f, q)
		h = fits.Header.fromtextfile(fieldwcsloc)
		w = WCS(h)
		if s == 0:
			ra, dec = w.all_pix2world(522, 522, 1)
		elif s == 1:
			ra, dec = w.all_pix2world(1566, 522, 1)
		elif s == 2:
			ra, dec = w.all_pix2world(522, 1566, 1)
		elif s == 3:
			ra, dec = w.all_pix2world(1566, 1566, 1)	
		
		racent[i] = ra
		deccent[i] = dec
	

	ra = coord.Angle(racent * u.degree)
	ra = ra.wrap_at(180 * u.degree)
	ra = -ra
	dec = coord.Angle(deccent * u.degree)
	
	fig = plt.figure(figsize = (10,5))
	ax = fig.add_subplot(111, projection = 'mollweide')
	sc = ax.scatter(ra.radian, dec.radian, c = numvisits, cmap=plt.cm.autumn, s = 5)
	ax.set_xticklabels(np.flip(['14h', '16h', '18h', '20h', '22h', '0h', '2h', '4h', '6h', '8h', '10h'], axis = 0), fontsize = 20)
	ax.set_yticklabels(['-75d', '-60d', '-45d', '-30d', '-15d', '0d', '15d', '30d', '45d', '60d', '75d'], fontsize = 20)
	ax.grid(True)
	cbar = plt.colorbar(sc, fraction = 0.025, pad = 0.04)
	cbar.ax.tick_params(labelsize = 20)
	plt.title('Number of visits in last 2 nights', fontsize = 20)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'visits_twonights_nid%d.pdf'%nightID)
	
	
	#Create a map of time since last observed for all fields observed in the last 7 days 
	query = 'select field, quadpos, subquadpos, max(nightid) as lastnight from %s ss inner join %s sp on sp.stackquadid = ss.stackquadid where nightid > %d and nightid <= %d and limmagpsf != -1 group by field, quadpos, subquadpos order by lastnight desc;'%(stackScienceQuadTable,stackScienceQuadPhotoTable, nightID - 7, nightID)
	
	try:
		cursor.execute(query)
		out_lv = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
		
	fields = [o['field'] for o in out_lv]
	quadpos = [o['quadpos'] for o in out_lv]
	subquadpos = [o['subquadpos'] for o in out_lv]
	lastnight = [nightID - o['lastnight'] for o in out_lv]
	
	racent = np.zeros(len(fields))
	deccent = np.zeros(len(fields))

	for i in range(len(fields)):
		f = fields[i]
		q = quadpos[i]
		s = subquadpos[i]
	
		fieldwcsloc = fieldstack_wcsLoc + 'field%d_q%d.wcs'%(f, q)
		h = fits.Header.fromtextfile(fieldwcsloc)
		w = WCS(h)
		if s == 0:
			ra, dec = w.all_pix2world(522, 522, 1)
		elif s == 1:
			ra, dec = w.all_pix2world(1566, 522, 1)
		elif s == 2:
			ra, dec = w.all_pix2world(522, 1566, 1)
		elif s == 3:
			ra, dec = w.all_pix2world(1566, 1566, 1)	
		
		racent[i] = ra
		deccent[i] = dec
	

	ra = coord.Angle(racent * u.degree)
	ra = ra.wrap_at(180 * u.degree)
	ra = -ra
	dec = coord.Angle(deccent * u.degree)
	
	fig = plt.figure(figsize = (10,5))
	ax = fig.add_subplot(111, projection = 'mollweide')
	sc = ax.scatter(ra.radian, dec.radian, c = lastnight, cmap=plt.cm.rainbow, s = 5)
	ax.set_xticklabels(np.flip(['14h', '16h', '18h', '20h', '22h', '0h', '2h', '4h', '6h', '8h', '10h'], axis = 0), fontsize = 20)
	ax.set_yticklabels(['-75d', '-60d', '-45d', '-30d', '-15d', '0d', '15d', '30d', '45d', '60d', '75d'], fontsize = 20)
	ax.grid(True)
	cbar = plt.colorbar(sc, fraction = 0.025, pad = 0.04)
	cbar.ax.tick_params(labelsize = 20)
	plt.title('Number of nights since the last visit', fontsize = 20)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'visits_lastvisit_nid%d.pdf'%nightID)
	
	
	##CREATE A FOCAL PLANE PHOTOMETRY MAP
	
	query = "SELECT * from %s sp inner join %s ss on sp.stackquadid = ss.stackquadid where nightid = %d and mside = 'East' and limmagpsf!= -1"%(stackScienceQuadPhotoTable, stackScienceQuadTable, nightID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	fwhm_matrix = np.zeros((num_quads, num_sub_quads))
	ellip_matrix = np.zeros((num_quads, num_sub_quads))	
	limmag_matrix = np.zeros((num_quads, num_sub_quads))	
	zp_matrix = np.zeros((num_quads, num_sub_quads))	
	
	for i in range(num_quads):
		for j in range(num_sub_quads):
			q = i
			sq = j
			
			fwhms = np.array([o['psffwhm_mean'] * 4.3 for o in out if o['quadpos'] == q and o['subquadpos'] == sq])
			ellips = np.array([o['ellip_mean'] for o in out if o['quadpos'] == q and o['subquadpos'] == sq])
			limmags = np.array([o['limmagpsf'] + vegatoab for o in out if o['quadpos'] == q and o['subquadpos'] == sq])			
			zp_psfs = np.array([o['zp_psf'] for o in out if o['quadpos'] == q and o['subquadpos'] == sq])
			
			fwhm_median = np.median(fwhms)
			ellip_median = np.median(ellips)
			limmag_median = np.median(limmags)
			zp_median = np.median(zp_psfs)
			
			fwhm_matrix[i][j] = fwhm_median
			ellip_matrix[i][j] = ellip_median
			limmag_matrix[i][j] = limmag_median
			zp_matrix[i][j] = zp_median
	
	
	fig, axs = plt.subplots(2,2)
	ax_fwhm = axs[0,0]
	ax_ellip = axs[0,1]
	ax_limmag = axs[1,0]
	ax_zp = axs[1,1]
	
	cmap = plt.cm.autumn
	cmap_r = plt.cm.autumn
	norm_fwhm = matplotlib.colors.Normalize(vmin = np.min(fwhm_matrix), vmax = np.max(fwhm_matrix))
	norm_ellip = matplotlib.colors.Normalize(vmin = np.min(ellip_matrix), vmax = np.max(ellip_matrix))	
	norm_limmag = matplotlib.colors.Normalize(vmin = np.min(limmag_matrix), vmax = np.max(limmag_matrix))	
	norm_zp = matplotlib.colors.Normalize(vmin = np.min(zp_matrix), vmax = np.max(zp_matrix))	
	cmap_fwhm = cmap(norm_fwhm(fwhm_matrix))
	cmap_ellip = cmap(norm_ellip(ellip_matrix))	
	cmap_limmag = cmap_r(norm_limmag(limmag_matrix))	
	cmap_zp = cmap_r(norm_zp(zp_matrix))
	
	for i in range(num_quads):
		for j in range(num_sub_quads):
			q = i
			sq = j
			xlow, ylow = boxcoord(q, sq)
			
			rect_fwhm = patches.Rectangle((xlow, ylow), 0.25, 0.25, color = cmap_fwhm[i][j])
			rect_ellip = patches.Rectangle((xlow, ylow), 0.25, 0.25, color = cmap_ellip[i][j])
			rect_limmag = patches.Rectangle((xlow, ylow), 0.25, 0.25, color = cmap_limmag[i][j])
			rect_zp = patches.Rectangle((xlow, ylow), 0.25, 0.25, color = cmap_zp[i][j])
			
			ax_fwhm.add_patch(rect_fwhm)
			ax_ellip.add_patch(rect_ellip)
			ax_limmag.add_patch(rect_limmag)
			ax_zp.add_patch(rect_zp)
			
			
	sm_fwhm = plt.cm.ScalarMappable(cmap = cmap, norm = norm_fwhm)
	sm_fwhm.set_array([])
	sm_ellip = plt.cm.ScalarMappable(cmap = cmap, norm = norm_ellip)
	sm_ellip.set_array([])
	sm_limmag = plt.cm.ScalarMappable(cmap = cmap_r, norm = norm_limmag)		
	sm_limmag.set_array([])
	sm_zp = plt.cm.ScalarMappable(cmap = cmap_r, norm = norm_zp)
	sm_zp.set_array([])
	
	c = fig.colorbar(sm_fwhm, ax = ax_fwhm)
	c.ax.tick_params(labelsize = 10)
	c = fig.colorbar(sm_ellip, ax = ax_ellip)
	c.ax.tick_params(labelsize = 10)	
	c = fig.colorbar(sm_limmag, ax = ax_limmag)
	c.ax.tick_params(labelsize = 10)	
	c = fig.colorbar(sm_zp, ax = ax_zp)
	c.ax.tick_params(labelsize = 10)	
	
	ax_fwhm.set_title('PSF FWHM (arcsec)', fontsize = 13)		
	ax_ellip.set_title('PSF Ellipticity', fontsize = 13)			
	ax_limmag.set_title('Limiting magnitude (AB mag)', fontsize = 13)
	ax_zp.set_title('Zero-point (Vega mag)', fontsize = 13)			
			
	ax_fwhm.set_xlim(0, 1)
	ax_fwhm.set_ylim(0, 1)	
	ax_fwhm.set_aspect('equal')
	ax_fwhm.set_xticks([])
	ax_fwhm.set_yticks([])
	ax_ellip.set_xlim(0, 1)	
	ax_ellip.set_ylim(0, 1)
	ax_ellip.set_aspect('equal')	
	ax_ellip.set_xticks([])
	ax_ellip.set_yticks([])
	ax_limmag.set_xlim(0, 1)	
	ax_limmag.set_ylim(0, 1)
	ax_limmag.set_aspect('equal')
	ax_limmag.set_xticks([])
	ax_limmag.set_yticks([])
	ax_zp.set_xlim(0, 1)	
	ax_zp.set_ylim(0, 1)
	ax_zp.set_aspect('equal')
	ax_zp.set_xticks([])
	ax_zp.set_yticks([])
	
	fig.tight_layout()
	fig.savefig(nightReportFolder + 'photo_foc_nid%d.pdf'%nightID)
	
	return photstats
	
	

def getAstrometryStats(cursor, nightID, plot = True):
	query = 'select count(*), avg(astromsigma_reference1) avgsig1, avg(astromsigma_reference2) avgsig2, max(astromsigma_reference1) maxsig1, max(astromsigma_reference2) maxsig2, min(astromsigma_reference1) minsig1, min(astromsigma_reference2) minsig2 from %s qa, %s p where qa.quadid=p.quadid and p.nightID = %d and astromsigma_reference1 != 0 and astromsigma_reference1 != -99'%(astQuadTable, quadTable, nightID)

	try:
		cursor.execute(query)
		out = cursor.fetchone()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	ast_stats = {"count": out['count'], "avgsig1": out['avgsig1'], "avgsig2": out['avgsig2'], "maxsig1": out['maxsig1'], "maxsig2": out['maxsig2'], "minsig1": out['minsig1'], "minsig2": out['minsig2']}
	
	query = 'SELECT * from %s pq inner join %s qa on qa.quadid = pq.quadid where qa.astromsigma_reference1 != -99 and astromsigma_reference1 != 0 and nightid = %d'%(quadTable, astQuadTable, nightID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None
	
	if len(out) == 0:
		return ast_stats
	
	astrms_axis1 = np.array([o['astromsigma_reference1'] for o in out])
	astrms_axis2 = np.array([o['astromsigma_reference2'] for o in out])
	
	if not plot:
		return ast_stats
		
	plt.figure(figsize = (8,8))
	n_1, x_1, _ = plt.hist(astrms_axis1, color = 'steelblue', alpha = 0.5, density = True, label = 'Axis 1', range = (0, 2), bins = 15)
	n_2, x_2, _ = plt.hist(astrms_axis2, color = 'forestgreen', alpha = 0.5, density = True, label = 'Axis 2', range = (0, 2), bins = 15)
	
	plt.xlabel('Astrometric RMS (arcsec)', fontsize = 30)
	plt.ylabel('Density', fontsize = 30)
	
	plt.xticks(fontsize = 25)
	plt.yticks(fontsize = 25)
	plt.legend(fontsize = 30)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'AstrometricRMS_nid%d.pdf'%nightID)
	
	
	query = 'select * from %s sp inner join %s ss on ss.stackquadid = sp.stackquadid where nightid = %d and sp.limmagpsf != -1'%(stackScienceQuadPhotoTable, stackScienceQuadTable, nightID)
	
	try:
		cursor.execute(query)
		out = cursor.fetchall()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	
	astradmed = np.array([o['d2d_med'] for o in out])
	plt.figure(figsize = (8,8))
	n_med, x_med, _ = plt.hist(astradmed, color = 'steelblue', alpha = 0.9, density = True, label = 'Separation', range = (0, 1.3), bins = 25)
	
	rad_med = np.percentile(astradmed, 50)
	rad_95p = np.percentile(astradmed, 95)
	rad_5p = np.percentile(astradmed, 5)		
	uplim = 5.5
	
	plt.plot(np.linspace(rad_med, rad_med), np.linspace(0, uplim), 'k-')
	plt.plot(np.linspace(rad_95p, rad_95p), np.linspace(0, uplim), 'k--')
	plt.plot(np.linspace(rad_5p, rad_5p), np.linspace(0, uplim), 'k--')
	
	#plt.plot(x_med, astradmed, color = 'steelblue', linewidth = 1)
	plt.xlabel('Median radial separation (arcsec)', fontsize = 30)
	plt.ylabel('Density', fontsize = 30)
	plt.xticks(fontsize = 25)
	plt.yticks(fontsize = 25)
	plt.legend(fontsize = 30)
	plt.xlim(0.42, 1.2)
	plt.ylim(0, uplim)
	plt.tight_layout()
	plt.savefig(nightReportFolder + 'StackRadialMedian_nid%d.pdf'%nightID)

	return ast_stats 

def getHumidityStats(cursor, nightID):
	query = 'select avg(envhum) avgenvhum, max(envhum) maxenvhum, min(envhum) minenvhum from %s where nightid=%d;'%(rawTable, nightID)

	try:
		cursor.execute(query)
		out = cursor.fetchone()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	humiditystats = {"avgenvhum": out['avgenvhum'], "maxenvhum": out['maxenvhum'], "minenvhum": out['minenvhum']}
	
	return humiditystats


def getMoonStats(cursor, nightID):	
	
	query = 'select avg(moondist) avdist, min(moondist) mindist, max(moondist) maxdist, avg(moonphse) avphase from %s where nightid=%d'%(quadTable, nightID)

	try:
		cursor.execute(query)
		out = cursor.fetchone()
	except psycopg2.Error as error:
		logger.warning(error)
		return None

	moonstats = {"moondistavg": out['avdist'], "moondistmin": out['mindist'], "moondistmax": out['maxdist'], "moonphaseavg": out['avphase']}

	return moonstats
	
		
def getProcStackFrames(cursor, datebeg, dateend, startnightid, endnightid):
	
	query = "select count(*) c, min(procquads.nightid) nid, procquads.fieldseq fs, procquads.quadpos qp, min(procquads.filename) qfile, min(stackscience.filename) sfile, min(procquads.field) fid, max(procquads.airmass) airmass, min(procquads.moondist) moondist, max(procquads.envhum) envhum, min(stackscience.datebeg) datebeg from %s p LEFT OUTER JOIN %s ss ON (ss.nightid = p.nightid and ss.fieldseq = p.fieldseq and ss.quadpos = p.quadpos) WHERE p.datebeg > '%s' and p.dateend < '%s' and p.nightid >= %d and p.nightid <= %d group by p.fieldseq, p.quadpos order by p.fieldseq;"%(quadTable, stackScienceTable, datebeg, dateend, startnightid, endnightid)

	try:
		cursor.execute(query)
		out = cursor.fetchall()
		return(out)
	except psycopg2.Error as error:
		logger.warning('CAUGHT PSCYCOPG2 ERROR: %s'%error)
		return None
		
		

def insertSingleTab(cursor, conn, tabName, schema, values, returnID = None):	
	'''
	Function to insert one single row into a database table
	
	Arguments
	----------------
	cursor: psycopg2 cursor object
	To access DB
	
	conn: psycopg2 connection
	To modify DB
	
	tabName: string
	Table name where entry is to be inserted
	
	schema: string
	comma separated list of keywords in the new entry
	
	values: list
	list of values to be inserted
	
	returnID: string (optional)
	keyword to be returned from the latest insert, if needed
	
	Returns
	---------------
	-1 if insert is unsuccessful, 1 if insert was successful and no return keyword was requested
	If a return keyword was requested, the new keyword is returned
	'''
	
	logger = logging.getLogger(__name__)

	numValues = len(values)
	valueString = ",".join(["%s" for x in range(numValues)])
	
	try:		
		if returnID is not None:
			insertString = 'INSERT INTO %s (%s) VALUES (%s) RETURNING %s'%(tabName, schema, valueString, returnID)
			cursor.execute(insertString, values)
			retVal = cursor.fetchone()[0]
		else:
			insertString = 'INSERT INTO %s (%s) VALUES (%s)'%(tabName, schema, valueString)
			cursor.execute(insertString, values)
			
		#print(insertString, valueString, values)
			
	except psycopg2.Error as error:
		logger.warning('CAUGHT PSCYCOPG2 ERROR: %s'%error)
		conn.rollback()
		return -1
	else:
		conn.commit()
		if returnID is not None:
			return retVal
		else:
			return 1;

def updateSingleTab(cursor, conn, tabName, keys, values, condVar, condVal):
	'''
	Function to update the record of a single row in a table
	
	Arguments
	----------------
	cursor: psycopg2 cursor object
	To access DB
	
	conn: psycopg2 connection
	To modify DB
	
	tabName: string
	Table name where entry is to be inserted
	
	keys: list of strings
	columns to be updated
	
	values: list of values
	updated values of columns
	
	condVar: string
	name of comparison variable where update is to be done
	
	condVal: string
	value of comparison variable where update is to be done
	
	Returns 
	----------------
	1 if update is successful, -1 otherwise
	'''


	setString = 'SET '
	for key in keys:
		setString += "%s = %%s, "%key
	setString = setString[:-2]

	updateString = "UPDATE %s "%tabName+setString+" WHERE %s = %s"%(condVar, condVal)
	
	try:
		cursor.execute(updateString, values)
	except psycopg2.Error as error:
		logger.warning('CAUGHT PSCYCOPG2 ERROR: %s'%error)
		conn.rollback()
		return -1
	else:
		conn.commit()
		return 1;	
	
	
def updateMultipleTabs(cursor, conn, tabName, updlist, rowkey, keys):

	numrows = len(updlist)
	
	setString = 'SET '
	matchcolumnstring = '%s, '%rowkey
	for key in keys:
		setString += '%s = c.%s, '%(key, key)
		matchcolumnstring += '%s, '%key
	setString = setString[:-2]
	matchcolumnstring = matchcolumnstring[:-2]
	
	matchtablestring = ''
	for i in range(numrows):
		matchtablestring += '(%d, '%updlist[i][0]
		for j in range(1, len(updlist[i])):
			if isinstance(updlist[i][j], str):
				matchtablestring += "'%s', "%updlist[i][j]
			else:
				matchtablestring += '%s, '%str(updlist[i][j])
		matchtablestring = matchtablestring[:-2] + '), '
	
	matchtablestring = matchtablestring[:-2]
	
	updateString = 'UPDATE %s AS t '%tabName + setString + ' FROM (values %s) '%matchtablestring + 'as c(%s) '%matchcolumnstring + 'WHERE c.%s = t.%s;'%(rowkey, rowkey)
	
	try:
		cursor.execute(updateString)
	except psycopg2.Error as error:
		logger.warning('CAUGHT PSCYCOPG2 ERROR: %s'%error)
		conn.rollback()
		return -1
	else:
		conn.commit()
		return 1;	
	

	
