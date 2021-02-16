import requests 
import json
import argparse
import numpy as np
import re
import io
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
	print('Candid %d got rbscore of %.2f from nightid %d'%(canddict['candid'], rbscore, canddict['nightid']))
	return float(rbscore)
	
	
	
def main(nightid, doast = True, dorb = True, skipdone = False):
	t0 = time.time()
	conn, cur = dbOps.getDBCursor()
	
	if dorb:
		query = "SELECT cand.candid, ra, dec, jd, cand.nightid, cut.* FROM candidates cand inner join cutouts cut on cut.candid = cand.candid WHERE nightid=%d ORDER BY jd asc LIMIT %d;"%(nightid, candlimit)
	else:
		query = "SELECT cand.candid, ra, dec, jd, cand.nightid FROM candidates cand WHERE nightid=%d ORDER BY jd asc LIMIT %d;"%(nightid, candlimit)

	#if skipdone:
	#	print('Skipping done sources..')
	#	query = query.replace('ORDER', 'AND rbscore = -1 ORDER')
	
	print(query)
	cur.execute(query)
	out = cur.fetchall()

	jd_list = np.array([o['jd'] for o in out])
	candid_list = np.array([o['candid'] for o in out])
	ra_list = np.array([float(o['ra']) for o in out])
	dec_list = np.array([float(o['dec']) for o in out])

	print('Found %d candidates to process'%(len(ra_list)))
	if len(ra_list) == 0:
		return 0
	
	minjd = np.min(jd_list)
	
	if doztf or doclu:
		k = getKowalskiCrossmatch(ra_list, dec_list, minjd)
		if k is None:	
			print('Kowlaski crossmatch did not work.. Exiting here .. ')
			return -99
	else:
		print('Skipping Kowalski cross-match because not requested ..')
	
	pool = ThreadPool()
	
	if doztf:
		ztf_process = pool.apply_async(doZTFcrossmatch, args = (k, candid_list, ra_list, dec_list,))
	if doclu:
		clu_process = pool.apply_async(doCLUcrossmatch, args = (k, candid_list, ra_list, dec_list,))
	if doast:
		ast_process = pool.apply_async(doSScrossmatch, args = (nightid, candid_list, jd_list, ra_list, dec_list,))
	
	if doztf:
		ztf_ids = ztf_process.get()
	if doclu:
		clu_dists = clu_process.get()
	if doast:
		ssdists, ssmags, ssnames = ast_process.get()
	
	pool.close()	
	
	num_update = 0
	for i in range(len(ra_list)):
		updkeys = []
		updvals = []
		if doztf:
			updkeys.append('ztf_crossmatch')
			updvals.append(ztf_ids[i])
		if doclu:
			updkeys.append('clu_distance')
			updvals.append(float(clu_dists[i]))
		if doast:
			updkeys.extend(['ssdistnr', 'ssmagnr', 'ssnamenr'])
			updvals.extend([float(ssdists[i]), float(ssmags[i]), ssnames[i]])
		if dorb:
			rbscore = getscore(out[i])
			updkeys.append('rbscore')
			updvals.append(rbscore)	
			updkeys.append('rbver')
			updvals.append(current_model_json)
		dbOps.updateSingleTab(cur, conn, 'candidates', updkeys, updvals, 'candid', candid_list[i])
		num_update += 1

	print('Updated database entries for %d sources'%num_update)
		
	dbOps.closeCursor(conn, cur)
	
	t1 = time.time()
	print('Took %.2f seconds to process cross-matching for %d sources'%(t1 - t0, len(ra_list)))
	return len(ra_list)
	
if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(description = 'Code to cross-match Gattini transients to ZTF alerts and CLU galaxies')
	parser.add_argument('nightid', help = 'Night ID for cross-match')
	parser.add_argument('--skipast', help = 'Add to NOT do MPC cross-match', action = 'store_true', default = False)
	parser.add_argument('--skipdone', help = 'Add to skip sources where cross-match is done', action = 'store_true', default = False)
	parser.add_argument('--skiprb', help = 'Add to NOT do RB', action = 'store_true', default = False)
	args = parser.parse_args()

	doast = True
	dorb = True
	skipdone = False
	nightid = args.nightid
	if args.skipast:
		doast = False
	if args.skiprb:
		dorb = False
	if args.skipdone:
		skipdone = True
	main(int(nightid), doast = doast, dorb = dorb, skipdone = skipdone)
	
