#Import a bunch of necessary libraries
import psycopg2
import psycopg2.extras
import sys
import warnings
import logging
import itertools
warnings.simplefilter("ignore")
import argparse
from datetime import datetime
from datetime import timedelta
from astropy.time import Time
from astropy.table import Table
from astropy.io.votable import parse
import astropy.coordinates as coord
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
palomar = EarthLocation.of_site('Palomar')
from astropy.wcs import WCS
import astropy.units as u
from astropy.stats import sigma_clipped_stats, sigma_clip, median_absolute_deviation
import matplotlib
import matplotlib.pyplot as plt
mpl_logger = logging.getLogger('matplotlib')
mpl_logger.setLevel(logging.WARNING)
from matplotlib import rc
plt.rc('text', usetex=True)
plt.rc('font',family='sans-serif')
import matplotlib.patches as patches
import matplotlib.colors
from matplotlib.backends.backend_pdf import PdfPages
import os
import os.path
import shutil
import glob
import time
import numpy as np
import numpy.ma as ma
import pandas as pd
import requests
import getpass
import socket
import subprocess
import drizzle.drizzle
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from astropy.io import fits, ascii
import io
from io import BytesIO
import fastavro
import json
import confluent_kafka
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

hostname = socket.gethostname()
#read credentials
userdata = ascii.read('config/gdb.config', format = 'no_header')
username = userdata['col1'][0]
password = userdata['col1'][1]
gattinibot_login = "dbname=gattini user=%s password=%s"%(username, password)

if 'gattinidrp' in hostname:
	#how to login into the database
	gattinibot_login += " host=localhost"
else:
	#how to login into the database
	gattinibot_login += " host=gattinidrp"

#Folder for calibration files
calFlatFolder = '/scr3/kde/survey/cals/flats/'
calDarkFolder = '/scr3/kde/survey/cals/darks/'
calHPMFolder = '/scr3/kde/survey/cals/hpms/'
calDPMFolder = '/scr3/kde/survey/cals/dpms/'
masterBPM_name = 'masterBPM.fits'
#File storing the log from gattini during nightly operations
outputLog = '/scr2/kde/pipeline/survey/operator/logs/gattiniRobot.log'

osig_up = 84.13
osig_low = 15.86
#Dimension of the raw files
rawFileDim = [2048, 2048]
#Start night data processing at this hour
startNightProcHour = 17.0
#Finish night data processing at this hour -- begin end of night tasks at this hour
startEndNightHour = 8.0
#Do not start end of night tasks after this hour
finishEndNightHour = 17.0
#Reference date for the DRP to calculate Night ID from
drpRefDate = datetime(2018,10,1,17,0,0)

#Save the night logs everyday to this folder
nightLogFolder = '/scr3/kde/survey/nightlogs/'
#Save the night reports each night to this folder
nightReportFolder = '/scr3/kde/survey/nightreports/'
#Subtraction reports are stored at this location
subtraction_report_folder = '/scr3/mhankins/ImgSub/logfiles/'

pgiremaildata = ascii.read('config/gattiniir.email.config', format = 'no_header')
#Email address that sends the night report
nightReportSender = pgiremaildata['col1'][0]
#Password for night report sender
nightReportPassword = pgiremaildata['col1'][1]
#nightReportRecipients = ['kde@astro.caltech.edu','dekishalay@gmail.com']
#Night report is sent to these addresses
pgirmaillist = ascii.read('config/pgirmail.rec.list', format = 'no_header')
nightReportRecipients = np.array(pgirmaillist['col1'])

#Location of incoming files from Palomar
inboxArea = '/scr3/phase2operator/survey/raw/'
#Location of storing status files for ingested data
stagingArea = '/scr3/kde/survey/raw/'
#Location of stooring pre-processed data
reduxArea = '/scr3/kde/survey/redux/'
#Location of storing stacked data
stackArea = '/scr3/kde/survey/stacks/'
#Location of storing references
referenceArea = '/scr2/kde/survey/references/'
#The stauration level in the detectors
saturLevel = 30000
#The known gain of the detector
detector_gain = 4.54 # in e-/ADU
#drizzle saturate change because of splitting of individual pixels
driz_satur_corr = 4.0

#Wait time for DRP before looking for new files
realtimeWait = 120
#Distortion keywords to be removed from pre-existing astrometry solutions since python does not understand them
rawDistortKeywords = ['LTM1_1', 'LTM2_2', 'WAT0_001', 'WAT1_001', 'WAT1_002', 'WAT1_003', 'WAT1_004', 'WAT1_005', 'WAT2_001', 'WAT2_002', 'WAT2_003', 'WAT2_004', 'WAT2_005']
#Number of quadrants to be used
num_quads = 4
num_sub_quads = 4
#Quadrant ID numbers
quad_ids = range(num_quads)
#Pixel coordinates of quadrant center for assigning WCS coordinates to split processed files
quadWCSPixCenters_east = [[512, 512], [1536, 512], [512, 1536], [1536, 1536]] 	#reflected with respect to the data quadrant definition
quadWCSPixCenters_west = [[1536, 1536], [512, 1536], [1536, 512], [512, 512]]
padding_size = 10 #Padding of each quadrant in pixels

#SExtractor config for extracting souces for astrometry
astrometry_sexConfig = '/data/kde/pipeline/survey/astrometry/astromCat.sex'
#Scamp configuration for performing astrometry
astrometry_scampConfig = '/data/kde/pipeline/survey/astrometry/astromScamp.conf'
#Folder where the reference catalog is stored for scamp astrometry
scamp_catFolder = '/scr2/kde/catalogs/gaiadr2_2mass_neighbor_12arcsec/'
#Master name for file lists to be fed to scamp
scampMasterName = 'scampList'
scamp_timeout = 90 # in seconds
#folder where the drizzle grid for each field and quadrant is stored
fieldstack_wcsLoc = '/data/kde/pipeline/survey/astrometry/fieldWCS/'
#Database table recording the output wcsgrid
wcsTable = 'wcsgrid'
#default number of dithers in the survey
num_def_dithers = 8.0

#Number of processes to run for drizzling and photometric solutions
numQuadStackFieldSeqProcesses = 22
min_stack_processes = 4
#Dimensions of drizzled quadrant images
stackQuadDrizzleDim = [2088, 2088]
#Number of extensions in drizzled images
numStackExtensions = 3
#Pixel shrinking factor in drizzle
drizzlePixFrac = 0.9
#Minimum number of files for a reference to be called complete
numReferenceFiles = 200
#Minimum depth of a reference image to be called complete
#psf_limmagref = 16.5
#Number of processes running while creating references
numReferenceProcesses = 12

#Folder with simulated noise drizzled images for correlated noise correction
driz_sim_noise_folder = '/data/kde/survey/correlated_noise/template_images/'
num_noise_driz_apertures = 100
#SExtractor config for extracting souces for photometry
photometry_sexConfig = '/data/kde/pipeline/survey/astrometry/photomCat.sex'
#PSFex config for computing PSF model for images
photometry_psfConfig = '/data/kde/pipeline/survey/astrometry/photom.psfex'
#SExtractor parameter file for feeding to PSFEx
photometry_psfParam = '/data/kde/pipeline/survey/astrometry/photomPSF.param'
#Minimum SNR for photometry
photoSNR_thresh = 10.0
brightstarmag = 12.0
photoSNRLimLow = 5.0
photoSNRLimHigh = 5.4
photoFaintNumSources = 5
photLimSigma = 5.0
#Cross-match radius for photometry
photoDistThresh = 1.6 #in arcsec
#Pixel limits on sub-quadrants for sources to be used for photometric solutions
#Excluding 40 pixels from the edges of the image
photImageLims = [[40, 980], [40, 980]]
vegatoab = 0.9

#Below are the image selection criteria for reference images
#Minimum PSF limiting magnitude
ref_limpsfmag = 13
#Maximum scatter in PSF photometric solution
ref_limzpstd = 0.1
#Minimum number of images required for references
ref_minimages = 16

#Parameters for creating dead pixel masks
dpmlow_field = -4
#Count limits for low flats
dpm_low_lc = 6000
dpm_low_hc = 6500
dpmhigh_field = -8
#count limits for high flats
dpm_high_lc = 14000
dpm_high_hc = 15000
#clipping threshold for dead pixel masks
dpm_nthreshsigma = 4.0
#minimum number of files to create a mask
dpm_numfiles = 100
dpm_datelim = 90.0

#Below are the image selection criteria for constructing flats
#Recommended number of files
flat_numFileRec = 200
#Minimum number of files required
flat_numFileMin = 100
#Create flats using images taken within the last * days
flatFieldDateLim = 1.0 #in days
#Minimum moon distance for flat images
flat_moonDistLim = 0.7 #in radians
#Maximum humidity for flat images
flat_humLim = 30 # in percentage
#Maximum median counts for flat images
flat_countLim = 15000 #in detector counts

crosstalk_corr_script = '/data/kde/pipeline/survey/crosstalk/ct'

#arcserv versions in use
program_list = ['arcserv-20181018', 'arcserv May 18 2020 02:55:47', 'arcserv May 21 2020 20:40:17', 'arcserv Jul 27 2020 14:37:59', 'arcserv Jul 30 2020 16:45:12', 'arcserv Aug 20 2020 15:07:16', 'arcserv Aug 22 2020 17:30:18']

#Scanning rota
#scanners = ['Kishalay D.', 'Anna M.', 'Matt H.', 'Jacob J.', 'Roberto S.', 'Tony T.', 'Jamie S.']
