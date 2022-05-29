#!/usr/bin/env python
import pytz
import argparse
import os
import shutil
import logging
#import logging.config
from datetime import datetime
from parse import parse
from google.cloud import storage
from google.cloud import pubsub
# import google.cloud.logging
from google.cloud.exceptions import NotFound, Conflict
# from concurrent.futures import ThreadPoolExecutor
import pysftp

#loading dotenv  not necessry for the cloud function
from dotenv import load_dotenv
load_dotenv()
### End load_dotenv

this_dir, _ = os.path.split(__file__)

path = os.path.join(this_dir, 'logging.conf')

#logging.config.fileConfig(path, disable_existing_loggers=False)
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

def force_utc(d_object):
    if hasattr(d_object, 'tzinfo') and d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    return d_object

def from_tz(d_object, tz):
  t = pytz.timezone(tz).localize(d_object) 
  return t.astimezone(pytz.UTC)

def sftp_list_dir(sftp, path=None, tz='UTC'):
  if not path:
    path = '/'
  files = []
  with sftp.cd(path):
    for file in sftp.listdir_attr():
      files.append({
        'file': file.filename,
        'modified': from_tz(datetime.fromtimestamp(file.st_mtime), tz)
      })
  return files

def gcs_list_dir(bucket, path=None):
  files = []
  for blob in bucket.list_blobs(prefix=path):
    # exclude directories
    modified = None
    if blob.metadata:
      try:
        modified = force_utc(datetime.fromtimestamp(blob.metadata.get('lastmodified')))
      except:
        pass      
    if not modified:
      modified = force_utc(blob.updated)
    if blob.name[-1]!='/':
      files.append({
        'file': blob.name,
        'modified': modified,
        '_blob': blob
      })
  return files


#Probably want to refactor this into separate functions. Simplest would be GCP to FTP and another for FTP to GCS, pass in all of the parameters you need from the sync function 
def sync(data, context):
  
  LOGGER.info(f"Running: {os.environ['MODE']}")
  #gcs_config = job.get('gcs')
  bucket = os.environ['BUCKET']

  #ftp_config = job.get('ftp')

  # RSA hack
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None

  sftp = pysftp.Connection(
    host=os.environ['HOSTNAME']
    ,port=int(os.environ['PORT'])
    ,username=os.environ['USERNAME']
    ,password=os.environ['PASSWORD']
    ,cnopts=cnopts
  )
  
  mode = os.environ['MODE']

  bucket_path = ''
  if os.environ['GCS_PATH']:
    bucket_path = os.environ['GCS_PATH'] + '/'    

# Section 1 to break into new function
  if mode in ("gcs<ftp", "gcs<>ftp"):
    bucket_files = gcs_list_dir(bucket, os.environ['GCS_PATH'])
    ftp_files = sftp_list_dir(sftp, os.environ['FTP_PATH'], os.environ['TIMEZONE'])
    # FTP to GCS
    if not os.path.exists('tmp'):
      os.mkdir('tmp')
    LOGGER.info("GCS<FTP")
    download_files = []
    for ftp_file in ftp_files:
      ftp_filename = ftp_file.get('file')
      exists = False
      for gcs_file in bucket_files:
        gcs_filename = os.path.basename(gcs_file['file'])
        if gcs_filename == ftp_filename:
          exists = True
          if gcs_file.get('modified', datetime.min) < ftp_file.get('modified', datetime.max):
            download_files.append(ftp_filename)
      if exists == False:
        LOGGER.info(f"GCS<FTP: {ftp_filename}")
        download_files.append(ftp_filename)
    
    for filename in download_files:
      with sftp.cd(os.environ['FTP_PATH']+'/'):
        sftp.get(filename, localpath=os.path.join('tmp', filename), preserve_mtime=True)
      blob = bucket.blob(bucket_path + filename)
      t = os.path.getmtime(os.path.join('tmp', filename))
      LOGGER.info(str(int(t)))
      blob.metadata = {'lastmodified': str(int(t))}
      blob.upload_from_filename(os.path.join('tmp', filename))
    
    if os.path.exists('tmp'):
      shutil.rmtree(os.path.join(this_dir, 'tmp'))

#Section 2 to break into new function
#The way you download a file from GCS is different from the way you download a file from FTP
  if mode in ("gcs>ftp", "gcs<>ftp"):
    bucket_files = gcs_list_dir(bucket, os.environ['GCS_PATH'])
    ftp_files = sftp_list_dir(sftp, os.environ['FTP_PATH'], os.environ['TIMEZONE'])
    # GCS to FTP
    if not os.path.exists('tmp'):
      os.mkdir('tmp')
    LOGGER.info("GCS>FTP")
    download_files = []
    for gcs_file in bucket_files:
      gcs_filename = os.path.basename(gcs_file['file'])
      exists = False
      for ftp_file in ftp_files:
        ftp_filename = ftp_file.get('file')
        if gcs_filename == ftp_filename:
          exists = True
          if gcs_file.get('modified', datetime.min) > ftp_file.get('modified', datetime.max):
            download_files.append(gcs_file)
      if exists == False:
        LOGGER.info(f"GCS>FTP: {gcs_filename}")
        download_files.append(gcs_file)
      
    for file in download_files:
      filename = os.path.basename(file['file'])
      with open(os.path.join('tmp', filename), "wb") as file_obj:
        file['_blob'].download_to_file(file_obj)
      sftp.put(
        os.path.join('tmp', filename)
        ,remotepath=os.path.join(os.environ['FTP_PATH']+'/', filename)
        ,preserve_mtime=True
      )

    if os.path.exists('tmp'):
      shutil.rmtree(os.path.join(this_dir, 'tmp'))