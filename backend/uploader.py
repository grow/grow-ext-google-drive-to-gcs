from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import urlfetch_errors
from google.appengine.ext import deferred
from googleapiclient import discovery
from googleapiclient import errors
from googleapiclient import http
from oauth2client import appengine
from oauth2client import client
import cStringIO
import cloudstorage as gcs
import httplib
import httplib2
import json
import logging
import os
import time

DEFAULT_BUCKET = app_identity.get_default_gcs_bucket_name()
SERVICE_ACCOUNT_EMAIL = app_identity.get_service_account_name()

SCOPE = [
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/userinfo.email',
]

SKIP_MIMETYPES = [
    'application/vnd.google-apps.document',
]

CHUNKSIZE = 10 * 1024 * 1024  # 20 MB per request.
GCS_UPLOAD_CHUNKSIZE = 10 * 1024 * 1024
NUM_RETRIES = 2
BACKOFF = 4  # Seconds.

# KeyError: 'range'
# lib/googleapiclient/http.py", line 902, in _process_response
# self.resumable_progress = int(resp['range'].split('-')[1]) + 1
ERRORS_TO_RETRY = (IOError, httplib2.HttpLib2Error, urlfetch_errors.DeadlineExceededError, KeyError)

# Silence extra logging from googleapiclient.
discovery.logger.setLevel(logging.WARNING)


class Error(Exception):
    pass


class UploadRequiredError(Error):
    pass


service = None
drive3_service = None
storage_service = None


def get_service():
    global service
    if service is None:
        service_http = httplib2.Http()
        credentials = appengine.AppAssertionCredentials(SCOPE)
        credentials.authorize(service_http)
        service = discovery.build('drive', 'v2', http=service_http)
    return service


def get_drive3_service():
    global drive3_service
    if drive3_service is None:
        http = httplib2.Http()
        credentials = appengine.AppAssertionCredentials(SCOPE)
        credentials.authorize(http)
        drive3_service = discovery.build('drive', 'v3', http=http)
    return drive3_service


def get_storage_service():
    global storage_service
    if storage_service is None:
        http = httplib2.Http()
        credentials = appengine.AppAssertionCredentials(SCOPE)
        credentials.authorize(http)
        storage_service = discovery.build('storage', 'v1', http=http)
    return storage_service


def download_folder(resource_id, process_deletes=True):
    service = get_service()
    page_token = None
    child_resource_responses = []
    while True:
        params = {}
        if page_token:
            params['pageToken'] = page_token
        children = service.children().list(
            folderId=resource_id, **params).execute()
        for child in children.get('items', []):
            child_resource_responses.append(child)
        page_token = children.get('nextPageToken')
        if not page_token:
            break
    return child_resource_responses


def download_resource(resource_id, gcs_path_format=None, queue='sync'):
    service = get_service()
    resp = service.files().get(fileId=resource_id).execute()
    if 'mimeType' not in resp:
        logging.error('Received {}'.format(resp))
        return
    title = resp['title']
    title = title.encode('utf-8') if isinstance(title, unicode) else title
    mime_type = resp.get('mimeType', '')
    if mime_type == 'application/vnd.google-apps.folder':
        logging.info('Processing folder: {} ({})'.format(title, resource_id))
        return process_folder_response(resp, gcs_path_format=gcs_path_format, queue=queue)
    elif mime_type in SKIP_MIMETYPES:
        logging.info('Skipping file due to incompatible mimetype: {} ({})'.format(title, mime_type))
    else:
        logging.info('Processing file: {} ({})'.format(title, resource_id))
        # NOTE: Can use deferred here instead.
        # return deferred.defer(replicate_asset_to_gcs, resp, gcs_path_format)
        return replicate_asset_to_gcs(resp, gcs_path_format=gcs_path_format)


def process_folder_response(resp, gcs_path_format, queue='sync'):
    resource_id = resp['id']
    folder_title = resp['title']
    gcs_path_format = os.path.join(gcs_path_format, folder_title)
    logging.info('Using folder -> {}'.format(gcs_path_format))
    child_resource_responses = download_folder(resp['id'])
    uploaded_paths = []
    for child in child_resource_responses:
        path = download_resource(child['id'], gcs_path_format=gcs_path_format)
        uploaded_paths.append(path)
    return uploaded_paths


def replicate_asset_to_gcs(resp, gcs_path_format):
    bucket_path = os.path.join(gcs_path_format, resp['title'])  # /foo/bar/baz/file.png
    path = '/'.join(bucket_path.lstrip('/').split('/')[1:])  # bar/baz
    bucket = bucket_path.lstrip('/').split('/')[0]  # foo
    if True or not appengine_config.DEV_SERVER:
        try:
            stat = gcs.stat(bucket_path)
            if stat.etag != resp['etag']:
                raise UploadRequiredError()
            raise UploadRequiredError()
        except (gcs.NotFoundError, UploadRequiredError):
            # TODO - Verify if asset needs to be replicated.
            fp = download_asset_in_parts(resp['id'])
            write_gcs_file(path, bucket, fp, resp['mimeType'])
    return bucket_path


def get_file_content(resp):
    service = get_service()
    for mimetype, url in resp['exportLinks'].iteritems():
        if mimetype.endswith('html'):
            resp, content = service._http.request(url)
            if resp.status != 200:
                raise Exception()
            return content


def download_asset_in_parts(file_id):
    drive3 = get_drive3_service()
    fp = cStringIO.StringIO()
    request = drive3.files().get_media(fileId=file_id)
    req = http.MediaIoBaseDownload(fp, request, chunksize=CHUNKSIZE)
    done = False
    connections = 0
    while done is False:
        try:
            status, done = req.next_chunk()
            backoff = BACKOFF
            connections += 1
            continue
        except ERRORS_TO_RETRY as e:
            logging.warn('Drive download encountered error to retry: %s' % e)
        retries += 1
        if retries > NUM_RETRIES:
            raise ValueError('Hit max retry attempts with error: %s' % e)
        time.sleep(backoff)
        backoff *= 2
    fp.seek(0)
    logging.info('Downloaded in %s tries: %s', connections, file_id)
    return fp


def write_gcs_file(path, bucket, fp, mimetype):
    storage_service = get_storage_service()
    media = http.MediaIoBaseUpload(
        fp, mimetype=mimetype, chunksize=GCS_UPLOAD_CHUNKSIZE, resumable=True)
    req = storage_service.objects().insert(
        media_body=media, name=path, bucket=bucket)
    resp = None
    backoff = BACKOFF
    retries = 0
    connections = 0
    while resp is None:
        try:
            _, resp = req.next_chunk()
            backoff = BACKOFF  # Reset backoff.
            connections += 1
            continue
        except ERRORS_TO_RETRY as e:
            logging.warn('GCS upload encountered error to retry: %s' % e)
        retries += 1
        if retries > NUM_RETRIES:
            raise ValueError('Hit max retry attempts with error: %s' % e)
        time.sleep(backoff)
        backoff *= 2
    logging.info('Uploaded in %s tries: %s' % (connections, path))
