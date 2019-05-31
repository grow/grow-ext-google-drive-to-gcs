from google.appengine.api import app_identity
from google.appengine.ext import blobstore
from google.appengine.api import images
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import urlfetch_errors
from google.appengine.ext import deferred
from google.appengine.ext import ndb
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


class Tag(ndb.Model):
    num_assets = ndb.IntegerProperty()
    modified = ndb.DateTimeProperty(auto_now=True)


class Asset(ndb.Model):
    basename = ndb.StringProperty()
    bucket_path = ndb.StringProperty()
    drive_id = ndb.StringProperty()
    etag = ndb.StringProperty()
    folder_id = ndb.StringProperty()
    mimetype = ndb.StringProperty()
    modified = ndb.DateTimeProperty(auto_now=True)
    parent_tag = ndb.StringProperty()
    serving_url = ndb.StringProperty()
    status = ndb.StringProperty()
    tag = ndb.StringProperty()
    title = ndb.StringProperty()


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


def create_tag(resource_id, gcs_path_format, tag, parent_tag):
    tasks = download_resource(resource_id, gcs_path_format, tag, parent_tag)
    tag = Tag(key=ndb.Key('Tag', tag))
    tag.num_assets = len(tasks)
    tag.put()


def get_tag(tag):
    return ndb.Key('Tag', tag).get()


def download_resource(resource_id, gcs_path_format=None, tag=None, parent_tag=None):
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
        return process_folder_response(resp, gcs_path_format=gcs_path_format, tag=tag)
    elif mime_type in SKIP_MIMETYPES:
        logging.info('Skipping file due to incompatible mimetype: {} ({})'.format(title, mime_type))
    else:
        logging.info('Processing file: {} ({})'.format(title, resource_id))
        return deferred.defer(replicate_asset_to_gcs, resp, gcs_path_format, tag=tag, parent_tag=parent_tag)
        # NOTE: Can use deferred here instead.
        return replicate_asset_to_gcs(resp, gcs_path_format=gcs_path_format)


def process_folder_response(resp, gcs_path_format, tag=None):
    resource_id = resp['id']
    folder_title = resp['title']
    gcs_path_format = os.path.join(gcs_path_format, folder_title)
    logging.info('Using folder -> {}'.format(gcs_path_format))
    child_resource_responses = download_folder(resp['id'])
    uploaded_paths = []
    for child in child_resource_responses:
        path = download_resource(child['id'], gcs_path_format=gcs_path_format, tag=tag)
        uploaded_paths.append(path)
    return uploaded_paths


def get_parent_tag(folder_id):
    query = Asset.query()
    query = query.filter(Asset.folder_id == folder_id)
    query = query.order(-Asset.modified)
    assets = query.fetch(limit=1)
    return assets[0].tag if assets else None


def replicate_asset_to_gcs(resp, gcs_path_format, tag=None, parent_tag=None, upload_to_cloud_images=True):
    drive_id = resp['id']
    etag = resp['etag']
    mimetype = resp['mimeType']
    title = resp['title']
    basename = title
    # tag is: <request hash>-<folder id>
    folder_id = tag.split('-')[-1]

    bucket_path = os.path.join(gcs_path_format, title)  # /foo/bar/baz/file.png
    asset = Asset(tag=tag, bucket_path=bucket_path, status='started',
            drive_id=drive_id, title=title, basename=basename, mimetype=mimetype,
            etag=etag, folder_id=folder_id, parent_tag=parent_tag)
    asset.put()
    path = '/'.join(bucket_path.lstrip('/').split('/')[1:])  # bar/baz
    bucket = bucket_path.lstrip('/').split('/')[0]  # foo
    if True or not appengine_config.DEV_SERVER:
        try:
            stat = gcs.stat(bucket_path)
            if stat.etag != etag:
                raise UploadRequiredError()
            raise UploadRequiredError()
        except (gcs.NotFoundError, UploadRequiredError):
            # TODO - Verify if asset needs to be replicated.
            fp = download_asset_in_parts(drive_id)
            write_gcs_file(path, bucket, fp, mimetype)
    if ('png' in mimetype or 'jpeg' in mimetype) and upload_to_cloud_images:
        gs_path = '/gs/{}'.format(bucket_path.lstrip('/'))
        blob_key = blobstore.create_gs_key(gs_path)
        url = images.get_serving_url(blob_key, secure_url=True)
        asset.serving_url = url
    # Obfuscate filenames for mp4 files.
    if ('mp4' in mimetype) and upload_to_cloud_images:
        stat_result = gcs.stat(bucket_path)
        clean_etag = stat_result.etag.replace('"', '').replace("'", '')
        destination = '/{}/blobs/{}.mp4'.format(bucket, clean_etag)
        gcs.copy2(bucket_path, destination)
        url = 'https://storage.googleapis.com/{}'.format(destination.lstrip('/'))
        asset.serving_url = url
    asset.status = 'finished'
    asset.put()
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
    # Fix for handling SVGs.
    mimetype = 'image/svg+xml' if path.endswith('.svg') else mimetype
    storage_service = get_storage_service()
    media = http.MediaIoBaseUpload(
        fp, mimetype=mimetype, chunksize=GCS_UPLOAD_CHUNKSIZE, resumable=True)
    email = 'grow-prod@appspot.gserviceaccount.com'
    # allUsers:READER needed for Google Dynamic Image Service uploads.
    acl = [{
        'role': 'READER',
        'entity': 'allUsers',
    }, {
        'role': 'OWNER',
        'entity': 'user-{}'.format(email),
        'email': email,
    }]
    body = {
        'name': path,
        'acl': acl,
    }
    req = storage_service.objects().insert(
        media_body=media, name=path, bucket=bucket, body=body)
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
