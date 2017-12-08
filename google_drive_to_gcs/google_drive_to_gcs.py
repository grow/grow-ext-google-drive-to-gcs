from googleapiclient import discovery
from googleapiclient import http
from googleapiclient.http import MediaIoBaseDownload
from grow.preprocessors import google_drive
from jinja2.ext import Extension
from grow.common import oauth
from oauth2client import client
from grow.common import utils
from oauth2client import tools
from protorpc import messages
import grow
import httplib2
import io
import jinja2
import logging
import os


GCS_UPLOAD_CHUNKSIZE = 10 * 1024 * 1024
NUM_RETRIES = 2
BACKOFF = 4
ERRORS_TO_RETRY = (IOError, httplib2.HttpLib2Error, KeyError)

STORAGE_KEY = 'Grow SDK - Drive to GCS'
SCOPES = [
    'https://www.googleapis.com/auth/devstorage.full_control',
]


def url_to_file_id(url):
    # Formatted as https://drive.google.com/file/d/<file>/view?usp=sharing
    if url.endswith('sharing'):
        return url.split('/')[-2]
    # Formatted as https://drive.google.com/file/d/<file>
    if url.startswith('https://drive.google.com/file/d/'):
        return url.split('/')[-1]
    # Formatted as https://drive.google.com/open?id=<file>
    return url[url.index('=') + 1:]


def download_google_drive_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    completed = False
    while completed is False:
        status, completed = downloader.next_chunk()
        logging.info(
            'Downloading from Google Drive -> {} {}%'.format(file_id, int(status.progress() * 100)))
    return fh


def upload_google_storage_file(service, gs_path, fp, mimetype=None):
    bucket, path = gs_path.lstrip('/').split('/', 1)
    media = http.MediaIoBaseUpload(
        fp, chunksize=GCS_UPLOAD_CHUNKSIZE, mimetype=mimetype, resumable=True)
    text = 'Uploading to Google Cloud Storage -> {}:{}'
    logging.info(text.format(bucket, path))
    req = service.objects().insert(
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
    logging.info('Uploaded in %s tries -> %s' % (connections, gs_path))
    return '/{}/{}'.format(bucket, path)


class GoogleDriveSyncer(object):

    def __init__(self, pod):
        self.pod = pod
        self._cache = None

    def __repr__(self):
        return '<GoogleDriveSyncer>'

    @property
    def cache(self):
        if self._cache is None:
            podcache = self.pod.podcache
            ident = 'ext-google-drive-to-gcs'
            self._cache = podcache.get_object_cache(ident, write_to_file=True)
        return self._cache

    @property
    def drive_service(self):
        return google_drive.BaseGooglePreprocessor.create_service('drive', 'v3')

    @property
    def storage_service(self):
        credentials = oauth.get_or_create_credentials(
                scope=SCOPES, storage_key=STORAGE_KEY)
        http = httplib2.Http(ca_certs=utils.get_cacerts_path())
        http = credentials.authorize(http)
        return discovery.build('storage', 'v1', http=http)

    def execute(self, file_id, bucket_path):
        result = self.cache.get(file_id)
        if result is None:
            bucket_path = bucket_path.rstrip('/')
            gs_path = '{}/{}'.format(bucket_path, file_id)
            message = 'Syncing Google Drive file to GCS -> {} -> {}'
            message = message.format(file_id, gs_path)
            fh = download_google_drive_file(self.drive_service, file_id)
            mimetype = 'image/jpeg'  # TODO: Use real mimetype.
            gs_path = upload_google_storage_file(
                    self.storage_service, gs_path, fh, mimetype)
            result = {
                'gs_path': gs_path,
            }
            self.cache.add(file_id, result)
        return result


class GoogleDriveToGCSExtension(Extension):

    def __init__(self, environment):
        super(GoogleDriveToGCSExtension, self).__init__(environment)
        environment.globals['google_drive_to_gcs'] = \
                GoogleDriveToGCSExtension.do_drive_to_gcs

    @staticmethod
    @jinja2.contextfunction
    def do_drive_to_gcs(ctx, bucket_path, drive_url):
        pod = ctx['doc'].pod
        file_id = url_to_file_id(drive_url)
        syncer = GoogleDriveSyncer(pod)
        return syncer.execute(file_id, bucket_path)
