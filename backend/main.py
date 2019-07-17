from google.appengine.api import app_identity
from datetime import datetime
import os
import jinja2
import uploader
import webapp2
import yaml

JINJA2_LOADER = jinja2.FileSystemLoader(
    os.path.join(os.path.dirname(__file__), 'views'))
JINJA2_ENV = jinja2.Environment(loader=JINJA2_LOADER)


def upload_folder_id(folder_id, gcs_path_format):
    tag = '{}-{}'.format(os.getenv('REQUEST_ID_HASH', ''), folder_id)
    parent_tag = uploader.get_parent_tag(folder_id)
    from google.appengine.ext import deferred
    clean_date = datetime.today().strftime('%Y-%m-%d')
    gcs_path_format = gcs_path_format.replace('{date}', clean_date)
    deferred.defer(uploader.create_tag, folder_id, gcs_path_format, tag, parent_tag)
    # tasks = uploader.download_resource(resource_id=folder_id, gcs_path_format=gcs_path_format, tag=tag, parent_tag=parent_tag)
    return tag


class CronHandler(webapp2.RequestHandler):

    def get(self):
        schedule = yaml.load(open('schedule.yaml'))['schedule']
        for item in schedule:
            gcs_path_format = item['gcs_path_format']
            for folder_id in item['folder_ids']:
                tag = upload_folder_id(folder_id, gcs_path_format)
#        sender = '{}@appspot.gserviceaccount.com'.format(app_identity.get_application_id())
#        to = []
#        mail.send_mail(sender=sender, to=to, subject='Asset upload', body=content)


class MainHandler(webapp2.RequestHandler):

    def render_template(self, name, context=None):
        template = JINJA2_ENV.get_template(name)
        context = context or {}
        context['service_account_email'] = uploader.SERVICE_ACCOUNT_EMAIL
        html = template.render(context)
        self.response.content_type = 'text/html'
        self.response.out.write(html)

    def post(self):
        folder_id = self.request.POST['folder_id']
        gcs_path_format = self.request.POST['gcs_path_format']
        tag = upload_folder_id(folder_id, gcs_path_format)
        kwargs = {
            'folder_id': folder_id,
            'gcs_path_format': gcs_path_format,
            'tag': tag,
#            'tasks': tasks,
#            'paths': paths,
        }
        self.render_template('index.html', kwargs)

    def get(self):
        tag = self.request.get('tag')
        folder_id = self.request.get('folder_id')
        gcs_path_format = self.request.get('gcs_path_format')
        entities = None
        if tag:
            query = uploader.Asset.query()
            query = query.filter(uploader.Asset.tag == tag)
            query = query.order(uploader.Asset.basename)
            entities = query.fetch()
        parent_entity_ids_to_entities = {}
        parent_tag = uploader.get_parent_tag(folder_id)
        if folder_id and parent_tag:
            parent_query = uploader.Asset.query()
            parent_query = parent_query.filter(uploader.Asset.tag == parent_tag)
            parent_query = parent_query.order(uploader.Asset.basename)
            parent_entities = query.fetch()
            for ent in parent_entities:
                parent_entity_ids_to_entities[ent.drive_id] = ent
        tag_ent = uploader.get_tag(tag) if tag else None
        kwargs = {
            'folder_id': folder_id,
            'gcs_path_format': gcs_path_format,
            'parent_entity_ids_to_entities': parent_entity_ids_to_entities,
            'entities': entities,
            'tag': tag,
            'tag_ent': tag_ent,
        }
        self.render_template('index.html', kwargs)


app = webapp2.WSGIApplication([
    ('/cron', CronHandler),
    ('/', MainHandler),
])
