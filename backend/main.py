from google.appengine.ext import vendor
vendor.add('lib')

import os
import jinja2
import uploader
import webapp2

JINJA2_LOADER = jinja2.FileSystemLoader(
    os.path.join(os.path.dirname(__file__), 'views'))
JINJA2_ENV = jinja2.Environment(loader=JINJA2_LOADER)


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
        paths = uploader.download_resource(resource_id=folder_id, gcs_path_format=gcs_path_format)
        kwargs = {
            'folder_id': folder_id,
            'gcs_path_format': gcs_path_format,
            'paths': paths,
        }
        self.render_template('index.html', kwargs)

    def get(self):
        self.render_template('index.html')


app = webapp2.WSGIApplication([
    ('/', MainHandler),
])
