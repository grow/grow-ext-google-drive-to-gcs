from google.appengine.ext import vendor
vendor.add('lib')

import uploader
import webapp2


class MainHandler(webapp2.RequestHandler):

    def get(self):
        import uploader
        folder_id = '1Csrpq-QNztGYE1iojSKaQpDzaZ0Dwfrx'
        gcs_path_format = '/mannequin/2018/exports/auto/'
        uploader.download_resource(resource_id=folder_id, gcs_path_format=gcs_path_format)


app = webapp2.WSGIApplication([
    ('/', MainHandler),
])
