project := 
app_identity_email ?= $(project)@appspot.gserviceaccount.com
app_identity_key_path ?= key.pem

enable-services:
	gcloud services enable sheets.googleapis.com --project=$(project)
	gcloud services enable drive --project=$(project)
	gcloud services enable storage-api.googleapis.com --project=$(project)

deploy:
	gcloud app deploy \
	  -q \
	  --project=$(project) \
	  --no-promote \
	  --version=auto \
	  --verbosity=error \
	  app.yaml

install:
	pip install -t lib -r requirements.txt

run:
	dev_appserver.py \
	  --appidentity_email_address $(app_identity_email) \
	  --appidentity_private_key_path $(app_identity_key_path) \
	  app.yaml
