# grow-ext-google-drive-to-gcs

[![Build Status](https://travis-ci.org/grow/grow-ext-google-drive-to-gcs.svg?branch=master)](https://travis-ci.org/grow/grow-ext-google-drive-to-gcs)

(WIP)

An extension for replicating a Google Drive file to Google Cloud Storage. 

## Concept

The concept allows stakeholders to provide you with Drive file IDs and you can
immediately leverage them on the site without manually downloading and
reuploading them.

Here's the workflow:

1. Upload an image to Google Cloud Storage.
1. Ensure the backend microservice has read access to the object in GCS. (More on this below).
1. Use the template function or YAML extension provided in this extension.
1. Supply options to the extension to generate the right URL.

### Grow setup

1. Create an `extensions.txt` file within your pod.
1. Add to the file: `git+git://github.com/grow/grow-ext-google-drive-to-gcs`
1. Run `grow install`.
1. Add the following section to `podspec.yaml`:

```
extensions:
  preprocessors:
  - extensions.google_drive_to_gcs.GoogleDriveToGCSExtension

preprocessors:
- kind: google_drive_to_gcs
```

### Google Drive setup

(WIP)

### Google Cloud Storage setup

(WIP)

### Usage in templates

(WIP)

```
{{google_drive_to_gcs("/bucket/folder/", "https://drive.google.com/open?id=0B2D-9SyFrh1CcHVjOGlsT0Y4UG8").gs_path}}
```
