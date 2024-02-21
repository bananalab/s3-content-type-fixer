import boto3
import click
import json
import mimetypes
from multiprocessing import Pool

session = boto3.Session()
s3 = session.resource('s3')

@click.command()
@click.option('--bucket', required=True, help="Bucket to scan.")
@click.option('--concurrency', type = int, help="Number of processes to run.")
@click.option('--dry-run', default=True, type=bool, help="Don't make any changes.")
@click.option('--mime-types', '-m', multiple=True, help="Only update objects with this mimetype.  If not specified all mimetype are updated. Can be specified multiple times.")
@click.option('--output', help="Ouput format.",
              type=click.Choice(['none', 'json', 'csv'], case_sensitive=False),
              default='csv')
def cli(**kwargs):
    '''
    Scan an S3 bucket for misconfigured content-type metadata.
    '''
    with Pool(kwargs["concurrency"]) as p:
        for page in s3.Bucket(kwargs["bucket"]).objects.pages():
            l = [{**{"bucket":obj.bucket_name, "key":obj.key}, **kwargs} for obj in page]
            p.map(scan_bucket, l)

def scan_bucket(args):
    obj = s3.Object(args["bucket"], args["key"])
    mime_type = mimetypes.guess_type(obj.key)[0]
    if mime_type in args["mime_types"] or not args["mime_types"]:
        if mime_type and not mime_type == obj.content_type:
            output_mismatch(obj, args["output"], mime_type)
            if not args["dry_run"]:
                set_content_type(obj, mime_type)

def output_mismatch(obj, output, mime_type):
    item = {
         "last_modified":str(obj.last_modified),
         "key":obj.key,
         "s3_content_type":obj.content_type, 
         "mimetype":mime_type
    }

    if output == "csv":
            print(",".join(list(item.values())))
    elif output == "json":
            print(json.dumps(item))

def set_content_type(obj, mime_type):
    metadata = obj.metadata
    copy_args = {
        "Bucket": obj.Bucket().name,
        "Key": obj.key
    }

    extra_args = {
        "ContentType": mime_type,
        "Metadata": metadata,
        "MetadataDirective": "REPLACE"
    }

    obj.copy(copy_args, ExtraArgs = extra_args)
