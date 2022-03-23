from datetime import datetime
from io import BytesIO
import json
import uuid
import boto3
import os
from PIL import ImageOps, Image

s3 = boto3.client('s3')
size = int(os.environ['THUMBNAIL_SIZE'])
db_table = str(os.environ['DYNAMODB_TABLE'])
dynamodb = boto3.resource('dynamodb', region_name=str(os.environ['REGION_NAME']))


def get_s3_image(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    image_content = response['Body'].read()

    file = BytesIO(image_content)
    img = Image.open(file)
    return img


def image_to_thumbnail(image):
    return ImageOps.fit(image, (size, size), Image.ANTIALIAS)


def new_filename(key):
    key_split = key.rsplit('.', 1)
    return key_split[0] + '_thumbnail.png'


def upload_to_s3(bucket, key, image, img_size):
    out_thumbnail = BytesIO()

    image.save(out_thumbnail, 'PNG')
    out_thumbnail.seek(0)

    response = s3.put_object(
        ACL='public-read',
        Body=out_thumbnail,
        Bucket=bucket,
        ContentType='image/png',
        Key=key
    )
    print(response)

    url = f"{s3.meta.endpoint_url}/{bucket}/{key}"

    s3_save_thumbnail_url_to_dynamodb(url, img_size)

    return url


def s3_save_thumbnail_url_to_dynamodb(url_path, img_size):
    to_int = float(img_size * 0.53) / 1000
    table = dynamodb.Table(db_table)
    response = table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'url': str(url_path),
            'approxRedycedSize': str(to_int) + str(' KB'),
            'created_at': str(datetime.now()),
            'updated_at': str(datetime.now())
        }
    )

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response)
    }


def s3_thumbnail_generator(event, context):
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    img_size = event['Records'][0]['s3']['object']['size']
    print(bucket, key, img_size)

    if not key.endswith("_thumbnail.png"):
        image = get_s3_image(bucket, key)

        thumbnail = image_to_thumbnail(image)
        thumbnail_key = new_filename(key)

        url = upload_to_s3(bucket, thumbnail_key, thumbnail, img_size)

        return url
