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


def s3_get_item(event, context):
    table = dynamodb.Table(db_table)
    response = table.get_item(Key={
        'id': event['pathParameters']['id']
    })
    item = response['Item']

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(item),
        'isBase64Encoded': False
    }


def s3_delete_item(event, context):
    item_id = event['pathParameters']['id']
    response = {
        "statusCode": 500,
        "body": f"An error occured while deletegin post {item_id}"
    }
    table = dynamodb.Table(db_table)
    resp = table.delete_item(Key={
        'id': event['pathParameters']['id']
    })
    all_good_response = {
        "deleted": True,
        "itemDeletedId": item_id
    }

    if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            "statusCode": 200,
            'headers': {'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(all_good_response)
        }
    return response


def s3_get_thumbnail_urls(event, context):
    table = dynamodb.Table(db_table)
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return {
        'statusCode': 200,
        'headers': {'ContentType': 'application-json'},
        'body': json.dumps(data)
    }
