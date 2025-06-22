import boto3
import json
import uuid
import os
from datetime import datetime

def lambda_handler(event, context):
    # Obtener el bucket de entorno
    bucket_name = os.environ.get('INGEST_BUCKET')

    # Parsear el body
    raw_body = event.get('body', {})
    if isinstance(raw_body, str):
        try:
            comentario = json.loads(raw_body)
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'JSON inválido'})
            }
    else:
        comentario = raw_body

    # Generar nombre único para el archivo
    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    file_name = f"comentario_{timestamp}_{uuid.uuid4().hex}.json"

    # Subir comentario como archivo JSON al bucket
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, f"comentarios/{file_name}").put(
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': {'message': 'Comentario ingresado correctamente', 'archivo': file_name}
    }
