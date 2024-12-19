from PIL import Image
from pathlib import Path
import argparse
import boto3

def check_and_convert_images(path, action, bucket_name=None, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Checks images in the given path for AVIF format and performs the specified action.
    Supports both local and S3 storage.

    Args:
        path: Path to the directory or a single file (local or S3 URI).
        action: 
            - 'list': Print a list of files not in AVIF format.
            - 'count': Count the number of files not in AVIF format.
            - 'convert': Convert non-AVIF images to AVIF format.
        bucket_name: S3 bucket name (required for S3 operations).
        aws_access_key_id: AWS Access Key ID (required for S3 operations).
        aws_secret_access_key: AWS Secret Access Key (required for S3 operations).
    """

    is_s3 = path.startswith("s3://") 
    if is_s3:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket_name = path.split('/')[2] 
        prefix = '/'.join(path.split('/')[3:]) 
        paginator = s3.get_paginator('list_objects_v2')
        objects = paginator.paginate(Bucket=bucket_name, Prefix=prefix)['Contents']
        files = [obj['Key'] for obj in objects]
    else:
        if Path(path).is_dir():
            files = list(Path(path).glob('**/*.jpg')) + list(Path(path).glob('**/*.jpeg')) + list(Path(path).glob('**/*.png'))
        elif Path(path).is_file():
            files = [path]
        else:
            raise ValueError("Invalid path. Please provide a valid file or directory path.")

    non_avif_files = []
    for file in files:
        try:
            if is_s3:
                obj = s3.get_object(Bucket=bucket_name, Key=file)
                img = Image.open(obj['Body'])
            else:
                img = Image.open(file)
            if img.format != 'AVIF':
                non_avif_files.append(file)
        except Exception as e:
            print(f"Error processing {file}: {e}")

    if action == 'list':
        print("Files not in AVIF format:")
        for file in non_avif_files:
            print(file)
    elif action == 'count':
        print(f"Number of files not in AVIF format: {len(non_avif_files)}")
    elif action == 'convert':
        for file in non_avif_files:
            try:
                if is_s3:
                    obj = s3.get_object(Bucket=bucket_name, Key=file)
                    img = Image.open(obj['Body'])
                    new_filename = file.replace(Path(file).suffix, '.avif') 
                    s3.upload_fileobj(img, bucket_name, new_filename)
                    print(f"Converted {file} to {new_filename}")
                else:
                    img = Image.open(file)
                    new_filename = Path(file).with_suffix('.avif')
                    img.save(new_filename, 'avif')
                    print(f"Converted {file} to {new_filename}")
            except Exception as e:
                print(f"Error converting {file}: {e}")
    else:
        print("Invalid action. Please choose from 'list', 'count', or 'convert'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check and convert images to AVIF format.")
    parser.add_argument("path", help="Path to the directory or file (local or S3 URI).")
    parser.add_argument("action", choices=['list', 'count', 'convert'], help="Action to perform.")
    parser.add_argument("--bucket", help="S3 bucket name (required for S3 operations).")
    parser.add_argument("--aws_access_key_id", help="AWS Access Key ID (required for S3 operations).")
    parser.add_argument("--aws_secret_access_key", help="AWS Secret Access Key (required for S3 operations).")
    args = parser.parse_args()

    check_and_convert_images(args.path, args.action, args.bucket, args.aws_access_key_id, args.aws_secret_access_key)
