import os
import io
import logging
import time
import uuid
from azure.storage.blob import BlobClient, BlobServiceClient

'''
Valid types are:
* watermark
* video_chunk_orig
* video_chunk_mod
* thumbnail
* output_video
* output_thumbnail
* audio
Note that for video_chunk_orig, video_chunk_mod and thumnail, and index is required
'''
def _form_filename(job_id, type, index=None):
    if job_id is None:
        raise RuntimeError("_form_filename: no job_id")
    if type not in ['watermark', 'video_chunk_orig', 'video_chunk_mod', 'thumbnail', 'output_video', 'output_thumbnail', 'audio']:
        raise RuntimeError("_form_filename: invalid type")
    if index is None and type in ['video_chunk_orig', 'video_chunk_mod', 'thumbnail']:
        raise RuntimeError("_form_filename: no index")
    
    filename = f"{str(job_id)}_{type}"
    if index is not None:
        filename += f"_{str(index)}"

    if type in ['watermark', 'thumbnail', 'output_thumbnail']:
        filename += '.jpg'
    else:
        filename += '.mp4'

    logging.info(f"filename is {filename}")
    return filename


def _unique_filepath_tmp(extension):
    filename = str(uuid.uuid4()) + "." + extension
    path = os.path.join("/tmp", filename)
    return path

'''
The user uploads a video with a SAS or provides a SAS. This function reads the video
from that SAS and stores it locally at save_path so that it can be split into chunks.
'''
def get_user_video(sas_url, save_path):
    logging.info("Executing get_user_video")

    try:
        blob_client = BlobClient.from_blob_url(sas_url)
        stream = blob_client.download_blob()
        data = stream.readall()
    except Exception as e:
        raise RuntimeError(f"get_user_video failed. Invalid SAS URL or download error: {e}") 
    
    try:
        # Write the downloaded bytes to a file
        with open(save_path, "wb") as f:
            f.write(data)
        logging.info(f"Successfully saved video to {save_path}")
    except Exception as e:
        raise RuntimeError(f"get_user_video failed. Not able to save video: {e}") from e
    



'''
The user provides a SAS or uploads an image to a SAS. This function moves the watermark image
to the correct container. 
'''
def move_watermark(job_id, sas_url):
    logging.info("Executing move_watermark")
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container_name = 'internal'

    try:
        blob_client = BlobClient.from_blob_url(sas_url)
        stream = blob_client.download_blob()
        data = stream.readall() 
    except Exception as e:
        raise RuntimeError("Moving watermark failed. Invalid SAS") from e

    # create new blob client and upload the data
    name = _form_filename(job_id, 'watermark')

    try:
        new_blob = BlobClient.from_connection_string(conn_str, container_name, name)
        new_blob.upload_blob(io.BytesIO(data), overwrite=True)
        logging.info(f"Uploaded {name} to container '{container_name}'")
    except Exception as e:
        raise RuntimeError("Moving watermak failed. upload failed.") from e

'''
Use this function to upload a file to blob storage internally. So e.g. if a worker is done
with a chunk of the watermarking, that chunk can be uploaded to blob storage with this function. 
Params: 
* job_id
* filepath: Where the file is locally
* type: can be 'video_chunk_orig', 'video_chunk_mod', 'thumbnail', 'output_video', 'audio' or 'output_thumbnail'
* index: in case of video_chunk_orig, video_chunk_mod and thumbnail, an index is needed
  because we have multiple video chunks and multiple thumbnail parts.
'''
def upload_file_internal(job_id, filepath, type, index=None):
    logging.info("Executing upload_file_internal")

    if type not in ['video_chunk_mod', 'video_chunk_orig', 'thumbnail', 'output_video', 'output_thumbnail', 'audio']:
        raise RuntimeError("upload_file_internal: invalid type parameter")
    if index is None and type in ['video_chunk_orig', 'video_chunk_mod', 'thumbnail']:
        raise RuntimeError("upload_file_internal: missing index parameter")

    if 'output' in type:
        container_name = 'downloads'    
    else:
        container_name = "internal"
    
    filename = _form_filename(job_id, type, index)

    attempt = 0
    max_attempts = 2
    while attempt < max_attempts:
        try:
            conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
            blob = BlobClient.from_connection_string(conn_str, container_name, filename)

            with open(filepath, "rb") as data:
                blob.upload_blob(data, overwrite=True)
            logging.info(f"Uploaded {filename} succesfully (internal)!")
            return
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                raise RuntimeError(f"Uploading {filename} (internal) failed.") from e
            time.sleep(0.1)
            logging.error("Upload file internal failed. Trying again.")
    


'''
Use this function to download a file from blob storage internally.
E.g. when a worker needs to read a previously uploaded chunk from blob storage.
Params: 
* job_id
* type: can be 'watermark', 'video_chunk_orig', 'video_chunk_mod', 'audio' or 'thumbnail'
* index: in case of video_chunk_orig, video_chunk_mod and thumb, an index is needed
  because we have multiple video chunks and multiple thumbnail parts.
* save_path: Where to save the file locally
'''
def download_file_internal(job_id, type, save_path, index=None):
    logging.info("Executing download_file_internal")

    if type not in ['watermark', 'video_chunk_orig', 'video_chunk_mod', 'thumbnail', 'audio']:
        raise RuntimeError("upload_file_internal: invalid type parameter")
    if index is None and type in ['video_chunk_orig', 'video_chunk_mod', 'thumbnail']:
        raise RuntimeError("upload_file_internal: missing index parameter")

    container_name = "internal"
    filename = _form_filename(job_id, type, index)

    attempt = 0
    max_attempts = 2
    while attempt < max_attempts:
        try:
            # Initialize blob client
            conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
            blob = BlobClient.from_connection_string(conn_str, container_name, filename)

            # Download and save the blob to a file
            with open(save_path, "wb") as file:
                stream = blob.download_blob()
                file.write(stream.readall())

            logging.info(f"Downloaded {filename} to {save_path} successfully!")
            return
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                raise RuntimeError(f"Downloading {filename} (internal) failed.") from e
            time.sleep(0.1)
            logging.error("Download file internal failed. Trying again.")


'''
Delete a file from blob storage.  
'''
def delete_file(job_id, type=None, index=None):
    if type not in ['watermark', 'video_chunk_orig', 'video_chunk_mod', 'thumbnail', 'output_video', 'output_thumbnail', 'audio']:
        raise RuntimeError("delete file: invalid type")
    if index is None and type in ['video_chunk_orig', 'video_chunk_mod', 'thumbnail']:
        raise RuntimeError("delete file: no index")
    
    if 'output' in type:
        container_name = 'downloads'
    else:
        container_name = 'internal'

    filename = _form_filename(job_id, type, index)

    try:
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob = BlobClient.from_connection_string(conn_str, container_name, filename)
        blob.delete_blob()
        logging.info(f"Deleted {filename} successfully!")
    except Exception as e:
        raise RuntimeError(f"Deteling {filename} failed.") from e


'''
Delete all files in the container 'internal' with a certain job_id
'''
def delete_files_from_job(job_id):
    try:
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_client.get_container_client('internal')

        blobs_to_delete = container_client.list_blobs(name_starts_with=job_id)

        deleted_count = 0
        for blob in blobs_to_delete:
            container_client.delete_blob(blob.name)
            deleted_count += 1
        logging.info(f"Deleted {deleted_count} for {job_id} successfully!")
    
    except Exception as e:
        raise RuntimeError(f"Deteling files for job {job_id} failed.") from e
