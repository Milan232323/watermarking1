import azure.functions as func
from watermarking import process_video_chunk, concat_chunks
import storage_functions
import json
import cv2
import numpy as np
import os
import uuid
from datetime import datetime, timedelta
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from run_pipeline import run_pipeline
from job_db import update_job, get_job, atomic_increment
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
import logging




app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# -----------------------------------------------------
# 1. Process a video chunk (apply watermark)
# -----------------------------------------------------
@app.function_name(name="process_chunk_func")
@app.queue_trigger(arg_name="msg", queue_name="watermarkqueue", connection="AZURE_STORAGE_CONNECTION_STRING")
def process_chunk_func(msg: func.QueueMessage) -> None:    
    logging.info("PROCESSING CHUNK")

    try:
        data = json.loads(msg.get_body().decode("utf-8"))
        job_id = data["job_id"]
        chunk_id = data["chunk_id"]
        
        job = get_job(job_id)
        current_busy = job["ChunkWatermarkBusy"]
        update_job(job_id, {"ChunkWatermarkBusy": current_busy + 1})


        # Download chunk and watermark to local storage
        chunk_path = storage_functions._unique_filepath_tmp('mp4')
        storage_functions.download_file_internal(job_id, 'video_chunk_orig', chunk_path, index=chunk_id)
        watermark_path = storage_functions._unique_filepath_tmp('jpg')
        storage_functions.download_file_internal(job_id, 'watermark', watermark_path)
        
        # watermark chunk
        output = process_video_chunk(job_id, chunk_path, watermark_path, chunk_id)

        # Upload watermarked chunk
        storage_functions.upload_file_internal(job_id, output, 'video_chunk_mod', index=chunk_id)

        # Watermark is uploaded so up database
        current_done = atomic_increment(job_id, "ChunkWatermarkDone")
        
        # Check if this was the last chunk. If so, send a trigger for the concat function
        total_num_chunks = job["TotalNumChunks"]
        if total_num_chunks > 0 and current_done == total_num_chunks:
            queue = QueueClient.from_connection_string(conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"], queue_name="watermarkdone")
            queue.message_encode_policy = BinaryBase64EncodePolicy()
            queue.message_decode_policy = BinaryBase64EncodePolicy()
            message = {
                "job_id": job_id,
                "num_watermark_chunks": total_num_chunks
            }
            message_string = json.dumps(message)
            message_bytes =  message_string.encode('utf-8')
            queue.send_message(queue.message_encode_policy.encode(content=message_bytes))

            # delete chunks and watermark from local storage
            os.remove(chunk_path)
            os.remove(watermark_path)
            os.remove(output)

            logging.info(f"Watermark {chunk_id} succesful")

    except Exception as e:
        logging.error(f"Error in concat processing chunk {chunk_id}: {e}")


 

    





# -----------------------------------------------------
# 2. Concatenate a list of chunks into one video
# -----------------------------------------------------
@app.function_name(name="concat_chunks_func")
@app.queue_trigger(arg_name="msg", queue_name="watermarkdone", connection="AZURE_STORAGE_CONNECTION_STRING")
def concat_chunks_func(msg: func.QueueMessage) -> None: 
    try:
        data = json.loads(msg.get_body().decode("utf-8"))
        job_id = data["job_id"]
        num_chunks = data["num_watermark_chunks"]

        chunk_paths = []
        
        # download all chunks to local storage
        for i in range(num_chunks):
            save_path = storage_functions._unique_filepath_tmp('mp4')
            storage_functions.download_file_internal(job_id, 'video_chunk_mod', save_path, index=i)
            chunk_paths.append(save_path)

        # concat final video
        output_path = storage_functions._unique_filepath_tmp('mp4')
        concat_chunks(chunk_paths, output_path)

        # Upload finished video to blob storage
        storage_functions.upload_file_internal(job_id, output_path, 'output_video')

        # Video is done!
        update_job(job_id, {"Concat": True})

        # delete chunk and final video from local storage
        for path in chunk_paths:
            os.remove(path)
        os.remove(output_path)


        logging.info("concat video chunks succesful")
    except Exception as e:
        logging.error("Error in concat video chunks")



@app.function_name(name="split_chunks_func")
@app.route(route="split_chunks_func", methods=["POST"])
def split_chunks_func(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        video_SAS = data['video_SAS']
        job_id = data['job_id']
        chunk_size = int(data.get('chunk_size', 150))

        # path to store the video locally
        video_path = storage_functions._unique_filepath_tmp('mp4')
        storage_functions.get_user_video(video_SAS, video_path)

        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')

        chunk_id = 0
        frame_count = 0
        writer = None

        w_queue = QueueClient.from_connection_string(conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"], queue_name="watermarkqueue")
        t_queue = QueueClient.from_connection_string(conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"], queue_name="thumbnailqueue")
        w_queue.message_encode_policy = BinaryBase64EncodePolicy()
        w_queue.message_decode_policy = BinaryBase64DecodePolicy()
        t_queue.message_encode_policy = BinaryBase64EncodePolicy()
        t_queue.message_decode_policy = BinaryBase64DecodePolicy()


        while True:
            success, frame = cap.read()
            if not success:
                break

            if frame_count % chunk_size == 0:
                if writer is not None:
                    writer.release()
                    # Upload the finished chunk to blob storage
                    storage_functions.upload_file_internal(job_id, chunk_path, "video_chunk_orig", index=chunk_id)
                    
                    # Up the database
                    try:
                        update_job(job_id, {"ChunkUploaded": chunk_id + 1})
                    except Exception as e:
                        print(f"Error: couldn't update DB after chunk {chunk_id} upload: {e}")
                    
                    # Trigger watermarking and thumbnailing for new chunk
                    message = {
                        "job_id": job_id,
                        "chunk_id": chunk_id
                    }
                    message_string = json.dumps(message)
                    message_bytes =  message_string.encode('utf-8')
                    w_queue.send_message(w_queue.message_encode_policy.encode(content=message_bytes))
                    t_queue.send_message(w_queue.message_encode_policy.encode(content=message_bytes))

                    os.remove(chunk_path)
                    chunk_id += 1

                chunk_path = storage_functions._unique_filepath_tmp('mp4')
                writer = cv2.VideoWriter(chunk_path, fourcc, fps, (width, height))
                print(f"[INFO] Writing to {chunk_path}")

            writer.write(frame)
            frame_count += 1

        # Handle the last chunk
        if writer is not None:
            writer.release()
            storage_functions.upload_file_internal(job_id, chunk_path, "video_chunk_orig", index=chunk_id)

            # Up the database
            try:
                update_job(job_id, {"ChunkUploaded": chunk_id + 1})
            except Exception as e:
                print(f"Error: couldn't update DB after chunk {chunk_id} upload: {e}")
            
            
            # Trigger watermarking for last chunk
            message = {
                "job_id": job_id,
                "chunk_id": chunk_id
            }
            message_string = json.dumps(message)
            message_bytes =  message_string.encode('utf-8')
            w_queue.send_message(w_queue.message_encode_policy.encode(content=message_bytes))
            t_queue.send_message(w_queue.message_encode_policy.encode(content=message_bytes))

            chunk_id += 1
            os.remove(chunk_path)
        
        cap.release()

        update_job(job_id, {"TotalNumChunks": chunk_id})

        os.remove(video_path) # we don't need the full input video anymore, so remove it.

        return func.HttpResponse(
            json.dumps({"status": "success"}), mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.function_name(name="thumbnail_chunk_func")
@app.queue_trigger(arg_name="msg", queue_name="thumbnailqueue", connection="AZURE_STORAGE_CONNECTION_STRING")
def thumbnail_chunk_func(msg: func.QueueMessage) -> None:    
    try:
        data = json.loads(msg.get_body().decode("utf-8"))
        job_id = data["job_id"]
        chunk_id = data["chunk_id"]

        job = get_job(job_id)
        current_busy = job["ThumbnailBusy"]
        update_job(job_id, {"ThumbnailBusy": current_busy + 1})

        # download chunk to local storage
        chunk_path = storage_functions._unique_filepath_tmp('mp4')
        storage_functions.download_file_internal(job_id, 'video_chunk_orig', chunk_path, index=chunk_id)

        cap = cv2.VideoCapture(chunk_path)
        success, frame = cap.read()
        cap.release()

        if not success:
            raise ValueError("Failed to read first frame from chunk")

        # save frame to local storage
        output_path = storage_functions._unique_filepath_tmp('jpg')
        cv2.imwrite(output_path, frame)  

        # write frame to blob storage
        storage_functions.upload_file_internal(job_id, output_path, 'thumbnail', index=chunk_id)

        # Done, so update the database
        current_done = atomic_increment(job_id, "ThumbnailDone")

        # Check if this was the last chunk. If so, send a trigger
        total_num_chunks = job["TotalNumChunks"]
        if total_num_chunks > 0 and current_done == total_num_chunks:
            queue = QueueClient.from_connection_string(conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"], queue_name="thumbnaildone")
            queue.message_encode_policy = BinaryBase64EncodePolicy()
            queue.message_decode_policy = BinaryBase64EncodePolicy()
            message = {
                "job_id": job_id,
                "num_thumbnail_chunks": total_num_chunks
            }
            message_string = json.dumps(message)
            message_bytes =  message_string.encode('utf-8')
            queue.send_message(queue.message_encode_policy.encode(content=message_bytes))

        # delete chunk and frame from local storage
        os.remove(chunk_path)
        os.remove(output_path)

        logging.info(f"Thumbnail chunk {chunk_id} succesful")

        
    except Exception as e:
        logging.error(f"Error processing thumbnail chunk {chunk_id}: {e}")


@app.function_name(name="concat_thumbnails_func")
@app.queue_trigger(arg_name="msg", queue_name="thumbnaildone", connection="AZURE_STORAGE_CONNECTION_STRING")
def concat_thumbnails_func(msg: func.QueueMessage) -> None:    
    try:
        data = json.loads(msg.get_body().decode("utf-8"))
        job_id = data["job_id"]
        num_thumbs = data["num_thumbnail_chunks"]

        thumbs = []
        paths = []
        # Download all thumbs to local storage and read the image
        for i in range(num_thumbs):
            save_path = storage_functions._unique_filepath_tmp('jpg')
            storage_functions.download_file_internal(job_id, 'thumbnail', save_path, index=i)
            img = cv2.imread(save_path)
            if img is not None:
                thumbs.append(img)
                paths.append(save_path)
            
        if not thumbs:
            raise ValueError("No valid thumbnails found")

        total_width = sum(img.shape[1] for img in thumbs)
        height = thumbs[0].shape[0]
        grid = np.zeros((height, total_width, 3), dtype=np.uint8)

        x = 0
        for img in thumbs:
            grid[:, x:x + img.shape[1]] = img
            x += img.shape[1]

        # Save to local storage and then upload to blob storage
        output_path = storage_functions._unique_filepath_tmp('jpg')
        cv2.imwrite(output_path, grid)
        storage_functions.upload_file_internal(job_id, output_path, 'output_thumbnail')

        # Thumbnail is done!
        update_job(job_id, {"ThumbnailConcat": True})

        # Remove frames and final thumbnail from local storage
        for path in paths:
            os.remove(path)
        os.remove(output_path)

        logging.info("concat thumbnail chunks succesful")

    except Exception as e:
        logging.error("error in cancating thumbnail chunks")
    

@app.function_name(name="move_watermark_func")
@app.route(route="move_watermark_func", methods=["POST"])
def move_watermark_func(req: func.HttpRequest) -> func.HttpResponse:
    data = req.get_json()
    job_id = data["job_id"]
    image_sas = data["image_SAS"]

    try:
        storage_functions.move_watermark(job_id, image_sas)
        return func.HttpResponse(
            json.dumps({"status": "success"}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)



@app.function_name(name="main_process_func")
@app.route(route="main_process_func", methods=["POST"])
def main_process_func(req: func.HttpRequest) -> func.HttpResponse:
    data = req.get_json()
    job_id = data["job_id"]
    video_sas = data["video_sas"]
    image_sas = data["image_sas"]

    try:
        run_pipeline(job_id, video_sas, image_sas)

        return func.HttpResponse(
            json.dumps({"status": "success"}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
   


@app.function_name(name="check_progress_func")
@app.route(route="check_progress_func", methods=["GET"])
def check_progress_func(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.params.get("job_id")
    
    try:
        job = get_job(job_id)
        progress_in_percent = 0
        done = False
        total_chunks = job["TotalNumChunks"]
        if total_chunks > 0:
            progress_in_percent += job["ChunkWatermarkDone"] / total_chunks * 40
            progress_in_percent += job["ThumbnailDone"] / total_chunks * 40

        if job["Concat"]:
            progress_in_percent += 10
        if job["ThumbnailConcat"]:
            progress_in_percent += 10
        
        if job["Concat"] and job["ThumbnailConcat"]:
            done = True

        
        a = job["ChunkWatermarkDone"]
        b = job["ThumbnailDone"]
        c = job["Concat"] 
        d = job["ThumbnailConcat"]

        logging.info(f"Progress is {progress_in_percent}%, done is {done}. Watermarked: {a}. Thumnailed: {b}. concat: {c}, thumnailconcat: {d}. totalchunks = {total_chunks}")

        return func.HttpResponse(
                json.dumps({"progress_value": progress_in_percent, "done": done}),
                mimetype="application/json",
                status_code=200
            )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)



'''
Use this function to get a url to upload a file. This function should be used if the client
wants to upload a file. The file can then be uploaded to the obtained URL.
'''
@app.function_name(name="get-upload-url")
@app.route(route="get-upload-url")
def generate_sas(req: func.HttpRequest) -> func.HttpResponse:
    filename = str(uuid.uuid4())

    container_name = "uploads"

    try:
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
        account_key = os.environ.get("AZURE_STORAGE_KEY")

        sas_token = generate_blob_sas(
            account_name=account_name,
            account_key=account_key,
            container_name=container_name,
            blob_name=filename,
            permission=BlobSasPermissions(write=True, read=True),
            expiry=datetime.utcnow() + timedelta(minutes=15),
        )

        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{filename}?{sas_token}"

        return func.HttpResponse(f'{{"uploadUrl": "{url}"}}',
                                 mimetype="application/json",
                                 status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error generating SAS URL: {str(e)}", status_code=500)


'''
Use this function when the client want to download the result file. The function will provide
the URL.
type can be either 'output_video' or 'output_thumbnail'
'''
@app.function_name(name="get-download-url")
@app.route(route='get-download-url')  
def get_download_url(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.params.get("job_id")
    type = req.params.get("type")  

    if job_id is None:
        return func.HttpResponse("Missing job_id parameter", status_code=400)
    if type not in ['output_video', 'output_thumbnail']:
        return func.HttpResponse("Invalid or missing type parameter", status_code=400)
    
    filename = storage_functions._form_filename(job_id, type)
    container_name = 'downloads'

    try:
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
        account_key = os.environ.get("AZURE_STORAGE_KEY")
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=filename,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(minutes=15),
        )

        url =  f"https://{account_name}.blob.core.windows.net/{container_name}/{filename}?{sas_token}"
        
        return func.HttpResponse(f'{{"downloadUrl": "{url}"}}',
                                 mimetype="application/json",
                                 status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error generating SAS URL: {str(e)}", status_code=500)

@app.function_name(name="cleanup-after-job")
@app.route(route='cleanup-after-job')  
def cleanup_after_job(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.params.get("job_id")
    try:
        storage_functions.delete_files_from_job(job_id)
        return func.HttpResponse(status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error in cleanup: {str(e)}", status_code=500)