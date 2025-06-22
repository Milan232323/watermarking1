import requests
import threading
import uuid
import argparse
import time

BASE_URL = 'https://watermark-backend.azurewebsites.net/api/'

URL_GET_UPLOAD_URL = BASE_URL + 'get-upload-url'
URL_UPLOAD_FILE = None  # SAS URL will be dynamic
URL_GET_DOWNLOAD_URL = BASE_URL + 'get-download-url'
URL_MAIN_PROCESS = BASE_URL + 'main_process_func'
URL_CHECK_PROGRESS = BASE_URL + 'check_progress_func'
URL_CLEANUP = BASE_URL + 'cleanup-after-job'


# ==== API functions =========

def get_upload_url():
    filename = 'dontcare'
    response = requests.get(URL_GET_UPLOAD_URL, params={'filename': filename})
    response.raise_for_status()
    return response.json()['uploadUrl']


def upload_file(sas_url, filepath):
    with open(filepath, 'rb') as f:
        headers = {'x-ms-blob-type': 'BlockBlob'}
        response = requests.put(sas_url, data=f, headers=headers)
    if response.status_code != 201:
        raise Exception(f"Upload failed: {response.status_code} - {response.text}")


def start_process_async(video_sas, image_sas):
    job_id = str(uuid.uuid4())
    payload = {
        'job_id': job_id,
        'video_sas': video_sas,
        'image_sas': image_sas
    }

    def do_request():
        try:
            response = requests.post(URL_MAIN_PROCESS, json=payload)
            response.raise_for_status()
            print(f"[{job_id}] Processing started: {response.json().get('message', '')}")
        except Exception as e:
            print(f"[{job_id}] Failed to start process: {e}")

    # Launch in background thread (non-blocking)
    threading.Thread(target=do_request, daemon=True).start()

    return job_id

def start_process_sync(video_sas, image_sas):
    job_id = str(uuid.uuid4())
    payload = {
        'job_id': job_id,
        'video_sas': video_sas,
        'image_sas': image_sas
    }
    response = requests.post(URL_MAIN_PROCESS, json=payload)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Print the response text to see server error message
        print(f"HTTP error occurred: {e}")
        print("Response content:", response.text)
        raise
    else:
        # Success: parse JSON or do whatever
        print(f"Processing started: {response.json().get('message', '')}")
        return job_id

def poll_process(job_id):
    response = requests.get(URL_CHECK_PROGRESS, params={'job_id': job_id})
    response.raise_for_status()
    data = response.json()
    print(f"Progress: {data['progress_value']}%")
    if data.get('done'):
        print("Processing complete.")
    return (data['progress_value'], data['done'])
    

def get_download_link(job_id, file_type):
    response = requests.get(URL_GET_DOWNLOAD_URL, params={'job_id': job_id, 'type': file_type})
    response.raise_for_status()
    return response.json()['downloadUrl']


def download_file(url, output_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(output_path, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded to: {output_path}")


def cleanup(job_id):
    response = requests.get(URL_CLEANUP, params={'job_id': job_id})
    if not response.ok:
        print(f"Cleanup warning: {response.text}")


# ==== functions to make testing easier =========
def save_sas_to_file(video_sas, image_sas, filename="sas.txt"):
    with open(filename, "w") as f:
        f.write(f"{video_sas}\n{image_sas}\n")

def read_sas_from_file(filename="sas.txt"):
    with open(filename, "r") as f:
        lines = f.read().splitlines()
    if len(lines) < 2:
        raise ValueError("sas.txt does not contain enough lines")
    return lines[0], lines[1]



if __name__ == "__main__":
    
    # Upload video and image
    parser = argparse.ArgumentParser()
    parser.add_argument("-upload", action="store_true", help="Upload new files and save SAS URL")
    args = parser.parse_args()

    if args.upload:
        video_sas = get_upload_url()
        image_sas = get_upload_url()

        video_path = "/home/imke/Downloads/timer.mp4"
        image_path = "/home/imke/Downloads/logo.jpeg"

        upload_file(video_sas, video_path)
        upload_file(image_sas, image_path)

        save_sas_to_file(video_sas, image_sas)
    else:
        video_sas, image_sas = read_sas_from_file()


    job_id = start_process_sync(video_sas, image_sas)


    done = False
    while not done:
        result = poll_process(job_id)
        done = result[1]
        print(f"Progress: {result[0]}%, done is {result[1]}")
        time.sleep(1)


    download_video_sas = get_download_link(job_id, 'output_video')
    download_image_sas = get_download_link(job_id, 'output_thumbnail')

    download_file(download_video_sas, './output_video.mp4')
    download_file(download_image_sas, './output_thumbnail.jpg')

    cleanup(job_id)

