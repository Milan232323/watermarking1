import requests
from job_db import create_job_entry

BASE_URL = 'https://watermark-backend.azurewebsites.net/api/'
MOVE_WATERMARK_URL = BASE_URL + 'move_watermark_func'
SPLIT_CHUNKS_URL = BASE_URL + 'split_chunks_func' 
CHUNK_SIZE = 50

# === Helper Function ===
def post_json(url, payload):
    """Send a POST request with JSON payload and print status."""
    r = requests.post(url, json=payload)
    print(f"{url} → {r.status_code}")
    if not r.ok:
        print("Response:", r.text)
    return r

def run_pipeline(job_id, video_SAS, image_SAS):
    create_job_entry(job_id)  # voeg job-status toe
    
    # === Step 1: Move watermark to a storage location that we can find
    print("\nStep 1: Moving watermark to reachable location...")
    response = post_json(MOVE_WATERMARK_URL, {
        "job_id" : job_id,
        "image_SAS": image_SAS
    })
    if not response.ok:
        raise Exception(f"Exception in moving watermark: {response.status_code}: {response.text}")

    # === Step 2: Split video into chunks ===
    print("\nStep 2: Splitting into chunks...")
    response = post_json(SPLIT_CHUNKS_URL, {
        "job_id" : job_id,
        "video_SAS": video_SAS,
        "chunk_size": CHUNK_SIZE
    })
    if not response.ok: 
        raise Exception(f"Exception in splitting video chunks: {response.status_code}: {response.text}")


    # === Step 3: Apply watermark to each chunk, imediately triggered after splitting func. 
    

    # === Step 4a: Generate thumbnails from first frame of each chunk, immediately triggered after splitting func ===

    
    # === Step 4b: Concatenate thumbnails into one image, tirggered after all thumbnails are done ===

    # === Step 5: Concatenate processed video chunks, triggered after all watermarking is done ===

    
    


 