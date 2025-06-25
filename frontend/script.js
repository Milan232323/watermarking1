const URL_HOST = 'https://watermark-backend.azurewebsites.net/api/';
const URL_GET_UPLOAD_URL = URL_HOST + 'get-upload-url';
const URL_GET_DOWNLOAD_URL = URL_HOST + 'get-download-url';
const URL_MAIN_PROCESS = URL_HOST + 'main_process_func';
const URL_CHECK_PROGRESS = URL_HOST + 'check_progress_func';
const URL_CLEANUP = URL_HOST + 'cleanup-after-job';

// Upload a file using a file input (must be passed as a File object)
async function uploadFile(file) {
    const filename = file.name;
    console.log(`Requesting SAS URL for file: ${filename}`);

    try {
        const res = await fetch(`${URL_GET_UPLOAD_URL}?filename=${encodeURIComponent(filename)}`);
        if (!res.ok) {
            throw new Error(`Failed to get SAS URL: ${await res.text()}`);
        }

        const data = await res.json();
        const sasUrl = data.uploadUrl;
        console.log(`SAS URL received:\n${sasUrl}\n`);

        console.log(`Uploading ${filename}...`);

        const uploadRes = await fetch(sasUrl, {
            method: 'PUT',
            headers: {
                'x-ms-blob-type': 'BlockBlob'
            },
            body: file
        });

        console.log("Upload response:", uploadRes.status, uploadRes.statusText);

        if (uploadRes.status === 201) {
            console.log('Upload successful!');
        } else {
            const errText = await uploadRes.text();
            throw new Error(`Upload failed with status ${uploadRes.status}: ${errText}`);
        }

        return sasUrl;

    } catch (error) {
        console.error(`uploadFile() error:`, error.message);
        throw error; // ensures it fails visibly
    }
}




// Download file in the browser using a hidden anchor
async function downloadFile(jobId, type) {
    try {
        const params = new URLSearchParams({ job_id: jobId, type });
        const response = await fetch(`${URL_GET_DOWNLOAD_URL}?${params.toString()}`);

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Server error: ${errorText}`);
        }

        const data = await response.json();
        const downloadUrl = data.downloadUrl;
        console.log('SAS download URL:', downloadUrl);

        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = type; // Suggested filename
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    } catch (error) {
        console.error('Failed to get download URL:', error);
    }
}


async function mainProcess(jobId, videoSAS, imageSAS) {
    console.log("Calling main_process_func with:", {
        job_id: jobId,
        video_sas: videoSAS,
        image_sas: imageSAS
    });

    try {
        const response = await fetch(URL_MAIN_PROCESS, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                job_id: jobId,
                video_sas: videoSAS,
                image_sas: imageSAS
            })
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Processing failed: ${errText}`);
        }

        const data = await response.json();
        console.log("Processing started:", data.message || data);
    } catch (err) {
        alert(`Error in processing: ${err.message}`);
    }
}

async function checkProcessingStatus(jobId) {
    try {
        const response = await fetch(`${URL_CHECK_PROGRESS}?job_id=${jobId}`, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
            }
        });

        if (!response.ok) {
            throw new Error("Status check failed");
        }

        const data = await response.json();

        document.getElementById('statusValue').textContent = data.status_message;

        return data;

    } catch (err) {
        console.error("Status error:", err.message);
        return { progress_value: 0, done: false }; // avoid breaking UI
    }
}

async function cleanup(jobId) {
    try {
        const response = await fetch(`${URL_CLEANUP}?job_id=${jobId}`, {
            method: 'GET'
        });

        if (!response.ok) {
            throw new Error("Cleanup failed");
        }

    } catch (err) {
        console.error("cleanup error:", err.message);
    }
}



// Event listener for 'start' button
document.getElementById('startButton').addEventListener('click', () => {
    const videoFile = document.getElementById('video_path').files[0];
    const imageFile = document.getElementById('image_path').files[0];
    videoSAS = document.getElementById('video_sas').value.trim();
    imageSAS = document.getElementById('image_sas').value.trim();

    const progressBar = document.getElementById('progressBar');
    progressBar.value = 0;
    
    document.getElementById('statusValue').textContent = 'Uploading video and watermark';


    // Validate input
    const videoPresent = videoFile || videoSAS
    const imagePresent = imageFile || imageSAS

    if (!videoPresent || !imagePresent) {
        alert("Please provide both a video and an image.");
        return;
    }

    // if an image SAS or video SAS is not present, we need to upload it.
    // And we need to wait until the uploads complete.
    let videoUploadPromise = Promise.resolve(videoSAS);
    let imageUploadPromise = Promise.resolve(imageSAS);
    if (!videoSAS) {
        videoUploadPromise = uploadFile(videoFile);
    }
    if (!imageSAS) {
        imageUploadPromise = uploadFile(imageFile);
    }


    Promise.all([videoUploadPromise, imageUploadPromise])
        .then(([resolvedVideoSAS, resolvedImageSAS]) => {

        if (!resolvedVideoSAS || !resolvedImageSAS) {
            alert("Upload failed — missing video or image SAS URL.");
            console.error("Upload returned:", resolvedVideoSAS, resolvedImageSAS);
            return;
        }

        videoSAS = resolvedVideoSAS;
        imageSAS = resolvedImageSAS;

        console.log("Uploads complete:", videoSAS, imageSAS);

        const jobId = crypto.randomUUID();  // Generate a unique ID
        console.log("Starting job with ID:", jobId);

        mainProcess(jobId, videoSAS, imageSAS);

        const pollInterval = setInterval(async () => {
        const status = await checkProcessingStatus(jobId);
        progressBar.value = status.progress_value;
        const statusEl = document.getElementById("statusValue");
        if (statusEl) {
            statusEl.textContent = `${status.status_message || 'Unknown'}`;
        }

        if (status.done) {
            clearInterval(pollInterval);
            alert("Processing complete! You can now download your files.");

            const downloadVideoBtn = document.getElementById('downloadVideo');
            const downloadThumbnailBtn = document.getElementById('downloadThumbnail');

            downloadVideoBtn.onclick = () => downloadFile(jobId, 'output_video');
            downloadThumbnailBtn.onclick = () => downloadFile(jobId, 'output_thumbnail');

            downloadVideoBtn.style.display = 'inline';
            downloadThumbnailBtn.style.display = 'inline';

            cleanup(jobId);
        }
        }, 500);

    })

        .catch(error => {
            console.error("Upload failed:", error);
        });

});
