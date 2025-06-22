import cv2
import numpy as np
import subprocess
import imageio_ffmpeg as ffmpeg
import os
import tempfile
import math
from multiprocessing import Pool, cpu_count
import logging
import storage_functions

def process_video_chunk(job_id, video_path, watermark_path, chunk_id, alpha=0.5):
    video_capture = cv2.VideoCapture(video_path)
    if not video_capture.isOpened():
        print("Can't open input video.")
        return None
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"[ERROR] Video not found: {video_path}")
    
    # extract audio because this will be removed by the watermarking
    #audio_path = f"audio_{chunk_id}"
    #extract_audio(video_path, audio_path)

    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    video_fps = video_capture.get(cv2.CAP_PROP_FPS)
    frame_width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

    watermark_image = cv2.imread(watermark_path, cv2.IMREAD_UNCHANGED)
    if watermark_image is None:
        print("Can't load watermark image.")
        return None

    watermark_scale_ratio = 0.5
    watermark_width = int(frame_width * watermark_scale_ratio)
    watermark_aspect_ratio = watermark_image.shape[0] / watermark_image.shape[1]
    watermark_height = int(watermark_width * watermark_aspect_ratio)

    resized_watermark = cv2.resize(watermark_image, (watermark_width, watermark_height))

    if resized_watermark.shape[2] == 4:
        watermark_alpha = resized_watermark[:, :, 3] / 255.0
        watermark_rgb = resized_watermark[:, :, :3]
    else:
        watermark_alpha = np.ones((watermark_height, watermark_width))
        watermark_rgb = resized_watermark

    output_filename =  storage_functions._unique_filepath_tmp('mp4')
    video_codec = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter(output_filename, video_codec, video_fps, (frame_width, frame_height))

    while True:
        success, video_frame = video_capture.read()
        if not success:
            break

        x_offset = (frame_width - watermark_width) // 2
        y_offset = (frame_height - watermark_height) // 2
        roi = video_frame[y_offset:y_offset+watermark_height, x_offset:x_offset+watermark_width]

        for channel in range(3):
            roi[:, :, channel] = (
                roi[:, :, channel] * (1 - watermark_alpha * alpha) +
                watermark_rgb[:, :, channel] * (watermark_alpha * alpha)
            )

        video_frame[y_offset:y_offset+watermark_height, x_offset:x_offset+watermark_width] = roi
        video_writer.write(video_frame)

    video_capture.release()
    video_writer.release()


    # Combine audio and video again
    #combine_audio_video(output_filename, audio_path, output_filename)

    print(f"Watermarked chunk saved to: {output_filename}")
    return output_filename


def concat_chunks(chunk_paths, output_path):
    ffmpeg_executable = ffmpeg.get_ffmpeg_exe()

    # Create file list
    concat_list_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w")
    for path in chunk_paths:
        concat_list_file.write(f"file '{os.path.abspath(path)}'\n")
    concat_list_file.close()

    command = [
        ffmpeg_executable,
        "-loglevel", "error",
        "-f", "concat",
        "-safe", "0",
        "-i", concat_list_file.name,
        "-c", "copy",
        "-y",
        output_path
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove(concat_list_file.name)


def split_and_process_video(video_path, watermark_path, chunk_size=100):
    video_capture = cv2.VideoCapture(video_path)
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    video_capture.release()

    for i in range(0, total_frames, chunk_size):
        process_video_chunk(video_path, watermark_path, i, i + chunk_size, chunk_id=i)


def combine_audio_video(video_input_path, audio_input_path, output_path):


    ffmpeg_executable = ffmpeg.get_ffmpeg_exe()
    command = [
        ffmpeg_executable,
        "-loglevel", "error",
        "-i", video_input_path,
        "-i", audio_input_path,
        "-c:v", "copy",
        "-c:a", "aac",
        "-strict", "experimental",
        "-y",
        output_path
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Video with audio saved to:", output_path)





def extract_audio(video_input_path, audio_output_path):
    ffmpeg_executable = ffmpeg.get_ffmpeg_exe()
    command = [
        ffmpeg_executable,
        "-i", video_input_path,
        "-vn",
        "-acodec", "copy",
        "-y",
        audio_output_path
    ]

    print("Running command:", " ".join(command))
    
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        print("FFmpeg failed with error:")
        print(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command, output=result.stdout, stderr=result.stderr)

    print("Audio extracted to:", audio_output_path)

def process_thumbnail_chunk(args):
    video_path, frame_indices, thumb_height = args
    thumbnails = []

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)

    for frame_index in frame_indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        success, frame = cap.read()
        if not success:
            continue
        h, w = frame.shape[:2]
        new_w = int(thumb_height * w / h)
        resized = cv2.resize(frame, (new_w, thumb_height))
        thumbnails.append(resized)

    cap.release()
    return thumbnails

def extract_first_frame_chunk(args):
    video_path, start_frame, thumb_height = args
    cap = cv2.VideoCapture(video_path)
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    success, frame = cap.read()
    cap.release()

    if not success:
        return None

    h, w = frame.shape[:2]
    new_w = int(thumb_height * w / h)
    resized = cv2.resize(frame, (new_w, thumb_height))
    return resized

def generate_chunked_thumbnail_parallel(video_path, output_image_path, chunk_size=150, thumb_height=120, n_workers=4):
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.release()

    start_frames = list(range(0, total_frames, chunk_size))
    args = [(video_path, sf, thumb_height) for sf in start_frames]

    with Pool(processes=n_workers) as pool:
        thumbnails = pool.map(extract_first_frame_chunk, args)

    thumbnails = [thumb for thumb in thumbnails if thumb is not None]
    if not thumbnails:
        print("No thumbnails extracted.")
        return

    thumb_height, thumb_width = thumbnails[0].shape[:2]
    num_thumbs = len(thumbnails)
    grid_img = np.zeros((thumb_height, thumb_width * num_thumbs, 3), dtype=np.uint8)

    for idx, thumb in enumerate(thumbnails):
        x = idx * thumb_width
        grid_img[:, x:x + thumb.shape[1]] = thumb

    cv2.imwrite(output_image_path, grid_img)
    print("Thumbnail saved to:", output_image_path)


def extract_audio(video_path, audio_output_path):
    ffmpeg_executable = ffmpeg.get_ffmpeg_exe()

    command = [
        ffmpeg_executable,
        "-loglevel", "error",
        "-i", video_path,
        "-vn",
        "-acodec", "copy",
        "-y",
        audio_output_path
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)






