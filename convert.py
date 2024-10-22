import os
import sys
import struct
import subprocess
import logging

# Formats corresponding to the codecs
formats = {
    0: "g729",
    1: "g726",
    2: "g726",
    3: "alaw",
    7: "mulaw",
    8: "g729",
    9: "g723_1",
    10: "g723_1",
    19: "g722"
}

def setup_logging(output_dir):
    logging.basicConfig(
        filename=os.path.join(output_dir, 'conversion.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_packet_header(data):
    "Get required information from packet header."
    return {
        "packet_type": struct.unpack("b", data[0:1])[0],
        "packet_subtype": struct.unpack("h", data[1:3])[0],
        "stream_id": struct.unpack("b", data[3:4])[0],
        "start_time": struct.unpack("d", data[4:12])[0],
        "end_time": struct.unpack("d", data[12:20])[0],
        "packet_size": struct.unpack("I", data[20:24])[0],
        "parameters_size": struct.unpack("I", data[24:28])[0]
    }

def get_compression_type(data):
    "Get compression type of the audio chunk."
    for i in range(0, len(data), 22):
        type_id = struct.unpack("h", data[i:i + 2])[0]
        data_size = struct.unpack("i", data[i + 2:i + 6])[0]
        temp = struct.unpack("16s", data[i + 6:i + 22])[0]
        if type_id == 10:
            return get_data_value(temp, data_size)

def get_data_value(data, data_size):
    '''The helper function to get the value of the data field from parameters header.'''
    fmt = "{}s".format(data_size)
    data_value = struct.unpack(fmt, data[0:data_size])
    if data_value == 0:
        data_value = struct.unpack(fmt, data[8:data_size])
    data_value = struct.unpack("b", data_value[0])
    return data_value[0]

def chunks_generator(path_to_file):
    "A python generator of the raw audio data."
    try:
        with open(path_to_file, "rb") as f:
            data = f.read()
    except IOError:
        sys.exit("No such file")
    
    packet_header_start = 0
    while True:
        packet_header_end = packet_header_start + 28
        headers = get_packet_header(data[packet_header_start:packet_header_end])

        if (headers["packet_type"] == 4 and headers["packet_subtype"] in [0, 3]) or (headers["packet_type"] == 5 and headers["packet_subtype"] == 300):
            chunk_start = packet_header_end + headers["parameters_size"]
            chunk_end = (chunk_start + headers["packet_size"] - headers["parameters_size"])
            chunk_length = chunk_end - chunk_start
            fmt = "{}s".format(chunk_length)
            raw_audio_chunk = struct.unpack(fmt, data[chunk_start:chunk_end])
            yield (get_compression_type(data[packet_header_end:packet_header_end + headers["parameters_size"]]), 
                   headers["stream_id"],
                   raw_audio_chunk[0])
        
        packet_header_start += headers["packet_size"] + 28
        if headers["packet_type"] == 7:
            break

def convert_to_mp3(path_to_file, output_dir):
    "Convert raw audio data using ffmpeg and subprocess."
    audio_chunks = {0: b'', 1: b''}  # Assuming 0 for caller and 1 for receiver

    for compression, stream_id, raw_audio_chunk in chunks_generator(path_to_file):
        if stream_id in audio_chunks:
            audio_chunks[stream_id] += raw_audio_chunk

    # Prepare a single output file
    output_file = os.path.join(output_dir, os.path.splitext(os.path.basename(path_to_file))[0] + "_combined.mp3")
    
    # Use ffmpeg to mix the audio streams together
    temp_file_caller = os.path.join(output_dir, "temp_caller.mp3")
    temp_file_receiver = os.path.join(output_dir, "temp_receiver.mp3")
    
    # Create temporary MP3 files for caller and receiver
    for stream_id in audio_chunks:
        format = formats[compression] if compression is not None else formats[0]
        process = subprocess.Popen(
            ("ffmpeg",
             "-hide_banner",
             "-y",
             "-f", format,
             "-i", "pipe:0",
             temp_file_caller if stream_id == 0 else temp_file_receiver),
            stdin=subprocess.PIPE
        )
        process.stdin.write(audio_chunks[stream_id])
        process.stdin.close()
        process.wait()

    # Mix both MP3 files
    subprocess.run([    
        "ffmpeg",
        "-y",
        "-i", temp_file_caller,
        "-i", temp_file_receiver,
        "-filter_complex", "[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2",
        output_file
    ])

    # Log the successful conversion
    logging.info(f"Converted {path_to_file} to {output_file}")
    print(f"Converted {path_to_file} to {output_file}")  # Notify in terminal

    # Clean up temporary files
    os.remove(temp_file_caller)
    os.remove(temp_file_receiver)

if __name__ == "__main__":
    try:
        source_base_folder = sys.argv[1]  # Source folder (e.g., D:\VOICE)
        output_base_folder = sys.argv[2]  # Base output folder (e.g., D:\OUTPUT_BASE_FOLDER)

        # Get the current date folder name (e.g., 20241016)
        date_folder = os.path.basename(source_base_folder)
        source_folder = os.path.join(source_base_folder, date_folder)

        # Ensure the base output folder exists
        if not os.path.exists(output_base_folder):
            os.makedirs(output_base_folder, exist_ok=True)

        # Setup logging for the output folder
        setup_logging(output_base_folder)

        # Process each hour folder under the current date folder
        for hour_folder in os.listdir(source_folder):
            hour_folder_path = os.path.join(source_folder, hour_folder)
            if os.path.isdir(hour_folder_path):
                for file_name in os.listdir(hour_folder_path):
                    if file_name.endswith('.nmf'):
                        path_to_file = os.path.join(hour_folder_path, file_name)

                        # Create the corresponding output folder structure
                        relative_path = os.path.relpath(hour_folder_path, source_base_folder)
                        output_dir = os.path.join(output_base_folder, relative_path)

                        # Ensure output directory exists
                        os.makedirs(output_dir, exist_ok=True)  # Create output folder structure if it doesn't exist

                        print(f"Starting conversion for: {path_to_file}")  # Notify in terminal
                        convert_to_mp3(path_to_file, output_dir)

    except IndexError:
        sys.exit("Please specify the source folder and output base folder")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(f"An error occurred: {e}")
