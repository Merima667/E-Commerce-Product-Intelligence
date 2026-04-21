import logging
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
 
logger = logging.getLogger(__name__)
 
def extract_exif(path):
    img = Image.open(path)
    exif_data = img.getexif()
    if not exif_data:
        logger.warning(f'No EXIF data found in {path}')
        return {}
    result = {}
    for tag_id, value in exif_data.items():
        tag_name = TAGS.get(tag_id, str(tag_id))
        # Convert bytes to string for MongoDB serialisation
        if isinstance(value, bytes):
            value = value.decode('utf-8', errors='replace')
        result[tag_name] = value
    return result
 
def extract_gps(path):
    img = Image.open(path)
    exif_data = img.getexif()
    if not exif_data:
        return None
    gps_ifd = exif_data.get_ifd(0x8825)   # GPSInfo sub-IFD
    if not gps_ifd:
        return None
    gps = {}
    for key, val in gps_ifd.items():
        gps[GPSTAGS.get(key, key)] = val
    return gps
 
def get_exif_summary(path):
    exif = extract_exif(path)
    gps  = extract_gps(path)
    return {
        'camera_make':  exif.get('Make'),
        'camera_model': exif.get('Model'),
        'date_taken':   exif.get('DateTimeOriginal') or exif.get('DateTime'),
        'exposure':     str(exif.get('ExposureTime')),
        'aperture':     str(exif.get('FNumber')),
        'iso':          exif.get('ISOSpeedRatings'),
        'focal_length': str(exif.get('FocalLength')),
        'orientation':  exif.get('Orientation'),
        'gps':          gps,
    }
 
def strip_exif(path, output_path):
    img = Image.open(path)
    clean = Image.new(img.mode, img.size)
    clean.putdata(list(img.getdata()))
    clean.save(output_path)
    logger.info(f'EXIF stripped: {output_path}')
    return output_path

if __name__ == "__main__":
    import os
    from pathlib import Path
    
    for sample in ["../../data/raw/exif_samples/sample1.jpeg", "../../data/raw/exif_samples/sample2.jpeg"]:
        print(f"\n=== {sample} ===")
        summary = get_exif_summary(sample)
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        output = sample.replace("exif_samples", "processed/resized").replace(".jpeg", "_clean.jpeg")
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        strip_exif(sample, output)
        print("Clean copy saved!")