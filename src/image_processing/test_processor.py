import logging
logging.basicConfig(level=logging.INFO)
from processor import inspect_image, resize_image, resize_proportional, generate_thumbnail, generate_fixed_thumbnail, crop_image, crop_top_banner, crop_center_square, convert_to_webp, convert_to_grayscale, save_optimised_jpeg, apply_blur, apply_sharpen, apply_edge_detection, enhance_contrast, enhance_brightness, enhance_color

# Part 3 - Inspect Image
result = inspect_image("../../data/raw/images/B09SM24S8C.jpg")
for key, value in result.items():
    print(f"{key}: {value}")

# Part 4 - Resize Images
resize_image(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/resized/B09SM24S8C_resized.jpg",
    width=300, height=400
)
print("resize_image done!")

resize_proportional(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/resized/B09SM24S8C_proportional.jpg",
    max_width=300
)
print("resize_proportional done!")

# Part 5 - Thumbnails
generate_thumbnail(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/thumbnails/B09SM24S8C_thumb.jpg"
)
print("generate_thumbnail done!")

generate_fixed_thumbnail(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/thumbnails/B09SM24S8C_fixed_thumb.jpg",
    size=(128, 128),
    method='pad'
)
print("generate_fixed_thumbnail done!")

# Part 6 - Crop Images
crop_top_banner(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/cropped/B09SM24S8C_banner.jpg"
)
print("crop_top_banner done!")

crop_center_square(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/cropped/B09SM24S8C_square.jpg"
)
print("crop_center_square done!")

crop_image(
    "../../data/raw/images/B09SM24S8C.jpg",
    "../../data/processed/cropped/B09SM24S8C_custom.jpg",
    box=(50, 50, 290, 350)
)
print("crop_image done!")

# Part 7 - Format Conversion
convert_to_webp("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/webp/B09SM24S8C.webp", quality=85)
print("WebP image generated at data/processed/webp/B09SM24S8C.webp")

convert_to_grayscale("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_grayscale.jpg")
print("Grayscale image generated at data/processed/resized/B09SM24S8C_grayscale.jpg")

save_optimised_jpeg("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_optimised.jpg", quality=85)
print("Optimized JPEG generated at data/processed/resized/B09SM24S8C_optimised.jpg")

# Part 8 - Filters
apply_blur("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_blur.jpg", radius=2)
print("apply_blur done!")

apply_sharpen("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_sharpen.jpg")
print("apply_sharpen done!")

apply_edge_detection("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_edges.jpg")
print("apply_edge_detection done!")

# Part 8 - Enhancements
enhance_contrast("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_contrast.jpg", factor=1.5)
print("enhance_contrast done!")

enhance_brightness("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_brightness.jpg", factor=1.2)
print("enhance_brightness done!")

enhance_color("../../data/raw/images/B09SM24S8C.jpg", "../../data/processed/resized/B09SM24S8C_color.jpg", factor=1.3)
print("enhance_color done!")