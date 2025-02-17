import chardet

def detect_csv_encoding(file_path, sample_size=1024):
    with open(file_path, "rb") as f:
        raw_data = f.read(sample_size)
    result = chardet.detect(raw_data)
    return result['encoding']

# Example Usage
file_path = "hb.csv"  # Replace with your actual file path
encoding = detect_csv_encoding(file_path)
print(f"Detected Encoding: {encoding}")

