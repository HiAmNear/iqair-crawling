import pandas as pd
import os
import glob # Thư viện để tìm file theo pattern

# --- Tự động xác định đường dẫn ---
# Lấy đường dẫn tuyệt đối đến thư mục chứa file script này (thư mục 'scripts')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Đi lùi một cấp để lấy đường dẫn thư mục gốc của dự án
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Đường dẫn đến thư mục 'result'
CSV_PATH = os.path.join(PROJECT_ROOT, 'result')
# ------------------------------------


CITY_LIST = ['can-tho','da-nang','hai-phong',
             'hanoi','ho-chi-minh-city','hue',
             'nha-trang','vinh']

# Tên file output bạn muốn
OUTPUT_FILE = 'merged_all_aqi_data.csv'

# 1. Tạo một list rỗng để chứa tất cả các DataFrame
all_data_frames = []

print(f"Đường dẫn thư mục kết quả (CSV_PATH): {CSV_PATH}")
print("Bắt đầu quá trình gộp file...")

# 2. Lặp qua từng thành phố trong CITY_LIST
for city in CITY_LIST:
    # Tạo đường dẫn đến thư mục của thành phố
    city_folder_path = os.path.join(CSV_PATH, city)
    
    # Tạo pattern để tìm tất cả các file .csv trong thư mục đó
    search_pattern = os.path.join(city_folder_path, "*.csv")
    
    # Lấy danh sách tất cả các file CSV cho thành phố hiện tại
    csv_files_for_city = glob.glob(search_pattern)
    
    if not csv_files_for_city:
        print(f"(!) Không tìm thấy file CSV nào cho thành phố: {city}")
        continue

    print(f"Đang xử lý {len(csv_files_for_city)} file cho thành phố: {city}...")

    # 3. Lặp qua từng file CSV tìm được
    for file_path in csv_files_for_city:
        try:
            # Đọc file CSV
            df = pd.read_csv(file_path)
            
            # (Rất quan trọng) Thêm cột 'city' để phân biệt dữ liệu
            df['city'] = city 
            
            # Thêm DataFrame này vào list
            all_data_frames.append(df)
        except Exception as e:
            print(f"(!) Lỗi khi đọc file {file_path}: {e}")

# 4. Kiểm tra xem có dữ liệu để gộp không
if not all_data_frames:
    print("Không có dữ liệu nào được đọc, kết thúc.")
else:
    # Gộp tất cả các DataFrame trong list lại thành 1 DataFrame duy nhất
    print("\nĐang gộp tất cả dữ liệu...")
    merged_df = pd.concat(all_data_frames, ignore_index=True)

    # =================================================================
    # === PHẦN MỚI: TÁCH CỘT TIMESTAMP ===
    # =================================================================
    print("Đang xử lý cột 'timestamp' để tách ngày, giờ...")
    try:
        # 1. Chuyển đổi cột 'timestamp' sang định dạng datetime
        # Thêm utc=True để xử lý múi giờ, sau đó chuyển về +07:00
        # Hoặc cách đơn giản hơn là để pandas tự nhận diện:
        merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
        
        # 2. Tạo các cột mới từ cột timestamp đã chuyển đổi
        merged_df['year'] = merged_df['timestamp'].dt.year
        merged_df['month'] = merged_df['timestamp'].dt.month
        merged_df['day'] = merged_df['timestamp'].dt.day
        merged_df['hour'] = merged_df['timestamp'].dt.hour
        
        print("   -> Đã tách xong year, month, day, hour.")
        
    except Exception as e:
        print(f"   (!) Lỗi khi xử lý cột 'timestamp': {e}. Sẽ bỏ qua bước này.")
    # =================================================================
    # === KẾT THÚC PHẦN MỚI ===
    # =================================================================

    # =================================================================
    # === PHẦN MỚI: SẮP XẾP LẠI THỨ TỰ CỘT ===
    # =================================================================
    print("Đang sắp xếp lại thứ tự cột...")
    try:
        # Lấy danh sách TẤT CẢ các cột hiện có
        all_cols = merged_df.columns.tolist()
        
        # Xác định các cột mới bạn muốn di chuyển
        new_date_cols = ['year', 'month', 'day', 'hour']
        
        # Lấy danh sách các cột CŨ (loại bỏ 'timestamp' và các cột ngày mới)
        # để đảm bảo chúng ta giữ lại mọi thứ khác (như aqi, city, v.v.)
        original_other_cols = [col for col in all_cols if col not in ['timestamp'] + new_date_cols]
        
        # Tạo thứ tự mới: 
        # ['timestamp'] + ['year', 'month', 'day', 'hour'] + [các cột còn lại]
        new_order = ['timestamp'] + new_date_cols + original_other_cols
        
        # Áp dụng thứ tự mới cho DataFrame
        merged_df = merged_df[new_order]
        print("   -> Đã sắp xếp lại cột thành công.")
    except Exception as e:
        print(f"   (!) Lỗi khi sắp xếp cột: {e}")
    # =================================================================
    # === KẾT THÚC PHẦN SẮP XẾP ===
    # =================================================================
    # 5. Lưu file gộp ra thư mục CSV_PATH (thư mục 'result')
    output_path = os.path.join(CSV_PATH, OUTPUT_FILE)
    
    print(f"\nĐang lưu file gộp tại: {output_path}...")
    # index=False để không lưu cột index của pandas vào file CSV
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("=====================================================")
    print(f"✅ HOÀN TẤT!")
    print(f"Đã gộp thành công {len(all_data_frames)} file.")
    print(f"Tổng số dòng trong file gộp: {len(merged_df)}")
    print(f"File output đã được lưu tại: {output_path}")
    print("=====================================================")
    
    # Hiển thị 5 dòng đầu tiên của dữ liệu đã gộp
    print("\n5 dòng đầu tiên của file gộp (đã bao gồm các cột mới):")
    
    # Kiểm tra xem các cột mới đã được tạo chưa
    cols_to_show = ['timestamp', 'city']
    if 'year' in merged_df.columns:
        cols_to_show.extend(['year', 'month', 'day', 'hour'])
        
    print(merged_df[cols_to_show].head())