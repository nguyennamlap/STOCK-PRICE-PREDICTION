import pandas as pd
import os

tickers = [
    "VNM", "FPT", "HPG", "VIC", "VHM", "VCB", "CTG", "BID", "TCB",
    "VPB", "GAS", "PLX", "MWG", "PDR", "BHC", "SAB", "MSN", "KBC"
]

for t in tickers:
    raw_path = f'./raw_data/{t}.csv'
    clean_path = f'./data/{t}.csv'

    if not os.path.exists(raw_path):
        print(f"File không tồn tại: {raw_path}")
        continue

    try:
        df = pd.read_csv(raw_path)

        df = df.drop_duplicates(subset=['Ngày'], keep='first')
        df = df.replace(['--', '-'], pd.NA)
        df = df.dropna(thresh=4)
        a = 'Giá mở cửa,Giá cao nhất,Giá thấp nhất,Giá đóng cửa,Khối lượng'.split(',')
        for col in a:
            df[col] = df[col].astype(str).str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['% thay đổi'] = df['% thay đổi'].astype(str).str.replace('%', '')
        df['% thay đổi'] = pd.to_numeric(df['% thay đổi'], errors='coerce')
        df['% thay đổi'] = df['% thay đổi'] / 100.0
        df.to_csv(clean_path, index=False)
        print(f"Cleaned: {t}")

    except Exception as e:
        print(f"Lỗi khi xử lý {t}: {e}")
