
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import requests
import json

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

driver = webdriver.Chrome(options=opts)
n = 50  # Số trang muốn lấy dữ liệu
b = ["VNM",
    "FPT", 
    "HPG", "VIC", "VHM", "VCB", "CTG", "BID", "TCB", "VPB", "GAS", "PLX", "MWG", "PDR", "BHC", "SAB", "MSN", "KBC"]

for i in b:
    # Truy cập trang lịch sử giá cổ phiếu
    driver.get(f"https://simplize.vn/co-phieu/{i}/lich-su-gia")
    table = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    print(f"Đã tải trang thành công cho mã cổ phiếu {i} 🤩")
    # Lấy số trang từ phần tử pagination
    for page in range(n): 
        page1 = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, f"//li[contains(@class,'simplize-pagination-item')]//a[text()='{page+1}']"))
        )
        driver.execute_script("arguments[0].click();", page1)
        print(f"Đang ở trang {page+1} 😸")
     # Lấy bảng dữ liệu
        try:
            print(f"Đã tải trang thành công cho mã cổ phiếu {i} 🤩")
            print("Bắt đầu lấy tiêu đề cột 🥳")
            # Lấy tiêu đề cột (thread)
            thead = table.find_element(By.TAG_NAME, "thead")
            # Lấy tất cả các th trong thead 
            headers = [th.text for th in thead.find_elements(By.TAG_NAME, "th")]
            print("Đã lấy được các cột tiêu đề 🫵")
            print(f"Tiêu đề cột 😼: {headers}")

            print("Bắt đầu lấy dữ liệu các dòng dữ liệu 🥳")
            tbody = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
            )
            # Lấy tất cả các tr trong tbody
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            print(f"Đã lấy được {len(rows)} dòng dữ liệu 🙂‍↔️")

            data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")

                row_data = []
                for c in cells:
                    h6s = c.find_elements(By.TAG_NAME, "h6")
                    if h6s:
                        row_data.append(h6s[0].text)
                    else:
                        row_data.append(c.text)

                data.append(row_data)

            print(f"Dữ liệu {i} 🙄:")
            for r in data:
                print(f"🥴 {r}")

            payload = {
            "symbol": i,
            "page": page+1,
            "headers": headers,
            "data": data}
            print(f"Dữ liệu payload {i} 🙄:")
            res = requests.post(
                "http://fastapi-service.kafka-namespace.svc.cluster.local:8088/push",
                json=payload,
                timeout=5
            )
            print("Kafka Push:", res.text)

        except Exception as e:
            print("Lỗi 😨:", e)
        time.sleep(2)

driver.quit()
print("Hoàn thành việc crawl dữ liệu tất cả các mã cổ phiếu! 🥳🎉")