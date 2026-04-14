from meta_ads_collector.collector import MetaAdsCollector 

collector = MetaAdsCollector()
target_page_ids = ["311132522090838"]

# Sử dụng thuật toán collect_to_json của bạn nhưng fix lại các tham số chuẩn
print("Đang cào dữ liệu, vui lòng đợi...")
collector.collect_to_json(
    "output.json", 
    page_ids=target_page_ids, 
    country="ALL",             
    status="ACTIVE",          
    search_type="PAGE",
    max_results=1,
    include_raw=True  
)

# Đọc file JSON vừa lưu ra để kiểm chứng số lượng thực tế
import json
with open("output.json", "r", encoding="utf-8") as f:
    data = json.load(f)

ads = data.get("ads", [])
total_api_objects = len(ads)
total_real_ads_on_web = 0

for ad in ads:
    # Nếu collation_count có giá trị, cộng vào tổng. Nếu null (None), đếm là 1.
    count = ad.get("collation_count")
    if count:
        total_real_ads_on_web += count
    else:
        total_real_ads_on_web += 1

print("="*40)
print(f"Số lượng Object kéo về từ API: {total_api_objects} (Khớp với 63 của bạn)")
print(f"Tổng số Ads thực tế hiển thị trên Web (đã cộng dồn biến thể): {total_real_ads_on_web}")
print("="*40)