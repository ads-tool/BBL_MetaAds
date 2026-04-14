from meta_ads_collector.collector import MetaAdsCollector 
import json

collector = MetaAdsCollector()
target_page_ids = ["311132522090838"]

print("🚀 Bắt đầu cào dữ liệu từ Meta Ad Library...")
print("Quá trình này có thể mất vài phút vì đang quét sâu vào từng Thẻ con, vui lòng đợi...\n")

# Chạy thu thập dữ liệu
collector.collect_to_json(
    "output.json", 
    page_ids=target_page_ids, 
    country="ALL", 
    max_results=None,
    status="ACTIVE",
    search_type="PAGE"
)

# Phân tích file JSON vừa lưu để in thống kê
try:
    with open("output.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    ads = data.get("ads", [])
    total_parent = len(ads)
    total_children = sum(len(ad.get("creatives", [])) for ad in ads)

    print("\n" + "="*50)
    print("🎉 HOÀN TẤT CÀO DỮ LIỆU!")
    print(f"👉 Tổng số Thẻ Cha (API Objects): {total_parent}")
    print(f"👉 Tổng số Thẻ Con thực tế trong mảng creatives: {total_children}")
    print("="*50)

except Exception as e:
    print(f"Có lỗi khi đọc file output.json: {e}")