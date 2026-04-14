from meta_ads_collector.client import MetaAdsClient
import json

# Khởi tạo Client
client = MetaAdsClient()
client.initialize()

# Truyền ID của "Thẻ con" và ID của Page (BẮT BUỘC CÓ)
the_con_id = "917102411009753"
page_id = "311132522090838"

print(f"Đang bypass client.py để lấy raw data cho thẻ: {the_con_id}...")

# 1. Tự build biến chính xác, sửa VN thành ALL
variables = {
    "adArchiveID": the_con_id,
    "pageID": page_id,
    "country": "ALL",  # Đã fix lỗi hardcode VN
    "sessionID": client._tokens.get("__hsi", ""),
    "source": None,
    "isAdNonPolitical": True,
    "isAdNotAAAEligible": False
}

# 2. Đóng gói Payload với Doc ID
payload = client._build_graphql_payload(
    doc_id="25068828942793558", 
    variables=variables,
    friendly_name="AdLibraryAdDetailsQuery",
)

# 3. Kẹp Header và bắn Request
headers = dict(client._fingerprint.get_graphql_headers())
headers["x-fb-friendly-name"] = "AdLibraryAdDetailsQuery"
headers["x-fb-lsd"] = client._tokens.get("lsd", "")

try:
    response = client._make_graphql_request(payload, headers)
    
    print("\n" + "="*50)
    print("RAW JSON TRẢ VỀ TỪ MÁY CHỦ META:")
    print("="*50)
    
    # Xử lý chuỗi an toàn của Meta (for (;;);)
    text = response.text
    if text.startswith("for (;;);"):
        text = text[9:]
        
    # In ra toàn bộ cấu trúc gốc
    raw_data = json.loads(text)
    print(json.dumps(raw_data, indent=2, ensure_ascii=False))
    print("="*50)
    
except Exception as e:
    print(f"Lỗi kết nối: {e}")