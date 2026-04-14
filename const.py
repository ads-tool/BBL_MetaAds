import pycountry

COUNTRY_MAPPING = {
    country.name: country.alpha_2
    for country in pycountry.countries
}

COUNTRY_MAPPING = {
    "Tất cả": "ALL",
    **COUNTRY_MAPPING
}

STATUS_MAPPING = {
    "Đang chạy": "ACTIVE",
    "Không hoạt động": "INACTIVE",
    "Tất cả": "ALL"
}