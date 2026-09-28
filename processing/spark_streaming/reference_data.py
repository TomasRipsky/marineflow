# =============================================================================
# MARINEFLOW — Shared reference data for the Silver jobs
# processing/spark_streaming/reference_data.py
#
# Lookups used by more than one job live here so the positions and metadata
# layers can never disagree (e.g. on a vessel's flag).
# =============================================================================

# Maritime Identification Digits (ITU-R M.585): the first three digits of a
# ship's MMSI identify its flag state. Values are ISO 3166-1 alpha-2 codes.
# Coverage is partial on purpose: an unlisted MID gives a null flag_country.
MID_MAP = {
    **dict.fromkeys(("211", "218"), "DE"),
    **dict.fromkeys(("219", "220"), "DK"),
    **dict.fromkeys(("224", "225"), "ES"),
    **dict.fromkeys(("226", "227", "228"), "FR"),
    **dict.fromkeys(("232", "233", "234", "235"), "GB"),
    **dict.fromkeys(("244", "245", "246"), "NL"),
    "247": "IT",
    **dict.fromkeys(("215", "229", "248", "249", "256"), "MT"),
    **dict.fromkeys(("204", "255", "263"), "PT"),
    **dict.fromkeys(("257", "258", "259"), "NO"),
    **dict.fromkeys(("265", "266"), "SE"),
    "269": "CH",
    "271": "TR",
    "273": "RU",
    "275": "LV",
    "276": "EE",
    "277": "LT",
    "278": "SI",
    **dict.fromkeys(("303", "338", "366", "367", "368", "369"), "US"),
    **dict.fromkeys(("412", "413", "414"), "CN"),
    "416": "TW",
    **dict.fromkeys(("431", "432"), "JP"),
    **dict.fromkeys(("440", "441"), "KR"),
    "477": "HK",
    "503": "AU",
    "512": "NZ",
    "518": "CK",
    **dict.fromkeys(("636", "637"), "LR"),
    "632": "GN",
    "657": "NG",
    "667": "SL",
    **dict.fromkeys(("674", "677"), "TZ"),
    "701": "AR",
    "710": "BR",
    "720": "BO",
}
