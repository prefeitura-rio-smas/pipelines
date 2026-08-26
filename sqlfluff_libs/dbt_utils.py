def generate_surrogate_key(field_list):
    cols = ", ".join(f"CAST({c} AS STRING)" for c in field_list)
    return f"TO_HEX(MD5(CONCAT({cols})))"
