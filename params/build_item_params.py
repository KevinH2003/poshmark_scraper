import json

# --- Category Setup ---

categories = {
    # MEN
    "Men-Shoes": "shoes",
    "Men-Shirts": "shirts",
        "Men-Shirts-Casual_Button_Down_Shirts": "outerwear",
    "Men-Pants": "pants",
        "Men-Pants-Cargo": "pants",
    "Men-Shorts": "pants",
    "Men-Jeans": "pants",
    "Men-Suits_&_Blazers": "outerwear",
    "Men-Jackets_&_Coats": "outerwear",
    "Men-Sweaters": "outerwear",
    "Men-Accessories": "other",

    # WOMEN
    "Women-Tops": "shirts",
    "Women-Jeans": "pants",
    "Women-Pants_&_Jumpsuits": "pants",
    "Women-Jackets_&_Coats": "outerwear",
    "Women-Sweaters": "outerwear",
}

# --- Size Groups ---

sizes = {
    "men": {
        "shirts": ["S", "M"],
        "pants": ["26", "27", "28", "29", "30"],
        "outerwear": ["S", "M", "L", "XL"],
        "shoes": ["8", "8.5"],
        "other": []
    },
    "women": {
        "shirts": ["S", "M", "L"],
        "pants": ["4", "6", "26", "27", "28", "29"],
        "outerwear": ["M", "L", "XL"],
        "shoes": [],
        "other": []
    }
}

# --- Optional Custom Filters (can be expanded) ---

custom_colors = {
    "Men-Shoes": [],
    "Women-Shoes": []
}

custom_brands = {
    "Men-Shoes": [],
    "Women-Shoes": []
}

# --- Generate Configs ---

category_configs = []

for cat, sub_type in categories.items():
    is_men = "Men" in cat
    group = "men" if is_men else "women"
    color_list = custom_colors.get(cat, [])
    brand_list = custom_brands.get(cat, [])

    category_configs.append({
        "name": cat,
        "colors": color_list,
        "sizes": sizes[group][sub_type],
        "brands": brand_list
    })

# --- Final Config Output ---

config = {
    "categories": category_configs,
    "price_range": [0, 15],
    "price_step": 3,
    "sort_by": "like_count",
    "max_pages": 50,
    "max_workers": 20
}

with open("item_params_generated.json", "w") as f:
    json.dump(config, f, indent=2)

print(f"✅ Generated item_params_generated.json with {len(category_configs)} categories.")
