import json,os,gzip

#load files

def load_files(file_path):
    files_pages = []
    if not os.path.exists(file_path):
        return []
    for files in os.listdir(file_path):
        fullpath = os.path.join(file_path, files)
        try:
            with gzip.open(fullpath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    files_pages.append(data)
                elif isinstance(data,list):
                    files_pages.extend(data)
        except Exception as e:
            print("Error in file:", files, e)
    return files_pages

#parser function
def parser(pages):
    page=[]
    for data in pages:

        merchant = data.get("merchant") or {}
        if not merchant:
            continue
        merchant_id = merchant.get("ID")
        name = merchant.get("name")

        if not merchant_id or not name:  #if merchant id, name is not there, it continue
            continue

        # Main dictionary
        result = {
            "merchant_id": merchant.get("ID"),
            "name": merchant.get("name"),
            "cuisine": merchant.get("cuisine"),
            "timingEveryday":merchant.get("openingHours",{}).get("sun"),
            "distance": merchant.get("distanceInKm"),
            "ETA": merchant.get("ETA"),
            "rating": merchant.get("rating"),
            "DeliveryBy":merchant.get("deliverBy"),
            "DeliveryOption":merchant.get("deliveryOptions"),
            "VoteCount": merchant.get("voteCount"),
            "Tips": [merchant.get("sofConfiguration",{}).get("tips")],
            "BuisinessType":merchant.get("businessType"),
            "Offers":[],
            "menu": []
        }
        for offers in merchant.get("offerCarousel",{}).get("offerHighlights",[]):
            off={
                    "Title": offers.get("highlight").get("title"),
                    "SubTitle":offers.get("highlight").get("subtitle")
            }
            result["Offers"].append(off)

        # Build Menu List Category Wise
        for category in merchant.get("menu", {}).get("categories", []):

            category_block = {
                "category_name": category.get("name"),
                "items": []
            }

            for item in category.get("items", []):

                item_block = {
                    "item_id": item.get("ID"),
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "price_display": float(item.get("priceV2", {}).get("amountDisplay")),
                    "available": True if item.get("available") else False,
                    "images": item.get("imgHref"),
                }

                category_block["items"].append(item_block)

            result["menu"].append(category_block)
        result = {k: v for k, v in result.items() if v is not None}
        page.append(result)
    return page

