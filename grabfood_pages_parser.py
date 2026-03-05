#parser function
def parser(pages):
    page=[]
    for data in pages:

        merchant = data.get("merchant")
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
                raw_price = item.get("priceV2", {}).get("amountDisplay")

                if raw_price:
                    try:
                        clean_price = float(str(raw_price).replace(",", ""))
                    except ValueError:
                        clean_price = None
                else:
                    clean_price = None
                item_block = {
                    "item_id": item.get("ID"),
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "price_display": clean_price,
                    "available": True if item.get("available") else False,
                    "images": item.get("imgHref"),
                }

                category_block["items"].append(item_block)

            result["menu"].append(category_block)
        page.append(result)
    return page
