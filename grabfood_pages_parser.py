def parser(pages):

    results = []

    for data in pages:

        merchant = data.get("merchant")
        if not merchant:
            continue

        merchant_id = merchant.get("ID")
        name = merchant.get("name")

        if not merchant_id or not name:
            continue

        opening = merchant.get("openingHours") or {}
        sof = merchant.get("sofConfiguration") or {}
        offers_block = merchant.get("offerCarousel") or {}
        menu_block = merchant.get("menu") or {}

        result = {
            "merchant_id": merchant_id,
            "name": name,
            "cuisine": merchant.get("cuisine"),
            "timingEveryday": opening.get("sun"),
            "distance": merchant.get("distanceInKm"),
            "ETA": merchant.get("ETA"),
            "rating": merchant.get("rating"),
            "DeliveryBy": merchant.get("deliverBy"),
            "DeliveryOption": merchant.get("deliveryOptions"),
            "VoteCount": merchant.get("voteCount"),
            "Tips": [sof.get("tips")],
            "BuisinessType": merchant.get("businessType"),
            "Offers": [],
            "menu": []
        }

        # OFFERS
        offer_highlights = offers_block.get("offerHighlights") or []
        for offer in offer_highlights:
            highlight = offer.get("highlight") or {}
            result["Offers"].append({
                "Title": highlight.get("title"),
                "SubTitle": highlight.get("subtitle")
            })

        # MENU
        categories = menu_block.get("categories") or []

        for category in categories:

            category_block = {
                "category_name": category.get("name"),
                "items": []
            }

            items = category.get("items") or []

            for item in items:

                price_display = None

                raw_price = item.get("priceV2", {}).get("amountDisplay")
                if raw_price:
                    try:
                        price_display = float(raw_price.replace(",", ""))
                    except Exception:
                        pass

                category_block["items"].append({
                    "item_id": item.get("ID"),
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "price_display": price_display,
                    "available": bool(item.get("available")),
                    "images": item.get("imgHref")
                })

            result["menu"].append(category_block)

        results.append(result)

    return results
