
expected_brewery_fields = [
    "id",
    "name"
]

def validate_breweries_fields(data: list):
    errors = {
        "missing_fields_indexes": []
    }

    for idx, brewery in enumerate(data):
        for field in expected_brewery_fields:
            if field not in brewery:
                errors["missing_fields_indexes"].append(idx)

    return errors