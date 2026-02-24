# - Data contract validation module -
#
# Validates data against expectations
#
# Returns validation results and error details, if any

expected_brewery_fields = [
    "id",
    "name",
    "brewery_type",
    "city",
    "country",
    "state",
]

def validate_breweries_data(data: list):
    errors = {
        "data_type": False,
        "data_empty": False,
        "missing_fields_indexes": []
    }

    if not isinstance(data, list):
        errors["data_type"] = True
        # early return, can't proceed with further checks on invalid data type
        return False, errors

    if len(data) == 0:
        errors["data_empty"] = True
        # early return, can't check if there are no records
        return False, errors
    
    for idx, brewery in enumerate(data):
        for field in expected_brewery_fields:
            if field not in brewery:
                errors["missing_fields_indexes"].append(idx)
                break # no need to check other fields if one is missing
    
    return True, errors
