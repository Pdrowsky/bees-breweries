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
        "data_empty": False
    }

    if not isinstance(data, list):
        errors["data_type"] = True
        return False, errors

    if len(data) == 0:
        errors["data_empty"] = True
        return False, errors
    
    return True, errors
