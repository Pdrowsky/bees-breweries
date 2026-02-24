
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
