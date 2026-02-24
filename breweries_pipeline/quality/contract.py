
def validate_breweries_data(data: list):
    errors = {
        "data_type": False,
        "data_empty": False
    }

    if not isinstance(data, list):
        errors["data_type"] = True

    if data is None or len(data) == 0:
        errors["data_empty"] = True
    
    if errors["data_type"] or errors["data_empty"]:
        return False, errors
    else:
        return True, errors
