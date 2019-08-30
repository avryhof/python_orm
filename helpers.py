def get_val(target_object, key, default_value=None):
    if isinstance(target_object, dict):
        retn = target_object.get(key, default_value)
    else:
        try:
            retn = getattr(target_object, key)
        except AttributeError:
            retn = default_value
        else:
            if retn is None:
                retn = default_value

    return retn
