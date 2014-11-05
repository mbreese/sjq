def convert_mem_val(val):
    try:
        if val[-1].upper() == 'G':
            return int(val[:-1]) * 1024 * 1024 * 1024
        if val[-1].upper() == 'M':
            return int(val[:-1]) * 1024 * 1024
        if val[-1].upper() == 'K':
            return int(val[:-1]) * 1024
    except:
        pass

    return 0
