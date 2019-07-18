def secure_import(module_name):
    import importlib
    try:
        module = importlib.import_module(module_name)
    except:
        import pip
        if hasattr(pip, 'main'):
            pip.main(['install', module_name])
        else:
            pip._internal.main(['install', module_name])
        module = importlib.import_module(module_name)
    return module