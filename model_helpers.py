import importlib

from django.urls import reverse

from external_db.admin_base import CustomAdmin


def get_app_details(app_name):
    custom_app = importlib.import_module(app_name)

    app_name = custom_app.__name__
    app_url = reverse("custom_admin_app", kwargs=dict(app_name=app_name))

    return dict(name=app_name, app_label=app_name.title(), app_url=app_url, models=[])


def get_admin_model(app_name, model_name=False):
    models = []

    try:
        import_string = "%s.external_admin" % app_name
        app_models = importlib.import_module(import_string)

        for app_model_name in dir(app_models):
            app_admin_model_attr = getattr(app_models, app_model_name)

            if callable(app_admin_model_attr):
                app_admin_model = app_admin_model_attr()

                if hasattr(app_admin_model, "model"):
                    if model_name and app_admin_model.model_name().lower() == model_name.lower():
                        return app_admin_model

                    else:
                        models.append(app_admin_model)

    except ModuleNotFoundError:
        pass

    return models


def get_model(app_name, model_name=False):
    models = []

    app_admin_models = get_admin_model(app_name, model_name)

    if len(app_admin_models) == 1:
        return app_admin_models[0].model

    else:
        for app_admin_model in app_admin_models:
            models.append(app_admin_model.model)

    return models
