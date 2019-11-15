import importlib

from django.shortcuts import render
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_protect
from django.views.generic import TemplateView

from external_db.model_helpers import get_model, get_admin_model, get_app_details


class CustomAdminAppView(TemplateView):
    extra_css = []
    extra_javascript = []
    template_name = "custom-admin/app.html"
    name = "Custom Admin App View"

    request = None

    def get_context_data(self, **kwargs):
        context = super(CustomAdminAppView, self).get_context_data(**kwargs)
        context["page_title"] = self.name
        context["extra_css"] = self.extra_css
        context["extra_javascript"] = self.extra_javascript
        context["request"] = self.request

        return context

    def get(self, request, *args, **kwargs):
        self.request = request
        context = self.get_context_data()

        app_name = kwargs.get("app_name")

        app = get_app_details(app_name)
        app_models = get_model(app_name)

        models = []
        for app_model in app_models:
            if app_model:
                model_name = app_model.__name__

                admin_url = reverse("custom_admin_model", kwargs=dict(app_name=app_name, model_name=model_name))
                models.append(dict(
                    object_name=model_name,
                    name=model_name.title(),
                    admin_url=admin_url,
                    add_url=admin_url,
                    edit_url=admin_url
                ))

        app['models'] = models
        context['app'] = app

        context['name'] = app_name

        return render(request, self.template_name, context)

    @method_decorator(csrf_protect)
    def dispatch(self, *args, **kwargs):
        return super(CustomAdminAppView, self).dispatch(*args, **kwargs)


class CustomAdminModelView(TemplateView):
    extra_css = []
    extra_javascript = []
    template_name = "custom-admin/model.html"
    name = "Custom Admin App View"

    request = None

    def get_context_data(self, **kwargs):
        context = super(CustomAdminModelView, self).get_context_data(**kwargs)
        context["page_title"] = self.name
        context["extra_css"] = self.extra_css
        context["extra_javascript"] = self.extra_javascript
        context["request"] = self.request

        return context

    def get(self, request, *args, **kwargs):
        self.request = request
        context = self.get_context_data()

        app_name = kwargs.get("app_name")
        model_name = kwargs.get("model_name")

        app = get_app_details(app_name)

        admin_model = get_admin_model(app_name, model_name)
        model = admin_model.model

        columns = admin_model.fields()

        results = model(debug=True).objects.all(result_limit=30)

        context['app'] = app
        context['model'] = model_name
        context['columns'] = columns
        context['results'] = results

        return render(request, self.template_name, context)

    @method_decorator(csrf_protect)
    def dispatch(self, *args, **kwargs):
        return super(CustomAdminModelView, self).dispatch(*args, **kwargs)
