from django.urls import path

from external_db.views import CustomAdminAppView, CustomAdminModelView

urlpatterns = [
    path("<str:app_name>/", CustomAdminAppView.as_view(), name="custom_admin_app"),
    path("<str:app_name>/<str:model_name>/", CustomAdminModelView.as_view(), name="custom_admin_model"),
]