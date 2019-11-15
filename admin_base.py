from external_db.models import Field


class CustomAdmin:
    model = None
    list_display = False
    raw_id_fields = ()
    search_fields = ()

    def model_name(self):
        retn = "No model specified."

        if self.model:
            retn = self.model.__name__

        return retn

    def fields(self, **kwargs):
        return_field_objects = kwargs.get("return_objects", False)

        object_names = self.list_display
        fields = []

        if not object_names:
            object_names = self.model.fields

        for object_name in object_names:
            if hasattr(self.model, object_name):
                obj = getattr(self.model, object_name)

                if return_field_objects:
                    fields.append(obj)
                else:
                    fields.append(object_name)

        return list(set(fields))

    def __str__(self):
        return "Admin module for %s" % self.model
