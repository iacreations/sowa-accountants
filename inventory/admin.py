from django.contrib import admin
from .models import Pclass, Category, Build, BuildLine
# Register your models here.

admin.site.register(Pclass)
admin.site.register(Category)


class BuildLineInline(admin.TabularInline):
    model = BuildLine
    extra = 1


@admin.register(Build)
class BuildAdmin(admin.ModelAdmin):
    list_display = ("id", "finished_product", "build_qty", "build_date", "status")
    list_filter = ("status",)
    inlines = [BuildLineInline]