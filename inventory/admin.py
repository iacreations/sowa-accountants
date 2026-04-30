from django.contrib import admin
from .models import Pclass, Category, Build, BuildLine, BillOfMaterials, BOMItem


admin.site.register(Pclass)
admin.site.register(Category)


class BOMItemInline(admin.TabularInline):
    model = BOMItem
    extra = 1
    fields = ("component_item", "quantity_required", "unit_cost")


@admin.register(BillOfMaterials)
class BillOfMaterialsAdmin(admin.ModelAdmin):
    list_display = ("id", "finished_product", "version", "is_active", "total_cost", "created_at")
    list_filter = ("is_active",)
    search_fields = ("finished_product__name",)
    inlines = [BOMItemInline]


class BuildLineInline(admin.TabularInline):
    model = BuildLine
    extra = 1


@admin.register(Build)
class BuildAdmin(admin.ModelAdmin):
    list_display = (
        "assembly_number", "finished_product", "build_qty",
        "build_date", "status", "total_cost", "location",
    )
    list_filter = ("status",)
    search_fields = ("assembly_number", "finished_product__name")
    readonly_fields = ("assembly_number", "total_cost", "completed_at", "created_at", "updated_at")
    inlines = [BuildLineInline]
