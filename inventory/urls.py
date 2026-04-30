from django.urls import path
from . import views


app_name='inventory'
# my urls
urlpatterns = [  
# inventory urls
    path('inventory/add/products', views.add_products, name='add-products'),
    path("products/<int:pk>/", views.product_detail, name="product-detail"),
    path("products/", views.inventory_products_list, name="products-list"),
    path("movements/", views.inventory_movements_list, name="movements-list"),
    path("products/<int:pk>/edit/", views.product_edit, name="product-edit"),
    path("add-category-ajax/", views.add_category_ajax, name="add_category_ajax"),
    path("add-class-ajax/", views.add_class_ajax, name="add_class_ajax"),
    path("suppliers/list/", views.suppliers_list_json, name="suppliers-list-json"),
    # stock movement
    path("stock-transfers/", views.stock_transfer_list, name="stock-transfer-list"),
    path("stock-transfers/new/", views.add_stock_transfer, name="add-stock-transfer"),
    path("stock-transfers/<int:pk>/", views.stock_transfer_detail, name="stock-transfer-detail"),
    path("locations/", views.locations_list_json, name="locations-list-json"),
    path("locations/create/", views.location_create_json, name="location-create-json"),
    path("locations/add-ajax/", views.add_location_ajax, name="add_location_ajax"),

    # Bill of Materials (BOM)
    path("boms/", views.bom_list, name="bom-list"),
    path("boms/new/", views.bom_create, name="bom-create"),
    path("boms/<int:pk>/", views.bom_detail, name="bom-detail"),
    path("boms/<int:pk>/delete/", views.bom_delete, name="bom-delete"),

    # Assembly builds
    path("builds/", views.build_list, name="build-list"),
    path("builds/new/", views.add_build, name="add-build"),
    path("builds/<int:pk>/", views.build_detail, name="build-detail"),
    path("builds/<int:pk>/complete/", views.complete_build_view, name="complete-build"),
    path("builds/<int:pk>/cancel/", views.cancel_build_view, name="cancel-build"),
    path("builds/<int:pk>/reverse/", views.reverse_build_view, name="reverse-build"),

    # Assembly import / export
    path("builds/export/csv/", views.assembly_export_csv, name="assembly-export-csv"),
    path("builds/import/csv/", views.assembly_import_csv, name="assembly-import-csv"),

    # Reports
    path("reports/movements/", views.report_movement_ledger, name="report-movement-ledger"),
    path("reports/valuation/", views.report_stock_valuation, name="report-stock-valuation"),
    path("reports/aging/", views.report_stock_aging, name="report-stock-aging"),
    path("reports/reorder/", views.report_reorder, name="report-reorder"),
    path("reports/expiry/", views.report_expiry, name="report-expiry"),
    path("reports/assembly/", views.report_assembly, name="report-assembly"),
    path("reports/component-consumption/", views.report_component_consumption, name="report-component-consumption"),
    path("reports/wip/", views.report_wip, name="report-wip"),

    # Stock Adjustments
    path("adjustments/", views.stock_adjustment_list, name="stock-adjustment-list"),
    path("adjustments/new/", views.stock_adjustment_create, name="stock-adjustment-create"),
    path("adjustments/<int:pk>/", views.stock_adjustment_detail, name="stock-adjustment-detail"),
    path("adjustments/<int:pk>/post/", views.stock_adjustment_post, name="stock-adjustment-post"),

    # Alerts
    path("alerts/", views.inventory_alerts, name="alerts"),
]
