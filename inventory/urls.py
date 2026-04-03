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

    # Assembly builds
    path("builds/", views.build_list, name="build-list"),
    path("builds/new/", views.add_build, name="add-build"),
    path("builds/<int:pk>/", views.build_detail, name="build-detail"),
    path("builds/<int:pk>/complete/", views.complete_build_view, name="complete-build"),
]
