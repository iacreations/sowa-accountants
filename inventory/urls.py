from django.urls import path
from . import views


app_name='inventory'
# my urls
urlpatterns = [  
# inventory urls
    path('inventory/add/products', views.add_products, name='add-products'),
    path("products/<int:pk>/", views.product_detail, name="product-detail"),
    path("products/<int:pk>/edit/", views.product_edit, name="product-edit"),
    path("add-category-ajax/", views.add_category_ajax, name="add_category_ajax"),
    path("add-class-ajax/", views.add_class_ajax, name="add_class_ajax"),
]
