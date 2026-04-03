import django, os, re
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sowafinance.settings')
django.setup()

from django.conf import settings
settings.ALLOWED_HOSTS = ['*']
from django.test import Client
from sowaAuth.models import Newuser
from tenancy.models import Company

user = Newuser.objects.first()
company = Company.objects.first()
client = Client()
client.force_login(user)
session = client.session
session['company_id'] = company.id
session['active_company_id'] = company.id
session['workspace_mode'] = 'client'
session.save()

# GET the add-products form
resp = client.get('/inventory/inventory/add/products')
print("GET status:", resp.status_code)
if resp.status_code == 200:
    content = resp.content.decode('utf-8', errors='replace')
    actions = re.findall(r'<form[^>]*action="([^"]*)"', content)
    for a in actions:
        print("Form action:", a)
    print("CSRF tokens:", content.count('csrfmiddlewaretoken'))
    print("Save action buttons:", content.count('save_action'))
    print("Has typeSelect:", 'typeSelect' in content)
    print("Has trackInventoryGroup:", 'trackInventoryGroup' in content)
elif resp.status_code == 302:
    print("Redirect to:", resp.url)

# POST to save
print("\n--- Test POST ---")
resp = client.post('/inventory/inventory/add/products', {
    'type': 'Service',
    'name': 'BrowserTestProduct',
    'sku': '',
    'save_action': 'save',
}, follow=False)
print("POST status:", resp.status_code)
if resp.status_code == 302:
    print("Redirect:", resp.url)
elif resp.status_code == 500:
    content = resp.content.decode('utf-8', errors='replace')
    matches = re.findall(r'exception_value[^>]*>(.*?)<', content, re.DOTALL)
    for m in matches[:3]:
        print("Exception:", m.strip()[:500])
elif resp.status_code == 200:
    print("200 - form re-rendered (possibly errors)")

from inventory.models import Product
p = Product.objects.filter(name='BrowserTestProduct').first()
if p:
    print("Product CREATED: id=%s company_id=%s" % (p.id, p.company_id))
    p.delete()
    print("Cleaned up")
else:
    print("Product NOT created!")
