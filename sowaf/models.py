from django.db import models
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
# Create your models here.

class Newcustomer(models.Model):
    logo = models.ImageField(null=True, blank=True)
    customer_name = models.CharField(max_length=255,null=True, blank=True)
    company_name = models.CharField(max_length=255,null=True, blank=True)
    email = models.EmailField(max_length=255,null=True, blank=True)
    phone_number = models.CharField(max_length=10,null=True, blank=True)
    mobile_number = models.CharField(max_length=10,null=True, blank=True)
    website = models.URLField(max_length=255,null=True, blank=True)
    tin_number = models.CharField(max_length=10,null=True, blank=True)
    opening_balance = models.DecimalField(max_digits=10,decimal_places=2,default=0,null=True, blank=True)
    registration_date = models.DateTimeField(auto_now_add=True,null=True, blank=True)
    street_one = models.CharField(max_length=255,null=True, blank=True)
    street_two = models.CharField(max_length=255,null=True, blank=True)
    city = models.CharField(max_length=255,null=True, blank=True)
    province = models.CharField(max_length=255,null=True, blank=True)
    postal_code = models.CharField(max_length=5,null=True, blank=True)
    country = models.CharField(max_length=255,null=True, blank=True)
    notes = models.TextField(max_length=1000,null=True, blank=True)
    is_active = models.BooleanField(default=True)
    attachments = models.FileField(upload_to='uploads/',null=True, blank=True)

    class Meta:
        ordering =['customer_name']

    def __str__(self):
        return f'{self.customer_name}-{self.company_name}-{self.phone_number}-{self.country}'
    
# supplier model
class Newsupplier(models.Model):
    PAYMENT_CHOICES = [
            ('Bank transafer', 'Bank transafer'),
            ('Cheque','Cheque'),
            ('Cash','Cash'),
    ]
    SUPPLIER_CHOICES = [
            ('Goods', 'Goods'),
            ('Services','Services'),
            ('Both','Both'),
    ]
    STATUS_CHOICES = [
            ('Active', 'Active'),
            ('Inactive','Inactive'),
    ]
    logo = models.ImageField(null=True, blank=True)
    company_name = models.CharField(max_length=255,null=True, blank=True)
    supplier_type = models.CharField(max_length=255, choices=SUPPLIER_CHOICES, default='',null=True, blank=True)
    status = models.CharField(max_length=255, choices=STATUS_CHOICES, default='',null=True, blank=True)
    contact_person = models.CharField(max_length=255,null=True, blank=True)
    contact_position = models.CharField(max_length=255,null=True, blank=True)
    contact = models.CharField(max_length=10,null=True, blank=True)
    email = models.EmailField(max_length=255,null=True, blank=True)
    open_balance = models.DecimalField(max_digits=10, decimal_places=2, default=0,null=True, blank=True)
    website = models.URLField(max_length=255,null=True, blank=True)
    address1 = models.CharField(max_length=255,null=True, blank=True)
    address2= models.CharField(max_length=255,null=True, blank=True)
    city = models.CharField(max_length=255,null=True, blank=True)
    state = models.CharField(max_length=255,null=True, blank=True)
    zip_code = models.CharField(max_length=5,null=True, blank=True)
    country = models.CharField(max_length=255,null=True, blank=True)
    bank = models.CharField(max_length=255,null=True, blank=True)
    bank_account = models.CharField(max_length=255,null=True, blank=True)
    bank_branch = models.CharField(max_length=255,null=True, blank=True)
    payment_terms = models.CharField(max_length=255,null=True, blank=True)
    currency = models.CharField(max_length=255,null=True, blank=True)
    payment_method = models.CharField(max_length=255, choices=PAYMENT_CHOICES, default='',null=True, blank=True)
    tin = models.CharField(max_length=10,null=True, blank=True)
    reg_number=models.CharField(max_length=255,null=True, blank=True)
    tax_rate=models.DecimalField(max_digits=10,default=0,decimal_places=2,null=True, blank=True)
    attachments = models.FileField(upload_to='uploads/',null=True, blank=True)

    class Meta:
        ordering =['company_name']

    def __str__(self):
        return f'{self.company_name}-{self.contact_person}-{self.contact}-{self.country}'
    
class Newclient(models.Model):
    CURRENCY_CHOICES = [
        ('UGX', 'UGX'),
        ('USD', 'UGX')
    ]
    INDUSTRY_CHOICES=[
        ('Consumer products','Consumer products'),
        ('Energy and natural resources','Energy and natural resources'),
        ('Financial services','Financial services'),
        ('Healthcare','Healthcare'),
        ('Industrial products','Industrial products'),
        ('Not for profit','Not for profit'),
        ('Individual private clients','Individual private clients'),
        ('Public sector','Public sector'),
        ('Real estate and construction','Real estate and construction'),
        ('Services','Services'),
        ('Technology, media and telecommunications','Technology, media and telecommunications'),
        ('Travel, tourism and leisure','Travel, tourism and leisure'),
        ('Others','Others'),
    ]
    STATUS_CHOICES = [
            ('Active', 'Active'),
            ('Inactive','Inactive'),
    ]
    logo = models.ImageField(null=True, blank=True)
    company = models.CharField(max_length=255,null=True, blank=True)
    phone = models.CharField(max_length=10,null=True, blank=True)
    company_email = models.EmailField(max_length=255,null=True, blank=True)
    address = models.CharField(max_length=255,null=True, blank=True)
    country = models.CharField(max_length=255,null=True, blank=True)
    reg_number=models.CharField(max_length=255,null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    contact_name= models.CharField(max_length=255,null=True, blank=True)
    position = models.CharField(max_length=255,null=True, blank=True)
    contact = models.CharField(max_length=10,null=True, blank=True)
    contact_email = models.CharField(max_length=255,null=True, blank=True)
    tin = models.CharField(max_length=10,null=True, blank=True)
    credit_limit = models.DecimalField(max_digits=255, decimal_places=2, default=0,null=True, blank=True)
    payment_terms = models.CharField(max_length=255,null=True, blank=True)
    currency = models.CharField(max_length=255,null=True, blank=True)
    industry = models.CharField(choices=INDUSTRY_CHOICES, default='',null=True, blank=True)
    status = models.CharField(max_length=255, choices=STATUS_CHOICES, default='',null=True, blank=True)
    notes = models.TextField(max_length=1000,null=True, blank=True)
    class Meta:
        ordering =['company']

    def __str__(self):
        return f'{self.company}-{self.contact_name}-{self.contact}-{self.country}'

class Newemployee(models.Model):
    PAYMENT_CHOICES = [
            ('Bank transafer', 'Bank transafer'),
            ('Cheque','Cheque'),
            ('Cash','Cash'),
    ]
    STATUS_CHOICES = [
            ('Active', 'Active'),
            ('Suspended','Suspended'),
            ('Terminated','Terminated'),
    ]
    EMPLOYMENT_CHOICES = [
        ('Full-time', 'Full-time'),
        ('Part-time', 'Part-time'),
        ('Contract', 'Contract'),
        ('Intern', 'Intern'),
        ('Volunteer', 'Volunteer'),
    ]
    GENDER_CHOICES = [
        ('Male', 'Male'),
        ('Female', 'Female'),
        ('Other', 'Other'),
    ]
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    gender = models.CharField(choices=GENDER_CHOICES,default='',max_length=255, null=True, blank=True)
    dob = models.DateField(max_length=255, null=True, blank=True)
    nationality = models.CharField(max_length=255, null=True, blank=True)
    nin_number = models.CharField(max_length=14, null=True, blank=True)
    tin_number= models.CharField(max_length=10, null=True, blank=True)
    profile_picture = models.ImageField(null=True, blank=True)
    phone_number = models.CharField(max_length=10, null=True, blank=True)
    email_address = models.EmailField(max_length=255, null=True, blank=True)
    residential_address = models.CharField(max_length=255, null=True, blank=True)
    emergency_person = models.CharField(max_length=255, null=True, blank=True)
    emergency_contact = models.CharField(max_length=255, null=True, blank=True)
    relationship = models.CharField(max_length=255, null=True, blank=True)
    job_title = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    employment_type = models.CharField(choices=EMPLOYMENT_CHOICES,default='',max_length=255, null=True, blank=True)
    status = models.CharField(choices=STATUS_CHOICES,default='',max_length=255, null=True, blank=True)
    hire_date = models.DateField(max_length=255, null=True, blank=True)
    supervisor = models.CharField(max_length=255, null=True, blank=True)
    salary = models.DecimalField(max_digits=255,decimal_places=2, default=0, null=True, blank=True)
    payment_frequency = models.CharField(max_length=255, null=True, blank=True)
    payment_method = models.CharField(choices=PAYMENT_CHOICES,default='',max_length=255, null=True, blank=True)
    bank_name = models.CharField(max_length=255, null=True, blank=True)
    bank_account = models.CharField(max_length=255, null=True, blank=True)
    bank_branch = models.CharField(max_length=255, null=True, blank=True)
    nssf_number = models.CharField(max_length=255, null=True, blank=True)
    insurance_provider = models.CharField(max_length=255, null=True, blank=True)
    taxable_allowances = models.DecimalField(max_digits=255,decimal_places=2,default=0, null=True, blank=True)
    intaxable_allowances= models.DecimalField(max_digits=255,decimal_places=2,default=0, null=True, blank=True)
    additional_notes = models.TextField(max_length=1000, null=True, blank=True)
    doc_attachments= models.FileField(upload_to='uploads/')
    
    doc_attachments= models.FileField(upload_to='uploads/')
# assets model
class Newasset(models.Model):
    DEPRECIATION_CHOICES = [
        ('Straight line','Straight line'),
        ('Reducing balance','Reducing balance'),
    ]
    STATUS_CHOICES = [
        ('Active','Active'),
        ('Disposed','Disposed'),
        ('Written-Off','Written-Off'),
    ]

    asset_name = models.CharField(max_length=255, null=True, blank=True)
    asset_tag = models.CharField(max_length=255, null=True, blank=True)
    asset_category = models.CharField(max_length=255, null=True, blank=True)
    asset_description = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    custodian = models.CharField(max_length=255, null=True, blank=True)
    asset_status = models.CharField(choices=STATUS_CHOICES,default='',max_length=255, null=True, blank=True)
    purchase_price = models.CharField(max_length=255, null=True, blank=True)
    purchase_date = models.DateField(max_length=255, null=True, blank=True)
    supplier = models.ForeignKey(Newsupplier,on_delete=models.CASCADE, related_name='supplied_assets')
    warranty = models.CharField(max_length=255, null=True, blank=True)
    funding_source = models.CharField(max_length=255, null=True, blank=True)
    life_span = models.CharField(max_length=255, null=True, blank=True)
    depreciation_method = models.CharField(choices=DEPRECIATION_CHOICES,default='',max_length=255, null=True, blank=True)
    residual_value = models.DecimalField(decimal_places=2,max_digits=10, null=True, blank=True)
    accumulated_depreciation = models.DecimalField(decimal_places=2,max_digits=10, null=True, blank=True)
    remaining_value= models.DecimalField(decimal_places=2,max_digits=10, null=True, blank=True)
    asset_account = models.DecimalField(decimal_places=2,max_digits=10, null=True, blank=True)
    capitalization_date = models.DateField(max_length=255, null=True, blank=True)
    cost_center = models.CharField(max_length=255, null=True, blank=True)
    asset_condition = models.CharField(max_length=255, null=True, blank=True)
    maintenance_schedule= models.CharField(max_length=255, null=True, blank=True)
    insurance_details =models.CharField(max_length=255, null=True, blank=True)
    notes =models.CharField(max_length=255, null=True, blank=True)
    asset_attachments= models.FileField(upload_to='uploads/')

    class Meta:
        ordering =['asset_name']

    def __str__(self):
        return f'{self.asset_name}-{self.asset_category}-{self.department}-{self.custodian}'
