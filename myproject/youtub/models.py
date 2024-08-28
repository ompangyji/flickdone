from django.db import models

# Create your models here.
class youtub(models.Model):
    FILE_TYPE_CHOICES = [
        ('image', 'Image'),
        ('audio', 'Audio'),
        ('text', 'Text'),
    ]

    file_name = models.CharField(max_length=255)
    file_type = models.CharField(max_length=50, choices=FILE_TYPE_CHOICES)
    file_path = models.FileField(upload_to='uploads/')

    def __str__(self):
        return self.file_name